/*
 * Copyright 2017 Christopher Zell <zelldon91@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <stdlib.h>
#include <stdint.h>

#include "de_zell_jnative_BucketBufferArray.h"
#include "BucketBufferHeader.h"
#include "BucketBufferArrayDescriptor.h"
#include "serialization.h"


void clearOverflowPointers(uint8_t *bucketBufferPtr, uint32_t maxBucketLength)
{
    const uint8_t startOffset = BUCKET_BUFFER_HEADER_LENGTH;
    for (uint32_t i = 0; i < ALLOCATION_FACTOR; i++)
    {
        serialize_int64(bucketBufferPtr + startOffset + i * maxBucketLength, 0L);
    }
}

void allocateBucketBufferHeader(struct BucketBufferArray* bucketBufferArray)
{
    bucketBufferArray->bucketBufferHeaderAddress = malloc(MAIN_BUCKET_BUFFER_HEADER_LEN);
    uint8_t *bucketBufferHeader = (uint8_t*) bucketBufferArray->bucketBufferHeaderAddress;

    serialize_int32(bucketBufferHeader + MAIN_BUFFER_COUNT_OFFSET, 0);
    serialize_int32(bucketBufferHeader + MAIN_BUCKET_COUNT_OFFSET, 0);
    serialize_int64(bucketBufferHeader + MAIN_BLOCK_COUNT_OFFSET, 0L);
}

void allocateNewBucketBuffer(struct BucketBufferArray* bucketBufferArray, int newBucketBufferId)
{
    
    int realAddressesLength = sizeof(bucketBufferArray->realAddresses) / sizeof(bucketBufferArray->realAddresses[0]);
    if (newBucketBufferId >= realAddressesLength)
    {
        // todo check
        void* *newAddressTable = malloc(realAddressesLength * 2);
        memcpy(newAddressTable, bucketBufferArray->realAddresses, realAddressesLength);
        free(bucketBufferArray->realAddresses);
        bucketBufferArray->realAddresses = newAddressTable;
    }

    // realAddresses[newBucketBufferId] = UNSAFE.allocateMemory(maxBucketBufferLength);
    bucketBufferArray->realAddresses[newBucketBufferId] = malloc(bucketBufferArray->maxBucketBufferLength);
    uint8_t *newBucketBufferPtr = (uint8_t*) bucketBufferArray->realAddresses[newBucketBufferId];

    // not necessary
    //    UNSAFE.setMemory(realAddresses[newBucketBufferId], maxBucketBufferLength, (byte) 0);

    //       capacity += maxBucketBufferLength;
    bucketBufferArray->capacity += bucketBufferArray->maxBucketBufferLength;

    // low lvl
    //        setBucketCount(newBucketBufferId, 0);
    //        clearOverflowPointers(newBucketBufferId);
    //     setBucketBufferCount(getBucketBufferCount() + 1);
    serialize_int32(newBucketBufferPtr + BUCKET_BUFFER_BUCKET_COUNT_OFFSET, 0);
    clearOverflowPointers(newBucketBufferPtr, bucketBufferArray->maxBucketLength);

    uint8_t *address = ((uint8_t*) bucketBufferArray->bucketBufferHeaderAddress) + MAIN_BUFFER_COUNT_OFFSET;
    int32_t count = 0;
    deserialize_int32(address, &count);
    serialize_int32(address,  count + 1);
}

JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_allocate
  (JNIEnv *env, jobject jobj, jlong size)
{
    return (long) malloc(size);
}

JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_free
  (JNIEnv *env, jobject jobj, jlong address)
{
    free((void*) address);
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    createInstance
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_createInstance
  (JNIEnv * env, jobject obj, jint maxBucketLength, jint maxBucketBlockCount,
                           jint maxKeyLength, jint maxValueLength, jint maxBucketBufferLength)
{
//   BucketBufferArray *bucketBufferArray = new BucketBufferArray(
//                    maxBucketLength, maxBucketBlockCount, maxKeyLength,
//                    maxValueLength, maxBucketBufferLength);


    struct BucketBufferArray *bucketBufferArray = malloc(sizeof(struct BucketBufferArray));
    bucketBufferArray->maxBucketLength = maxBucketLength;
    bucketBufferArray->maxBucketBlockCount = maxBucketBlockCount;
    bucketBufferArray->maxKeyLength = maxKeyLength;
    bucketBufferArray->maxValueLength = maxValueLength;
    bucketBufferArray->maxBucketBufferLength = maxBucketBufferLength;


    bucketBufferArray->realAddresses = malloc(ALLOCATION_FACTOR * sizeof(void*));
    bucketBufferArray->capacity = 0;
    bucketBufferArray->countOfUsedBytes = BUCKET_BUFFER_HEADER_LENGTH;


        //        bucketBufferHeaderAddress = UNSAFE.allocateMemory(MAIN_BUCKET_BUFFER_HEADER_LEN);
        //        setBucketBufferCount(0);
        //        setBucketCount(0);
        //        setBlockCount(0);
        allocateBucketBufferHeader(bucketBufferArray);

        allocateNewBucketBuffer(bucketBufferArray, 0);


   return (jlong) bucketBufferArray;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    clearInternal
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_clearInternal
  (JNIEnv *env, jobject obj)
{
    // free bucket buffers
    // free bucketbuffer header
    // free instance
    // create new instance
}


JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_allocateNewBucketBuffer
  (JNIEnv *env, jobject jobj, jlong instanceAddress, jint newBucketBufferId)
{
    allocateNewBucketBuffer((struct BucketBufferArray*) instanceAddress, newBucketBufferId);
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    readInt
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_readInt
  (JNIEnv *env, jobject jobj, jlong address)
{
    int32_t value = 0;
    deserialize_int32((uint8_t *) address, &value);
    return value;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    readLong
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_readLong
  (JNIEnv *env, jobject jobj, jlong address)
{
    int64_t value = 0L;
    deserialize_int64((uint8_t *) address, &value);
    return value;
    
}


/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketBufferCount
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketBufferCount
  (JNIEnv * env, jobject obj, jlong instanceAddress)
{
    int32_t value = 0;
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    deserialize_int32(bucketBufferArray->bucketBufferHeaderAddress + MAIN_BUFFER_COUNT_OFFSET, &value);
    return value;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketCount
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketCount
  (JNIEnv * env, jobject obj, jlong instanceAddress)
{
    int32_t value = 0;
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    deserialize_int32(bucketBufferArray->bucketBufferHeaderAddress + MAIN_BUCKET_COUNT_OFFSET, &value);
    return value;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBlockCount
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_getBlockCount
  (JNIEnv * env, jobject obj, jlong instanceAddress)
{
    int64_t value = 0L;
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    deserialize_int64(bucketBufferArray->bucketBufferHeaderAddress + MAIN_BLOCK_COUNT_OFFSET, &value);
    return value;
    
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getCapacity
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_getCapacity
  (JNIEnv * env, jobject obj, jlong instanceAddress)
{
    return ((struct BucketBufferArray*) instanceAddress)->capacity;    
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getCountOfUsedBytes
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_getCountOfUsedBytes
  (JNIEnv * env, jobject obj, jlong instanceAddress)
  {
    return ((struct BucketBufferArray*) instanceAddress)->countOfUsedBytes;
  }

int32_t getBucketBufferCount(struct BucketBufferArray* bucketBufferArray)
{
    int32_t value = 0;
    deserialize_int32(bucketBufferArray->bucketBufferHeaderAddress + MAIN_BUFFER_COUNT_OFFSET, &value);
    return value;
}

int32_t getBucketCount(struct BucketBufferArray* bucketBufferArray, int32_t bucketBufferId)
{
    int32_t value = 0;
    deserialize_int32(bucketBufferArray->realAddresses[bucketBufferId] + BUCKET_BUFFER_BUCKET_COUNT_OFFSET, &value);
    return value;
}

int64_t getRealAddress(JNIEnv *env, struct BucketBufferArray* bucketBufferArray, int32_t bucketBufferId, int32_t offset)
{
    if (offset < 0 || offset >= bucketBufferArray->maxBucketBufferLength)
    {
        jclass exClass;
        char *msg = "Can't access offset, which is larger then max bucket buffer length.";
        exClass = (*env)->FindClass( env, "java/lang/IllegalArgumentException");
        (*env)->ThrowNew( env, exClass, msg );
    }

    return (int64_t) bucketBufferArray->realAddresses[bucketBufferId] + offset;
}


JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_size
  (JNIEnv * env, jobject obj, jlong instanceAddress)
{
    return MAIN_BUCKET_BUFFER_HEADER_LEN + ((struct BucketBufferArray*) instanceAddress)->countOfUsedBytes;
}


JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getMaxBucketBufferLength
  (JNIEnv * env, jobject obj, jlong instanceAddress)
{
    return ((struct BucketBufferArray*) instanceAddress)->maxBucketBufferLength;
}


JNIEXPORT jfloat JNICALL Java_de_zell_jnative_BucketBufferArray_getLoadFactor
  (JNIEnv * env, jobject obj, jlong instanceAddress)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
        
    int32_t bucketCount = 0;
    deserialize_int32((uint8_t *) bucketBufferArray->bucketBufferHeaderAddress + MAIN_BUCKET_COUNT_OFFSET, &bucketCount);

    if (bucketCount <= 0)
    {
        return 0.0F;
    }
    else
    {
        int64_t blockCount = 0L;
        deserialize_int64((uint8_t *) bucketBufferArray->bucketBufferHeaderAddress + MAIN_BLOCK_COUNT_OFFSET, &blockCount);
        return (float) blockCount / (float) (bucketCount * bucketBufferArray->maxBucketBlockCount);
    }
}


JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getMaxBucketLength
  (JNIEnv * env, jobject obj, jlong instanceAddress)
{
    return ((struct BucketBufferArray*) instanceAddress)->maxBucketLength;
}

JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_allocateNewBucket
  (JNIEnv *env, jobject obj, jlong instanceAddress, jint newBucketId, jint newBucketDepth)
{
    
    //        setBucketId(bucketBufferId, bucketOffset, newBucketId);
    //        setBucketDepth(bucketBufferId, bucketOffset, newBucketDepth);
    //        initBucketFillCount(bucketBufferId, bucketOffset);
    
    //
    //
    //
    
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    long newUsed = bucketBufferArray->countOfUsedBytes + bucketBufferArray->maxBucketLength;
    int32_t bucketBufferId = getBucketBufferCount(bucketBufferArray) - 1;
    int32_t bucketCountInBucketBuffer = getBucketCount(bucketBufferArray, bucketBufferId);
    
    if (bucketCountInBucketBuffer >= ALLOCATION_FACTOR)
    {
        allocateNewBucketBuffer(bucketBufferArray, ++bucketBufferId);
        bucketCountInBucketBuffer = 0;
        newUsed += BUCKET_BUFFER_HEADER_LENGTH;
    }
    
    
    int32_t bucketOffset = BUCKET_BUFFER_HEADER_LENGTH + bucketCountInBucketBuffer * bucketBufferArray->maxBucketLength;

    int64_t bucketAddress = getRealAddress(env, bucketBufferArray, bucketBufferId, bucketOffset);
    bucketBufferArray->countOfUsedBytes = newUsed;
        
    void* address = bucketBufferArray->realAddresses[bucketBufferId] + bucketOffset;
    serialize_int32((uint8_t*) address + BUCKET_FILL_COUNT_OFFSET, 0);
    serialize_int32((uint8_t*) address + BUCKET_ID_OFFSET, newBucketId);
    serialize_int32((uint8_t*) address + BUCKET_DEPTH_OFFSET, newBucketDepth);

    // maybe set overflow pointer here? instead clear all on new bucket buffer
    
    //        setBucketCount(bucketBufferId, bucketCountInBucketBuffer + 1);
    uint8_t *bucketBufferBucketCountAddress = ((uint8_t*) bucketBufferArray->realAddresses[bucketBufferId]) + BUCKET_BUFFER_BUCKET_COUNT_OFFSET;
    int32_t count = 0;
    deserialize_int32(bucketBufferBucketCountAddress, &count);
    serialize_int32(bucketBufferBucketCountAddress,  count + 1);
    
    //        setBucketCount(getBucketCount() + 1);        
    uint8_t *mainBucketCountAddress = ((uint8_t*) bucketBufferArray->bucketBufferHeaderAddress) + MAIN_BUCKET_COUNT_OFFSET;
    count = 0;
    deserialize_int32(mainBucketCountAddress, &count);
    serialize_int32(mainBucketCountAddress,  count + 1);
        
    return bucketAddress;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getLoadFactor
 * Signature: (I)F
 */
//JNIEXPORT jfloat JNICALL Java_de_zell_jnative_BucketBufferArray_getLoadFactor
//  (JNIEnv *env, jobject jobj, jlong bucketBufferHeaderAddress, jint maxBucketBlockCount)
//{
//    int32_t bucketCount = 0;
//    deserialize_int32((uint8_t *) bucketBufferHeaderAddress + MAIN_BUCKET_COUNT_OFFSET, &bucketCount);
//
//    if (bucketCount <= 0)
//    {
//        return 0.0F;
//    }
//    else
//    {
//        int64_t blockCount = 0L;
//        deserialize_int64((uint8_t *) bucketBufferHeaderAddress + MAIN_BLOCK_COUNT_OFFSET, &blockCount);
//        return (float) blockCount / (float) (bucketCount * maxBucketBlockCount);
//    }
//}

