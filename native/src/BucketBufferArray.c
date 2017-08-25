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

void allocateBucketBufferHeader(struct BucketBufferArray* bucketBufferArray) {
    bucketBufferArray->bucketBufferHeaderAddress = malloc(MAIN_BUCKET_BUFFER_HEADER_LEN);
    uint8_t *bucketBufferHeader = (uint8_t*) bucketBufferArray->bucketBufferHeaderAddress;

    serialize_int32(bucketBufferHeader + MAIN_BUFFER_COUNT_OFFSET, 0);
    serialize_int32(bucketBufferHeader + MAIN_BUCKET_COUNT_OFFSET, 0);
    serialize_int64(bucketBufferHeader + MAIN_BLOCK_COUNT_OFFSET, 0L);
}

int32_t getBucketBufferCount(struct BucketBufferArray* bucketBufferArray) {
    int32_t value = 0;
    deserialize_int32(bucketBufferArray->bucketBufferHeaderAddress + MAIN_BUFFER_COUNT_OFFSET, &value);
    return value;
}

void allocateNewBucketBuffer(struct BucketBufferArray* bucketBufferArray, int newBucketBufferId) {
    
    // ALLOCATION FACTOR have to be power of two
    int32_t mod = newBucketBufferId & (ALLOCATION_FACTOR - 1);
    if (newBucketBufferId != 0 && mod == 0)
    {
        int32_t bucketBufferCount = getBucketBufferCount(bucketBufferArray);
        void **newAddressTable = malloc((bucketBufferCount + ALLOCATION_FACTOR) * sizeof (void*));
        memcpy(newAddressTable, bucketBufferArray->realAddresses, bucketBufferCount * sizeof(void*));
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
    // is done now on allocation bucket
//    clearOverflowPointers(newBucketBufferPtr, bucketBufferArray->maxBucketLength);

    uint8_t *address = ((uint8_t*) bucketBufferArray->bucketBufferHeaderAddress) + MAIN_BUFFER_COUNT_OFFSET;
    int32_t count = 0;
    deserialize_int32(address, &count);
    serialize_int32(address, count + 1);
}


int32_t getBucketCount(struct BucketBufferArray* bucketBufferArray, int32_t bucketBufferId) {
    int32_t value = 0;
    deserialize_int32(bucketBufferArray->realAddresses[bucketBufferId] + BUCKET_BUFFER_BUCKET_COUNT_OFFSET, &value);
    return value;
}

int64_t getRealAddress(JNIEnv *env, struct BucketBufferArray* bucketBufferArray, int32_t bucketBufferId, int32_t offset) {
    if (offset < 0 || offset >= bucketBufferArray->maxBucketBufferLength) {
        jclass exClass;
        char *msg = "Can't access offset, which is larger then max bucket buffer length.";
        exClass = (*env)->FindClass(env, "java/lang/IllegalArgumentException");
        (*env)->ThrowNew(env, exClass, msg);
    }

    return (int64_t) bucketBufferArray->realAddresses[bucketBufferId] + offset;
}

int64_t getBucketAddress(JNIEnv *env, struct BucketBufferArray* bucketBufferArray, int64_t bucketAddress)
{
    int32_t bucketBufferId = (int32_t) (bucketAddress >> 32);
    int32_t bucketOffset = (int32_t) bucketAddress;
    
    return getRealAddress(env, bucketBufferArray, bucketBufferId, bucketOffset);
}


///*
// * Class:     de_zell_jnative_BucketBufferArray
// * Method:    test
// * Signature: (J)V
// */
//JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_test
//  (JNIEnv *env, jobject obj, jlong addr)
//{
//    int32_t bucketBufferId = (int32_t) (addr >> 32);
//    int32_t bucketOffset = (int32_t) addr;
//    
//    printf("BufferID %d - bucket offset %d", bucketBufferId, bucketOffset);
//}


int64_t getBucketWrappedAddress(int32_t bucketBufferId, int32_t bucketOffset)
{
    int64_t bucketAddress = 0;
    bucketAddress += (int64_t) bucketBufferId << 32;
    bucketAddress += bucketOffset;
    return bucketAddress;
}

void clearBucketBufferArray(struct BucketBufferArray* bucketBufferArray)
{
    int32_t bucketBufferCount = getBucketBufferCount(bucketBufferArray);
//
//    printf("Count %d", bucketBufferCount);
//    printf("  %p  ", bucketBufferArray);
//    printf("  %p  ", bucketBufferArray->realAddresses);
    for (int i = 0; i < bucketBufferCount; i++) {
        // TODO
//        printf("  %p  ", bucketBufferArray->realAddresses[i]);
//        free(bucketBufferArray->realAddresses[i]);
    }
//        printf("  %p  ", bucketBufferArray->bucketBufferHeaderAddress);
    free(bucketBufferArray->realAddresses);
    free(bucketBufferArray->bucketBufferHeaderAddress);
}

struct BucketBufferArray* createInstance(jint maxBucketLength, jint maxBucketBlockCount,
        jint maxKeyLength, jint maxValueLength, jint maxBucketBufferLength)
{
    struct BucketBufferArray *bucketBufferArray = malloc(sizeof (struct BucketBufferArray));
    bucketBufferArray->maxBucketLength = maxBucketLength;
    bucketBufferArray->maxBucketBlockCount = maxBucketBlockCount;
    bucketBufferArray->maxKeyLength = maxKeyLength;
    bucketBufferArray->maxValueLength = maxValueLength;
    bucketBufferArray->maxBucketBufferLength = maxBucketBufferLength;


    bucketBufferArray->realAddresses = malloc(ALLOCATION_FACTOR * sizeof (void*));
    bucketBufferArray->capacity = 0;
    bucketBufferArray->countOfUsedBytes = BUCKET_BUFFER_HEADER_LENGTH;


    //        bucketBufferHeaderAddress = UNSAFE.allocateMemory(MAIN_BUCKET_BUFFER_HEADER_LEN);
    //        setBucketBufferCount(0);
    //        setBucketCount(0);
    //        setBlockCount(0);
    allocateBucketBufferHeader(bucketBufferArray);

    allocateNewBucketBuffer(bucketBufferArray, 0);
    return bucketBufferArray;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    createInstance
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_createInstance
(JNIEnv * env, jobject obj, jint maxBucketLength, jint maxBucketBlockCount,
        jint maxKeyLength, jint maxValueLength, jint maxBucketBufferLength) {
    return (jlong) createInstance(maxBucketLength, maxBucketBlockCount, maxKeyLength, maxValueLength, maxBucketBufferLength);
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    clearInternal
 * Signature: ()V
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_clearInternal
(JNIEnv *env, jobject jobj, jlong instanceAddress) {
    struct BucketBufferArray* bucketBufferArray =
            (struct BucketBufferArray*) instanceAddress;

    clearBucketBufferArray(bucketBufferArray);
    
    struct BucketBufferArray* newBucketBufferArray = createInstance(bucketBufferArray->maxBucketLength,
                   bucketBufferArray->maxBucketBlockCount, 
                   bucketBufferArray->maxKeyLength, 
                   bucketBufferArray->maxValueLength, 
                   bucketBufferArray->maxBucketBufferLength);
    free(bucketBufferArray);
    return (jlong) newBucketBufferArray;
}

JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_allocateNewBucketBuffer
(JNIEnv *env, jobject jobj, jlong instanceAddress, jint newBucketBufferId)
{
    allocateNewBucketBuffer((struct BucketBufferArray*) instanceAddress, newBucketBufferId);
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
 * Method:    writeLong
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_writeLong
  (JNIEnv *env, jobject jobj, jlong address, jlong value)
{
    serialize_int64((uint8_t*) address, value);
}
//
///*
// * Class:     de_zell_jnative_BucketBufferArray
// * Method:    readInt
// * Signature: (J)I
// */
//JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_readInt
//(JNIEnv *env, jobject jobj, jlong address) 
//{
//    int32_t value = 0;
//    deserialize_int32((uint8_t *) address, &value);
//    return value;
//}
//
///*
// * Class:     de_zell_jnative_BucketBufferArray
// * Method:    readLong
// * Signature: (J)J
// */
//JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_readLong
//(JNIEnv *env, jobject jobj, jlong address)
//{
//    int64_t value = 0L;
//    deserialize_int64((uint8_t *) address, &value);
//    return value;
//}

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

JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_closeInternal
(JNIEnv * env, jobject obj, jlong instanceAddress) 
{
    struct BucketBufferArray* bucketBufferArray =
            (struct BucketBufferArray*) instanceAddress;

    clearBucketBufferArray(bucketBufferArray);
    free(bucketBufferArray);
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getCapacity
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_getCapacity
(JNIEnv * env, jobject obj, jlong instanceAddress) {
    return ((struct BucketBufferArray*) instanceAddress)->capacity;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getCountOfUsedBytes
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_getCountOfUsedBytes
(JNIEnv * env, jobject obj, jlong instanceAddress) {
    return ((struct BucketBufferArray*) instanceAddress)->countOfUsedBytes;
}

JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_size
(JNIEnv * env, jobject obj, jlong instanceAddress) {
    return MAIN_BUCKET_BUFFER_HEADER_LEN + ((struct BucketBufferArray*) instanceAddress)->countOfUsedBytes;
}

JNIEXPORT jfloat JNICALL Java_de_zell_jnative_BucketBufferArray_getLoadFactor
(JNIEnv * env, jobject obj, jlong instanceAddress) {
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;

    int32_t bucketCount = 0;
    deserialize_int32((uint8_t *) bucketBufferArray->bucketBufferHeaderAddress + MAIN_BUCKET_COUNT_OFFSET, &bucketCount);

    if (bucketCount <= 0) {
        return 0.0F;
    } else {
        int64_t blockCount = 0L;
        deserialize_int64((uint8_t *) bucketBufferArray->bucketBufferHeaderAddress + MAIN_BLOCK_COUNT_OFFSET, &blockCount);
        return (float) blockCount / (float) (bucketCount * bucketBufferArray->maxBucketBlockCount);
    }
}

JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketBufferBucketCount
(JNIEnv * env, jobject obj, jlong instanceAddress, jint bucketBufferId) {
    
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    int32_t bucketCount = 0;
    deserialize_int32(bucketBufferArray->realAddresses[bucketBufferId], &bucketCount);
    
    return bucketCount;
}

int32_t getBucketFillCount(uint8_t* bucketPtr)
{
    int32_t bucketFillCount = 0;
    deserialize_int32(bucketPtr + BUCKET_FILL_COUNT_OFFSET, &bucketFillCount);

    return bucketFillCount;
}

JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketFillCount
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    
    return getBucketFillCount(bucketPtr);
}


int32_t getBucketLength(struct BucketBufferArray* bucketBufferArray, uint8_t* bucketPtr)
{
    int32_t bucketFillCount = 0;
    deserialize_int32(bucketPtr + BUCKET_FILL_COUNT_OFFSET, &bucketFillCount);
    
    int32_t bucketLength = bucketFillCount 
            * (bucketBufferArray->maxKeyLength + bucketBufferArray->maxValueLength)
            + BUCKET_HEADER_LENGTH;
    
    return bucketLength;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketLength
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketLength
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    return getBucketLength(bucketBufferArray, bucketPtr);
}



/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketOverflowPointer
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketOverflowPointer
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    int64_t overflowPtr = 0;
    deserialize_int64(bucketPtr + BUCKET_OVERFLOW_POINTER_OFFSET, &overflowPtr);
    return overflowPtr;
}

///*
// * Class:     de_zell_jnative_BucketBufferArray
// * Method:    keyEquals
// * Signature: (JJI[B)Z
// */
//JNIEXPORT jboolean JNICALL Java_de_zell_jnative_BucketBufferArray_keyEquals
//(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint blockOffset, jbyteArray keyBuffer)
//{
//    jbyte* bufferPtr = (*env)->GetByteArrayElements(env, keyBuffer, NULL);
//    jsize lengthOfArray = (*env)->GetArrayLength(env, keyBuffer);
//    
//    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
//    
//    jbyte* keyPtr = (jbyte*) (getBucketAddress(env, bucketBufferArray, bucketAddress) + blockOffset);
//    
//    
//    for (int i = 0; i < lengthOfArray; i++)
//    {
//        if (keyPtr[i] != bufferPtr[i])
//        {
//            (*env)->ReleaseByteArrayElements(env, keyBuffer, bufferPtr, JNI_ABORT);
//            return JNI_FALSE;
//        }
//    }
//    
//    // mode == JNI_ABORT free the buffer without copying back the possible changes
//    (*env)->ReleaseByteArrayElements(env, keyBuffer, bufferPtr, JNI_ABORT);
//    return JNI_TRUE;
//}

///*
// * Class:     de_zell_jnative_BucketBufferArray
// * Method:    readKey
// * Signature: (JJI[B)V
// */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_readKey
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint blockOffset)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    uint8_t* keyPtr = (uint8_t*) (getBucketAddress(env, bucketBufferArray, bucketAddress) + blockOffset);
        
    jlong key = 0;
    deserialize_int64(keyPtr, &key);
    
    return key;
}

///*
// * Class:     de_zell_jnative_BucketBufferArray
// * Method:    readValue
// * Signature: (JJI[B)V
// */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_readValue
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint blockOffset)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    uint8_t* valuePtr = (uint8_t*) (getBucketAddress(env, bucketBufferArray, bucketAddress) + blockOffset + bucketBufferArray->maxKeyLength);
        
    jlong value = 0;
    deserialize_int64(valuePtr, &value);
    
    return value;
}
//
//JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_updateValue
//(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint blockOffset, jbyteArray buffer)
//{
//    jbyte* bufferPtr = (*env)->GetByteArrayElements(env, buffer, NULL);
//    jsize lengthOfArray = (*env)->GetArrayLength(env, buffer);
//    
//    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
//    
//    jbyte* valuePtr = (jbyte*) (getBucketAddress(env, bucketBufferArray, bucketAddress) + blockOffset + bucketBufferArray->maxKeyLength);
//            
//    memcpy(valuePtr, bufferPtr, lengthOfArray);    
//    
//    // mode == JNI_ABORT free the buffer without copying back the possible changes
//    (*env)->ReleaseByteArrayElements(env, buffer, bufferPtr, JNI_ABORT);
//}


/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBlockAddress
 * Signature: (JJI)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_getBlockAddress
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint blockOffset)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    
    return (int64_t) bucketPtr + blockOffset;
}

char keyEquals(struct BucketBufferArray* bucketBufferArray, uint8_t* bucketPtr, int blockOffset, jlong key)
{
    int64_t value = *((int64_t*) (bucketPtr + blockOffset));
    return value == key;    
}

JNIEXPORT jboolean JNICALL Java_de_zell_jnative_BucketBufferArray_keyEquals
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint blockOffset, jlong key)
{
    
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    
//    int64_t currentKey = 0;
//    deserialize_int64(bucketPtr + blockOffset, &currentKey);
//    printf("Correct value %ld\n", currentKey);
    
//    printf("Direct access %ld\n", value);
    
//    printf("Equals %d", value == key);
    
    
    
//    return currentKey == key;    
    
    
//    int64_t value = *((int64_t*) (bucketPtr + blockOffset));
//    return value == key;
    
    return keyEquals(bucketBufferArray, bucketPtr, blockOffset, key);
}



jlong addBlock(JNIEnv *env, struct BucketBufferArray* bucketBufferArray, uint64_t bucketAddress, jlong key, jlong value)
{
    
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    int32_t bucketFillCount = getBucketFillCount(bucketPtr);
    
    // can add record
    uint8_t canAddRecord = bucketFillCount < bucketBufferArray->maxBucketBlockCount;
    int64_t blockAddress = -1;
    if (canAddRecord)
    {
        uint32_t blockOffset = getBucketLength(bucketBufferArray, bucketPtr);
        
//            keyHandler.writeKey(blockAddress + BLOCK_KEY_OFFSET);
//            valueHandler.writeValue(getBlockValueOffset(blockAddress, maxKeyLength));
//        jbyte* bufferPtr = (*env)->GetByteArrayElements(env, buffer, NULL);
//        jsize lengthOfArray = (*env)->GetArrayLength(env, buffer);
        
        blockAddress = (int64_t) bucketPtr + blockOffset;
//        serialize_int64((uint8_t*) blockAddress, key);
        *((int64_t*)(bucketPtr + blockOffset)) = key;
        
        serialize_int64((uint8_t*) blockAddress + bucketBufferArray->maxKeyLength, value);
//        memcpy(bucketPtr + blockOffset, bufferPtr, lengthOfArray);
        
        // mode == JNI_ABORT free the buffer without copying back the possible changes
//        (*env)->ReleaseByteArrayElements(env, buffer, bufferPtr, JNI_ABORT);
        
//            setBucketFillCount(bucketAddress, bucketFillCount + 1);
        serialize_int32((uint8_t*) bucketPtr + BUCKET_FILL_COUNT_OFFSET, bucketFillCount + 1);
        
//            setBlockCount(getBlockCount() + 1);
        uint8_t *mainBlockCountAddress = ((uint8_t*) bucketBufferArray->bucketBufferHeaderAddress) + MAIN_BLOCK_COUNT_OFFSET;
        int32_t count = 0;
        deserialize_int32(mainBlockCountAddress, &count);
        serialize_int32(mainBlockCountAddress, count + 1);
    }
    else
    {
        
        // overflow
        int64_t overflowPtr = 0;
        deserialize_int64(bucketPtr + BUCKET_OVERFLOW_POINTER_OFFSET, &overflowPtr);
        if (overflowPtr != 0)
        {
            return addBlock(env, bucketBufferArray, overflowPtr, key, value);
        }
    }
    
    return blockAddress;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    addBlock
 * Signature: (JJ[B)Z
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_addBlock
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jlong key, jlong value)
{    
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    return addBlock(env, bucketBufferArray, bucketAddress, key, value);
}

void moveRemainingMemory(struct BucketBufferArray* bucketBufferArray, 
        uint8_t* bucketPtr, int32_t srcOffset, int32_t moveBytes)
    {
        int32_t bucketLength = getBucketLength(bucketBufferArray, bucketPtr);

        if (srcOffset < bucketLength)
        {
            uint8_t* srcAddress = bucketPtr + srcOffset;
            int32_t remainingBytes = bucketLength - srcOffset;
            
            memcpy(srcAddress + moveBytes, srcAddress, remainingBytes);
        }
    }


void removeBlockFromBucket(JNIEnv *env, struct BucketBufferArray* bucketBufferArray, jlong bucketAddress, jint blockOffset)
{
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    
    int32_t blockLength = bucketBufferArray->maxKeyLength + bucketBufferArray->maxValueLength;
    int32_t nextBlockOffset = blockOffset + blockLength;
    
    moveRemainingMemory(bucketBufferArray, bucketPtr, nextBlockOffset, -blockLength);
    
//        setBucketFillCount(bucketAddress, getBucketFillCount(bucketAddress) - 1);
    int32_t bucketFillCount = getBucketFillCount(bucketPtr);
    serialize_int32((uint8_t*) bucketPtr + BUCKET_FILL_COUNT_OFFSET, bucketFillCount - 1);
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    removeBlock
 * Signature: (JJI)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_removeBlock
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint blockOffset)
{
    
//        final int blockLength = getBlockLength();
//        final int nextBlockOffset = blockOffset + blockLength;
//
//        moveRemainingMemory(bucketAddress, nextBlockOffset, -blockLength);
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    removeBlockFromBucket(env, bucketBufferArray, bucketAddress, blockOffset);
    
//        setBlockCount(getBlockCount() - 1);    
    uint8_t *mainBlockCountAddress = ((uint8_t*) bucketBufferArray->bucketBufferHeaderAddress) + MAIN_BLOCK_COUNT_OFFSET;
    int32_t count = 0;
    deserialize_int32(mainBlockCountAddress, &count);
    serialize_int32(mainBlockCountAddress, count - 1);
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketId
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketId
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    
    int32_t bucketId = 0;
    deserialize_int32(bucketPtr + BUCKET_ID_OFFSET, &bucketId);
    
    return bucketId;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    setBucketDepth
 * Signature: (JJI)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_setBucketDepth
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint newBucketDepth)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    
    serialize_int32(bucketPtr + BUCKET_DEPTH_OFFSET, newBucketDepth);    
}


/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketDepth
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketDepth
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    
    int32_t bucketId = 0;
    deserialize_int32(bucketPtr + BUCKET_DEPTH_OFFSET, &bucketId);
    
    return bucketId;
}


jlong allocateNewBucket(JNIEnv *env, struct BucketBufferArray* bucketBufferArray, jint newBucketId, jint newBucketDepth)
{
    
    long newUsed = bucketBufferArray->countOfUsedBytes + bucketBufferArray->maxBucketLength;
    int32_t bucketBufferId = getBucketBufferCount(bucketBufferArray) - 1;
    int32_t bucketCountInBucketBuffer = getBucketCount(bucketBufferArray, bucketBufferId);

    if (bucketCountInBucketBuffer >= ALLOCATION_FACTOR) {
        allocateNewBucketBuffer(bucketBufferArray, ++bucketBufferId);
        bucketCountInBucketBuffer = 0;
        newUsed += BUCKET_BUFFER_HEADER_LENGTH;
    }


    int32_t bucketOffset = BUCKET_BUFFER_HEADER_LENGTH + bucketCountInBucketBuffer * bucketBufferArray->maxBucketLength;

    int64_t bucketAddress = getRealAddress(env, bucketBufferArray, bucketBufferId, bucketOffset);
    bucketBufferArray->countOfUsedBytes = newUsed;

    serialize_int32((uint8_t*) bucketAddress + BUCKET_FILL_COUNT_OFFSET, 0);
    serialize_int32((uint8_t*) bucketAddress + BUCKET_ID_OFFSET, newBucketId);
    serialize_int32((uint8_t*) bucketAddress + BUCKET_DEPTH_OFFSET, newBucketDepth);
    serialize_int64((uint8_t*) bucketAddress + BUCKET_OVERFLOW_POINTER_OFFSET, 0L);

    // maybe set overflow pointer here? instead clear all on new bucket buffer

    //        setBucketCount(bucketBufferId, bucketCountInBucketBuffer + 1);
    uint8_t *bucketBufferBucketCountAddress = ((uint8_t*) bucketBufferArray->realAddresses[bucketBufferId]) + BUCKET_BUFFER_BUCKET_COUNT_OFFSET;
    int32_t count = 0;
    deserialize_int32(bucketBufferBucketCountAddress, &count);
    serialize_int32(bucketBufferBucketCountAddress, count + 1);

    //        setBucketCount(getBucketCount() + 1);        
    uint8_t *mainBucketCountAddress = ((uint8_t*) bucketBufferArray->bucketBufferHeaderAddress) + MAIN_BUCKET_COUNT_OFFSET;
    count = 0;
    deserialize_int32(mainBucketCountAddress, &count);
    serialize_int32(mainBucketCountAddress, count + 1);

    return getBucketWrappedAddress(bucketBufferId, bucketOffset);
}

jlong overflow(JNIEnv *env, struct BucketBufferArray* bucketBufferArray, jlong bucketAddress)
{
    uint8_t *bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    
    int64_t overflowPtr = 0;
    deserialize_int64(bucketPtr + BUCKET_OVERFLOW_POINTER_OFFSET, &overflowPtr);
    
    if (overflowPtr > 0)
    {
        return overflow(env, bucketBufferArray, overflowPtr);
    }
    else
    {
        int64_t overflowBucketAddress = allocateNewBucket(env, bucketBufferArray, de_zell_jnative_BucketBufferArray_OVERFLOW_BUCKET_ID, 0);
        
        serialize_int64(bucketPtr + BUCKET_OVERFLOW_POINTER_OFFSET, overflowBucketAddress);
        
        return overflowBucketAddress;
    }
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    overflow
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_overflow
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    return overflow(env, bucketBufferArray, bucketAddress);    
}



JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_allocateNewBucket
(JNIEnv *env, jobject obj, jlong instanceAddress, jint newBucketId, jint newBucketDepth) {

    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;

    return allocateNewBucket(env, bucketBufferArray, newBucketId, newBucketDepth);
}

void relocate(JNIEnv * env, struct BucketBufferArray* bucketBufferArray, jlong bucketAddress, jint blockOffset, jlong newBucketAddress)
{
    
    uint8_t *destBucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, newBucketAddress);    
    int32_t destBucketFillCount = getBucketFillCount(destBucketPtr);
    
    if (destBucketFillCount >= bucketBufferArray->maxBucketBlockCount)
    {
        int64_t overflowBucketAddress = overflow(env, bucketBufferArray, newBucketAddress);
        relocate(env, bucketBufferArray, bucketAddress, blockOffset, overflowBucketAddress);
    }
    else
    {
        
        uint8_t *srcBlockPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress) + blockOffset;
        
        int32_t destBucketLength = getBucketLength(bucketBufferArray, destBucketPtr);
        uint8_t *destBlockPtr = destBucketPtr + destBucketLength;
        
        int32_t blockLength = bucketBufferArray->maxKeyLength + bucketBufferArray->maxValueLength;
        
        memcpy(destBlockPtr, srcBlockPtr, blockLength);
        
        serialize_int32((uint8_t*) destBucketPtr + BUCKET_FILL_COUNT_OFFSET, destBucketFillCount + 1);
        
        removeBlockFromBucket(env, bucketBufferArray, bucketAddress, blockOffset);
        
        
//            final long srcBlockAddress = getRealAddress(bucketAddress) + blockOffset;
//            final int destBucketLength = getBucketLength(newBucketAddress);
//            final long destBlockAddress = getRealAddress(newBucketAddress) + destBucketLength;
//
//            final int blockLength = getBlockLength();
//
//            // copy to new block
//            UNSAFE.copyMemory(srcBlockAddress, destBlockAddress, blockLength);
//            setBucketFillCount(newBucketAddress, destBucketFillCount + 1);
//
//            // remove from this block (compacts this block)
//            removeBlockFromBucket(bucketAddress, blockOffset);
//
    }
    
    
    
//        final int destBucketFillCount = getBucketFillCount(newBucketAddress);
//
//        if (destBucketFillCount >= maxBucketBlockCount)
//        {
//            // overflow
//            final long overflowBucketAddress = overflow(newBucketAddress);
//            relocateBlock(bucketAddress, blockOffset, overflowBucketAddress);
//        }
//        else
//        {
//            final long srcBlockAddress = getRealAddress(bucketAddress) + blockOffset;
//            final int destBucketLength = getBucketLength(newBucketAddress);
//            final long destBlockAddress = getRealAddress(newBucketAddress) + destBucketLength;
//
//            final int blockLength = getBlockLength();
//
//            // copy to new block
//            UNSAFE.copyMemory(srcBlockAddress, destBlockAddress, blockLength);
//            setBucketFillCount(newBucketAddress, destBucketFillCount + 1);
//
//            // remove from this block (compacts this block)
//            removeBlockFromBucket(bucketAddress, blockOffset);
//
//            // TODO remove overflow buckets
//        }
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    relocateBlock
 * Signature: (JJIJ)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_relocateBlock
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint blockOffset, jlong newBucketAddress)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    relocate(env, bucketBufferArray, bucketAddress, blockOffset, newBucketAddress);
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


JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray__1_1findBlockInBucket
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jlong key)
{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    int foundBlockOffSet = -1;
    
    char keyFound = 0;
//    
//     final Block foundBlock = blockHelperInstance;
//        foundBlock.reset();
//        boolean keyFound = false;

//    printf("search for %ld\n", key);
    do
    {
//            printf("searching bucket..%ld\n", bucketAddress);
        uint8_t *bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
        int bucketFillCount = getBucketFillCount(bucketPtr);
        int blockOffset = BUCKET_HEADER_LENGTH;
        int blocksVisited = 0;

        while (!keyFound && blocksVisited < bucketFillCount)
        {
            keyFound = keyEquals(bucketBufferArray, bucketPtr, blockOffset, key);

            if (keyFound)
            {
//                printf("found on block offset %d\n", blockOffset);
                foundBlockOffSet = blockOffset;
            }
            
            blockOffset += bucketBufferArray->maxKeyLength + bucketBufferArray->maxValueLength;
            blocksVisited++;
        }

        int64_t overflowPtr = 0;
        deserialize_int64(bucketPtr + BUCKET_OVERFLOW_POINTER_OFFSET, &overflowPtr);
        
        bucketAddress = overflowPtr;        
//        printf("check overflow ptr %ld\n", overflowPtr);
    } while (!keyFound && bucketAddress > 0);

//    printf("end search!");
    return foundBlockOffSet;
}

jlong getKeyHashCode(jlong key)
{
    return (int)(key ^ (key >> 32));
}


JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray__1_1splitBucket
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong filledBucketAddress, jint newBucketId, jint newBucketDepth)  

{
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, filledBucketAddress);
    
    
//        bucketBufferArray.setBucketDepth(filledBucketAddress, newBucketDepth);
    serialize_int32(bucketPtr + BUCKET_DEPTH_OFFSET, newBucketDepth);    
    
//        // create new bucket
//        final long newBucketAddress = bucketBufferArray.allocateNewBucket(newBucketId, newBucketDepth);
    jlong newBucketAddress = allocateNewBucket(env, bucketBufferArray, newBucketId, newBucketDepth);
//    printf("new bucket addr %ld\n", newBucketAddress);
    
//
//
//        // distribute entries into correct blocks
//        distributeEntries(filledBucketAddress, newBucketAddress, bucketDepth);
//        
        do
        {
            bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, filledBucketAddress);            
            int32_t bucketFillCount = getBucketFillCount(bucketPtr);
//            printf("fill count: %d", bucketFillCount );
            int32_t splitMask = 1 << (newBucketDepth -1);

            int blockOffset = BUCKET_DATA_OFFSET;
            int blocksVisited = 0;

            while (blocksVisited < bucketFillCount)
            {
                int blockLength = bucketBufferArray->maxKeyLength + bucketBufferArray->maxValueLength;

                
//                bucketBufferArray.readKey(splitKeyHandler, filledBucketAddress, blockOffset);
                uint8_t* keyPtr = (uint8_t*) (getBucketAddress(env, bucketBufferArray, filledBucketAddress) + blockOffset);        
                jlong key = 0;
                deserialize_int64(keyPtr, &key);
                
                // final long keyHashCode = splitKeyHandler.keyHashCode();
                long keyHashCode = getKeyHashCode(key);
//                printf("key %ld hash code: %ld\n", key, keyHashCode);
//                printf("splitmask %d and bitwise and %ld", splitMask, (keyHashCode & splitMask));

                if ((keyHashCode & splitMask) == splitMask)
                {
//                    printf("relocate %ld\n", key);
//                    bucketBufferArray.relocateBlock(filledBucketAddress, blockOffset, newBucketAddress);
                    relocate(env, bucketBufferArray, filledBucketAddress, blockOffset, newBucketAddress);
                }
                else
                {
                    blockOffset += blockLength;
                }

                blocksVisited++;
            }
            
            
//            filledBucketAddress = bucketBufferArray.getBucketOverflowPointer(filledBucketAddress);
            int64_t overflowPtr = 0;
            deserialize_int64(bucketPtr + BUCKET_OVERFLOW_POINTER_OFFSET, &overflowPtr);

            printf("Overflowptr: %ld " ,overflowPtr);
            filledBucketAddress = overflowPtr; 
        } while (filledBucketAddress != 0);
        return newBucketAddress;
}
