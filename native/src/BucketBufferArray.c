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

void clearOverflowPointers(uint8_t *bucketBufferPtr, uint32_t maxBucketLength) {
    const uint8_t startOffset = BUCKET_BUFFER_HEADER_LENGTH;
    for (uint32_t i = 0; i < ALLOCATION_FACTOR; i++) {
        serialize_int64(bucketBufferPtr + startOffset + i * maxBucketLength, 0L);
    }
}

void allocateBucketBufferHeader(struct BucketBufferArray* bucketBufferArray) {
    bucketBufferArray->bucketBufferHeaderAddress = malloc(MAIN_BUCKET_BUFFER_HEADER_LEN);
    uint8_t *bucketBufferHeader = (uint8_t*) bucketBufferArray->bucketBufferHeaderAddress;

    serialize_int32(bucketBufferHeader + MAIN_BUFFER_COUNT_OFFSET, 0);
    serialize_int32(bucketBufferHeader + MAIN_BUCKET_COUNT_OFFSET, 0);
    serialize_int64(bucketBufferHeader + MAIN_BLOCK_COUNT_OFFSET, 0L);
}

void allocateNewBucketBuffer(struct BucketBufferArray* bucketBufferArray, int newBucketBufferId) {

    int realAddressesLength = sizeof (bucketBufferArray->realAddresses) / sizeof (bucketBufferArray->realAddresses[0]);
    if (newBucketBufferId >= realAddressesLength) {
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
    serialize_int32(address, count + 1);
}

int32_t getBucketBufferCount(struct BucketBufferArray* bucketBufferArray) {
    int32_t value = 0;
    deserialize_int32(bucketBufferArray->bucketBufferHeaderAddress + MAIN_BUFFER_COUNT_OFFSET, &value);
    return value;
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

int32_t getBucketFillCount(struct BucketBufferArray* bucketBufferArray, uint8_t* bucketPtr)
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
    
    return getBucketFillCount(bucketBufferArray, bucketPtr);
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
 * Method:    keyEquals
 * Signature: (JJI[B)Z
 */
JNIEXPORT jboolean JNICALL Java_de_zell_jnative_BucketBufferArray_keyEquals
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint blockOffset, jbyteArray keyBuffer)
{
    jbyte* bufferPtr = (*env)->GetByteArrayElements(env, keyBuffer, NULL);
    jsize lengthOfArray = (*env)->GetArrayLength(env, keyBuffer);
    
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    jbyte* keyPtr = (jbyte*) (getBucketAddress(env, bucketBufferArray, bucketAddress) + blockOffset);
    
    
    for (int i = 0; i < lengthOfArray; i++)
    {
        if (keyPtr[i] != bufferPtr[i])
        {
            (*env)->ReleaseByteArrayElements(env, keyBuffer, bufferPtr, JNI_ABORT);
            return JNI_FALSE;
        }
    }
    
    // mode == JNI_ABORT free the buffer without copying back the possible changes
    (*env)->ReleaseByteArrayElements(env, keyBuffer, bufferPtr, JNI_ABORT);
    return JNI_TRUE;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    readKey
 * Signature: (JJI[B)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_readKey
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint blockOffset, jbyteArray keyBuffer)
{
    jbyte* bufferPtr = (*env)->GetByteArrayElements(env, keyBuffer, NULL);
    jsize lengthOfArray = (*env)->GetArrayLength(env, keyBuffer);
    
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    jbyte* keyPtr = (jbyte*) (getBucketAddress(env, bucketBufferArray, bucketAddress) + blockOffset);
        
    memcpy(bufferPtr, keyPtr, lengthOfArray);
    
    // mode == 0 copy back the content and free the elems buffer
    (*env)->ReleaseByteArrayElements(env, keyBuffer, bufferPtr, 0);
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    readValue
 * Signature: (JJI[B)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_readValue
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jint blockOffset, jbyteArray buffer)
{
    jbyte* bufferPtr = (*env)->GetByteArrayElements(env, buffer, NULL);
    jsize lengthOfArray = (*env)->GetArrayLength(env, buffer);
    
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    jbyte* valuePtr = (jbyte*) (getBucketAddress(env, bucketBufferArray, bucketAddress) + blockOffset + bucketBufferArray->maxKeyLength);
        
    memcpy(bufferPtr, valuePtr, lengthOfArray);
    
    // mode == 0 copy back the content and free the elems buffer
    (*env)->ReleaseByteArrayElements(env, buffer, bufferPtr, 0);    
}


jboolean addBlock(JNIEnv *env, struct BucketBufferArray* bucketBufferArray, uint64_t bucketAddress, jbyteArray buffer)
{
    
    uint8_t* bucketPtr = (uint8_t*) getBucketAddress(env, bucketBufferArray, bucketAddress);
    int32_t bucketFillCount = getBucketFillCount(bucketBufferArray, bucketPtr);
    
    // can add record
    uint8_t canAddRecord = bucketFillCount < bucketBufferArray->maxBucketBlockCount;
    if (canAddRecord)
    {
        uint32_t blockOffset = getBucketLength(bucketBufferArray, bucketPtr);
        
//            keyHandler.writeKey(blockAddress + BLOCK_KEY_OFFSET);
//            valueHandler.writeValue(getBlockValueOffset(blockAddress, maxKeyLength));
        jbyte* bufferPtr = (*env)->GetByteArrayElements(env, buffer, NULL);
        jsize lengthOfArray = (*env)->GetArrayLength(env, buffer);
        
        memcpy(bucketPtr + blockOffset, bufferPtr, lengthOfArray);
        
        // mode == JNI_ABORT free the buffer without copying back the possible changes
        (*env)->ReleaseByteArrayElements(env, buffer, bufferPtr, JNI_ABORT);
        
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
        deserialize_int64(bucketPtr, &overflowPtr);
        
        if (overflowPtr != 0)
        {
            addBlock(env, bucketBufferArray, overflowPtr, buffer);
        }
    }
    
    return canAddRecord;
}

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    addBlock
 * Signature: (JJ[B)Z
 */
JNIEXPORT jboolean JNICALL Java_de_zell_jnative_BucketBufferArray_addBlock
(JNIEnv * env, jobject obj, jlong instanceAddress, jlong bucketAddress, jbyteArray buffer)
{    
    struct BucketBufferArray* bucketBufferArray = (struct BucketBufferArray*) instanceAddress;
    
    return addBlock(env, bucketBufferArray, bucketAddress, buffer);
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


JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_allocateNewBucket
(JNIEnv *env, jobject obj, jlong instanceAddress, jint newBucketId, jint newBucketDepth) {

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

