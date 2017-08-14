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

#include "BucketBufferArray.h"
#include "BucketBufferArrayDescriptor.h"
#include "serialization.h"


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
 * Method:    allocateBucketBufferHeader
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_allocateBucketBufferHeader
  (JNIEnv *env, jobject jobj)
{
    uint8_t *bucketBufferHeader = malloc(MAIN_BUCKET_BUFFER_HEADER_LEN);
    
    serialize_int32(bucketBufferHeader + MAIN_BUFFER_COUNT_OFFSET, 0);
    serialize_int32(bucketBufferHeader + MAIN_BUCKET_COUNT_OFFSET, 0);
    serialize_int64(bucketBufferHeader + MAIN_BLOCK_COUNT_OFFSET, 0L);
    
    return (uint64_t) bucketBufferHeader;    
}

void clearOverflowPointers(uint8_t *bucketBufferPtr, uint32_t maxBucketLength)
{
    const uint8_t startOffset = BUCKET_BUFFER_HEADER_LENGTH;
    for (uint32_t i = 0; i < ALLOCATION_FACTOR; i++)
    {
        bucketBufferPtr[startOffset + i * maxBucketLength] = 0;
    }
}

JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_allocateNewBucketBuffer
  (JNIEnv *env, jobject jobj, jlong bucketBufferHeaderAddress, jlong maxBucketBufferLength, jint maxBucketLength)
{
    // on high lvl
//    if (newBucketBufferId >= realAddresses.length)
//    {
//        final long newAddressTable[] = new long[realAddresses.length * 2];
//        System.arraycopy(realAddresses, 0, newAddressTable, 0, realAddresses.length);
//        realAddresses = newAddressTable;
//    }
//  
    
    // low lvl
    // realAddresses[newBucketBufferId] = UNSAFE.allocateMemory(maxBucketBufferLength);
    uint8_t *newBucketBufferPtr = malloc(maxBucketBufferLength);
    
    // not necessary
    //    UNSAFE.setMemory(realAddresses[newBucketBufferId], maxBucketBufferLength, (byte) 0);
    
    // high lvl 
    //       capacity += maxBucketBufferLength;

    // low lvl
    //        setBucketCount(newBucketBufferId, 0);
    //        clearOverflowPointers(newBucketBufferId);
    //     setBucketBufferCount(getBucketBufferCount() + 1);
    newBucketBufferPtr[BUCKET_BUFFER_BUCKET_COUNT_OFFSET] = 0;
    clearOverflowPointers(newBucketBufferPtr, maxBucketLength);
    ((uint8_t*) bucketBufferHeaderAddress)[MAIN_BUFFER_COUNT_OFFSET]++;
    
    return (uint64_t) newBucketBufferPtr;
}



