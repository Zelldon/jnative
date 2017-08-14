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

#include "BucketBufferArray.h"
#include <stdlib.h>
#include "BucketBufferArrayDescriptor.h"


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


void clearOverflowPointers(char *bucketBufferPtr, int maxBucketLength)
{
    const int startOffset = BUCKET_BUFFER_HEADER_LENGTH;
    for (int i = 0; i < ALLOCATION_FACTOR; i++)
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
    char *newBucketBufferPtr = malloc(maxBucketBufferLength);
    
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
    ((char*) bucketBufferHeaderAddress)[MAIN_BUFFER_COUNT_OFFSET]++;
    
    return (long) newBucketBufferPtr;
}



