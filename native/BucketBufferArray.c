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