/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class de_zell_jnative_BucketBufferArray */

#ifndef _Included_de_zell_jnative_BucketBufferArray
#define _Included_de_zell_jnative_BucketBufferArray
#ifdef __cplusplus
extern "C" {
#endif
#undef de_zell_jnative_BucketBufferArray_ALLOCATION_FACTOR
#define de_zell_jnative_BucketBufferArray_ALLOCATION_FACTOR 32L
/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    createInstance
 * Signature: (IIIII)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_createInstance
  (JNIEnv *, jobject, jint, jint, jint, jint, jint);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    clearInternal
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_clearInternal
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    allocateNewBucketBuffer
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_allocateNewBucketBuffer
  (JNIEnv *, jobject, jlong, jint);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketBufferCount
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketBufferCount
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketCount
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketCount
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBlockCount
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_getBlockCount
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    closeInternal
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_closeInternal
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getCapacity
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_getCapacity
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getCountOfUsedBytes
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_getCountOfUsedBytes
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    size
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_size
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getLoadFactor
 * Signature: (J)F
 */
JNIEXPORT jfloat JNICALL Java_de_zell_jnative_BucketBufferArray_getLoadFactor
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketFillCount
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketFillCount
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketLength
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketLength
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    keyEquals
 * Signature: (JJI[B)Z
 */
JNIEXPORT jboolean JNICALL Java_de_zell_jnative_BucketBufferArray_keyEquals
  (JNIEnv *, jobject, jlong, jlong, jint, jbyteArray);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    readKey
 * Signature: (JJI[B)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_readKey
  (JNIEnv *, jobject, jlong, jlong, jint, jbyteArray);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    readValue
 * Signature: (JJI[B)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_readValue
  (JNIEnv *, jobject, jlong, jlong, jint, jbyteArray);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    addBlock
 * Signature: (JJ[B)Z
 */
JNIEXPORT jboolean JNICALL Java_de_zell_jnative_BucketBufferArray_addBlock
  (JNIEnv *, jobject, jlong, jlong, jbyteArray);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    removeBlock
 * Signature: (JJI)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_removeBlock
  (JNIEnv *, jobject, jlong, jlong, jint);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    removeBlockFromBucket
 * Signature: (JJI)V
 */
JNIEXPORT void JNICALL Java_de_zell_jnative_BucketBufferArray_removeBlockFromBucket
  (JNIEnv *, jobject, jlong, jlong, jint);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketId
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketId
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    getBucketDepth
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_zell_jnative_BucketBufferArray_getBucketDepth
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     de_zell_jnative_BucketBufferArray
 * Method:    allocateNewBucket
 * Signature: (JII)J
 */
JNIEXPORT jlong JNICALL Java_de_zell_jnative_BucketBufferArray_allocateNewBucket
  (JNIEnv *, jobject, jlong, jint, jint);

#ifdef __cplusplus
}
#endif
#endif
