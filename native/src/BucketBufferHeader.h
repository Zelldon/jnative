/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   BucketBufferHeader.h
 * Author: Christopher Zell <zelldon91@gmail.com>
 *
 * Created on August 16, 2017, 3:46 PM
 */

#ifndef BUCKETBUFFERHEADER_H
#define BUCKETBUFFERHEADER_H

#ifdef __cplusplus
extern "C" {
#endif

struct BucketBufferArray {

    int maxBucketLength;
    int maxBucketBlockCount;
    int maxKeyLength;
    int maxValueLength;
    int maxBucketBufferLength;

    void* *realAddresses;
    void* bucketBufferHeaderAddress;
    long capacity;

    long countOfUsedBytes;
};



#ifdef __cplusplus
}
#endif

#endif /* BUCKETBUFFERHEADER_H */

