/**
 * Copyright © 2017 Christopher Zell (zelldon91@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* 
 * File:   BucketBufferArrayDescriptor.h
 * Author: Christopher Zell <zelldon91@gmail.com>
 *
 * Created on August 14, 2017, 1:54 PM
 */

#ifndef BUCKETBUFFERARRAYDESCRIPTOR_H
#define BUCKETBUFFERARRAYDESCRIPTOR_H

#ifdef __cplusplus
extern "C" {
#endif
    
/**
 *
 * BucketBufferArray layout
 *
 * The main BucketBufferArray header contains the following information's: *
 *
 * <pre>
 *  0               1               2               3
 *  0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                         BUCKET BUFFER COUNT                 |
 *  +-------------------------------------------------------------+
 *  |                         BUCKET COUNT                        |
 *  +-------------------------------------------------------------+
 *  |                         BLOCK COUNT                         |
 *  |                                                             |
 *  +-------------------------------------------------------------+
 * </pre>
 *
 * The BUCKET BUFFER COUNT contains the count of all existing bucket buffers.
 * The BUCKET COUNT contains the count of all existing buckets.
 * The BLOCK COUNT contains the count of all existing block.
 * The last two headers are mainly used to calculate fast the load factor and also be available after
 * deserialization.
 *
 * There can exist multiple BucketBuffers, each of them have the same layout.
 * These BucketBuffers can contain till 32 buckets. The current bucket count is
 * stored in the BUCKET COUNT header.
 *
 * <pre>
 *  +----------------------------+
 *  |         BUCKET COUNT       |
 *  +----------------------------+
 *  |           BUCKET 0         |
 *  +----------------------------+
 *  |             ...            |
 *  +----------------------------+
 *  |           BUCKET 32        |
 *  +----------------------------+
 * </pre>
 *
 * Each bucket has the following layout
 *
 * <pre>
 *  0               1               2               3
 *  0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                       BUCKET FILL COUNT                     |
 *  +-------------------------------------------------------------+
 *  |                       BUCKET ID                             |
 *  +-------------------------------------------------------------+
 *  |                       BUCKET DEPTH                          |
 *  +-------------------------------------------------------------+
 *  |                       BUCKET OVERFLOW                       |
 *  |                       POINTER                               |
 *  +-------------------------------------------------------------+
 *  |                                                             |
 *  |                       BLOCK DATA                           ...
 * ...                                                            |
 *  +-------------------------------------------------------------+
 * </pre>
 *
 * The block data contains the blocks
 *
 * Each block has the following layout
 *
 * <pre>
 *  0               1               2               3
 *  0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                             Key                            ...
 * ...                                                            |
 *  +-------------------------------------------------------------+
 *  |                             Value                          ...
 * ...                                                            |
 *  +-------------------------------------------------------------+
 * </pre>
 *
 */

    #define ALLOCATION_FACTOR 32
    
    #define MAIN_BUFFER_COUNT_OFFSET 0
    #define MAIN_BUCKET_COUNT_OFFSET MAIN_BUFFER_COUNT_OFFSET + 4
    #define MAIN_BLOCK_COUNT_OFFSET MAIN_BUCKET_COUNT_OFFSET + 4
    #define MAIN_BUCKET_BUFFER_HEADER_LEN MAIN_BLOCK_COUNT_OFFSET + 8

    #define BUCKET_BUFFER_BUCKET_COUNT_OFFSET 0
    #define BUCKET_BUFFER_HEADER_LENGTH BUCKET_BUFFER_BUCKET_COUNT_OFFSET + 4
    
    #define BUCKET_FILL_COUNT_OFFSET 0
    #define BUCKET_ID_OFFSET BUCKET_FILL_COUNT_OFFSET + 4
    #define BUCKET_DEPTH_OFFSET BUCKET_ID_OFFSET + 4
    #define BUCKET_OVERFLOW_POINTER_OFFSET BUCKET_DEPTH_OFFSET + 4
    #define BUCKET_DATA_OFFSET BUCKET_OVERFLOW_POINTER_OFFSET + 8
    #define BUCKET_HEADER_LENGTH BUCKET_DATA_OFFSET

    #define BLOCK_KEY_OFFSET 0

#ifdef __cplusplus
}
#endif

#endif /* BUCKETBUFFERARRAYDESCRIPTOR_H */

