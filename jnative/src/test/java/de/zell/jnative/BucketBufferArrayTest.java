/*
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
package de.zell.jnative;

import static io.zeebe.map.BucketBufferArrayDescriptor.BUCKET_BUFFER_HEADER_LENGTH;
import static io.zeebe.map.BucketBufferArrayDescriptor.MAIN_BUCKET_BUFFER_HEADER_LEN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 *
 */
public class BucketBufferArrayTest
{

    @Test
    public void shouldCreateBucketBufferArray()
    {
        // when
        final BucketBufferArray bucketBufferArray = new BucketBufferArray(16, SIZE_OF_LONG, SIZE_OF_LONG);

        // then
        assertThat(bucketBufferArray.getCountOfUsedBytes()).isEqualTo(BUCKET_BUFFER_HEADER_LENGTH);
        assertThat(bucketBufferArray.size()).isEqualTo(MAIN_BUCKET_BUFFER_HEADER_LEN + BUCKET_BUFFER_HEADER_LENGTH);
    }

    @Test
    public void shouldAllocate()
    {
        //given
        final BucketBufferArray bucketBufferArray = new BucketBufferArray(16, SIZE_OF_LONG, SIZE_OF_LONG);

        // when
        final long address = bucketBufferArray.allocate(256);

        // then
        assertThat(address).isGreaterThan(-1);
    }

    @Test
    public void shouldFree()
    {
        //given
        final BucketBufferArray bucketBufferArray = new BucketBufferArray(16, SIZE_OF_LONG, SIZE_OF_LONG);
        final long address = bucketBufferArray.allocate(256);

        // when
        bucketBufferArray.free(address);

        // then no error
    }

    @Test
    public void shouldAllocateNewBucketBuffer()
    {
        //given
        final BucketBufferArray bucketBufferArray = new BucketBufferArray(16, SIZE_OF_LONG, SIZE_OF_LONG);

        // when
        bucketBufferArray.allocateNewBucketBuffer(1);

        // then
        assertThat(bucketBufferArray.getCountOfUsedBytes()).isEqualTo(BUCKET_BUFFER_HEADER_LENGTH);
        assertThat(bucketBufferArray.getCapacity()).isEqualTo(2 * bucketBufferArray.getMaxBucketBufferLength());
        assertThat(bucketBufferArray.size()).isEqualTo(MAIN_BUCKET_BUFFER_HEADER_LEN + BUCKET_BUFFER_HEADER_LENGTH);
    }



    @Test
    public void shouldAllocateBucket()
    {
        //given
        final BucketBufferArray bucketBufferArray = new BucketBufferArray(16, SIZE_OF_LONG, SIZE_OF_LONG);

        // when
        bucketBufferArray.allocateNewBucket(1, 1);

        // then
        assertThat(bucketBufferArray.getCountOfUsedBytes()).isEqualTo(BUCKET_BUFFER_HEADER_LENGTH + bucketBufferArray.getMaxBucketLength());
        assertThat(bucketBufferArray.size()).isEqualTo(MAIN_BUCKET_BUFFER_HEADER_LEN + BUCKET_BUFFER_HEADER_LENGTH + bucketBufferArray.getMaxBucketLength());
        assertThat(bucketBufferArray.getCapacity()).isEqualTo(bucketBufferArray.getMaxBucketBufferLength());

        assertThat(bucketBufferArray.getBucketBufferCount()).isEqualTo(1);
        assertThat(bucketBufferArray.getBucketCount()).isEqualTo(1);
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(0);
    }

    @Test
    public void shouldGetLoadFactor()
    {
        //given
        final BucketBufferArray bucketBufferArray = new BucketBufferArray(16, SIZE_OF_LONG, SIZE_OF_LONG);

        // when
        bucketBufferArray.allocateNewBucket(1, 1);

        // then
        final float loadFactor = bucketBufferArray.getLoadFactor();
        assertThat(loadFactor).isEqualTo(0.0F);
    }


}
