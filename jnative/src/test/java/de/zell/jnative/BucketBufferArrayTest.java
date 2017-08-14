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

import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 *
 */
public class BucketBufferArrayTest
{

    private final BucketBufferArray bucketBufferArray = new BucketBufferArray(16, SIZE_OF_LONG, SIZE_OF_LONG);

    @Test
    public void shouldAllocateBucket()
    {
        //given

        // when
        final long address = bucketBufferArray.allocate(256);

        // then
        assertThat(address).isGreaterThan(-1);
    }

    @Test
    public void shouldFreeBucket()
    {
        //given
        final long address = bucketBufferArray.allocate(256);

        // when
        bucketBufferArray.free(address);

        // then no error
    }

    @Test
    public void shouldInitBucketBufferArray()
    {
        // given


    }
}
