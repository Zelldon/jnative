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

import static io.zeebe.map.BucketBufferArray.ALLOCATION_FACTOR;
import static de.zell.jnative.BucketBufferArray.getBucketAddress;

import static io.zeebe.map.BucketBufferArrayDescriptor.BUCKET_BUFFER_HEADER_LENGTH;
import static io.zeebe.map.BucketBufferArrayDescriptor.BUCKET_DATA_OFFSET;
import static io.zeebe.map.BucketBufferArrayDescriptor.MAIN_BUCKET_BUFFER_HEADER_LEN;
import static io.zeebe.map.BucketBufferArrayDescriptor.getBlockLength;
import io.zeebe.map.types.LongKeyHandler;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.assertj.core.api.Assertions.assertThat;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

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
    public void shouldClose()
    {
        final BucketBufferArray bucketBufferArray = new BucketBufferArray(16, SIZE_OF_LONG, SIZE_OF_LONG);
        bucketBufferArray.allocateNewBucketBuffer(1);

        // when
        bucketBufferArray.close();

        // then
    }
    
    @Test
    public void shouldClear()
    {
        final BucketBufferArray bucketBufferArray = new BucketBufferArray(16, SIZE_OF_LONG, SIZE_OF_LONG);
        bucketBufferArray.allocateNewBucketBuffer(1);

        // when
        bucketBufferArray.clear();

        // then
        assertThat(bucketBufferArray.getCountOfUsedBytes()).isEqualTo(BUCKET_BUFFER_HEADER_LENGTH);
        assertThat(bucketBufferArray.size()).isEqualTo(MAIN_BUCKET_BUFFER_HEADER_LEN + BUCKET_BUFFER_HEADER_LENGTH);
    }
//
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
//
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

    

    private static final int MAX_KEY_LEN = SIZE_OF_LONG;
    private static final int MAX_VALUE_LEN = SIZE_OF_LONG;
    private static final int MIN_BLOCK_COUNT = 2;

    protected BucketBufferArray bucketBufferArray;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void init()
    {
        bucketBufferArray = new BucketBufferArray(MIN_BLOCK_COUNT, MAX_KEY_LEN, MAX_VALUE_LEN);
    }

    @After
    public void close()
    {
        bucketBufferArray.close();
    }


    @Test
    public void shouldCreateBucketArray()
    {
        // given bucketBufferArray
        assertThat(bucketBufferArray.getBucketCount()).isEqualTo(0);
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(0);
        assertThat(bucketBufferArray.getCapacity()).isEqualTo(BUCKET_BUFFER_HEADER_LENGTH + ALLOCATION_FACTOR * bucketBufferArray.getMaxBucketLength());
    }

    @Test
    public void shouldThrowOverflowExceptionForToLargeMinBlockCount()
    {
        // expect
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Maximum bucket buffer length exceeds integer maximum value.");

        // when
        new BucketBufferArray(1 << 54, SIZE_OF_LONG, SIZE_OF_LONG);
    }

    @Test
    public void shouldThrowOverflowExceptionForToLargeKeyLength()
    {
        // expect
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Maximum bucket buffer length exceeds integer maximum value.");

        // when
        new BucketBufferArray(8, 1 << 55, SIZE_OF_LONG);
    }

    @Test
    public void shouldThrowOverflowExceptionForToLargeValueLength()
    {
        // expect
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Maximum bucket buffer length exceeds integer maximum value.");

        // when
        new BucketBufferArray(8, SIZE_OF_LONG, 1 << 55);
    }

    @Test
    public void shouldCloseBucketArray()
    {
        // given bucketBufferArray
        final BucketBufferArray bucketBufferArray = new BucketBufferArray(MIN_BLOCK_COUNT, MAX_KEY_LEN, MAX_VALUE_LEN);
        bucketBufferArray.allocateNewBucket(1, 1);

        // when
        bucketBufferArray.close();

        // then next access is still possible
// ERROR        
//        bucketBufferArray.getBucketCount();
    }

    @Test
    public void shouldCreateBucket()
    {
        // given
        final int firstBucketAddress = bucketBufferArray.getFirstBucketOffset();
        assertThat(firstBucketAddress).isEqualTo(BUCKET_BUFFER_HEADER_LENGTH);

        // when
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);

        // then
        assertThat(newBucketAddress).isEqualTo(firstBucketAddress);
        assertThat(bucketBufferArray.getBucketCount()).isEqualTo(1);
        assertThat(bucketBufferArray.getCapacity()).isEqualTo(BUCKET_BUFFER_HEADER_LENGTH + ALLOCATION_FACTOR * bucketBufferArray.getMaxBucketLength());
        assertThat(bucketBufferArray.getBucketDepth(firstBucketAddress)).isEqualTo(1);
        assertThat(bucketBufferArray.getBucketId(firstBucketAddress)).isEqualTo(1);
        assertThat(bucketBufferArray.getBucketLength(firstBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET);
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(0);
    }

    @Test
    public void shouldAllocateNewBucketBufferOnCreatingBuckets()
    {
        // given
        for (int i = 0; i < ALLOCATION_FACTOR; i++)
        {
            bucketBufferArray.allocateNewBucket(i, i);
        }

        // when
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(0xFF, 0xFF);
        
        // then
        assertThat(newBucketAddress).isEqualTo(getBucketAddress(1, BUCKET_BUFFER_HEADER_LENGTH));
        assertThat(bucketBufferArray.getBucketBufferCount()).isEqualTo(2);
        assertThat(bucketBufferArray.getBucketCount()).isEqualTo(ALLOCATION_FACTOR + 1);
        assertThat(bucketBufferArray.getCapacity()).isEqualTo(bucketBufferArray.getMaxBucketBufferLength() * 2);

        final int firstBucketAddress = bucketBufferArray.getFirstBucketOffset();
        for (int i = 0; i < ALLOCATION_FACTOR; i++)
        {
            final int bucketOffset = firstBucketAddress + i * bucketBufferArray.getMaxBucketLength();
            final long bucketAddress = BucketBufferArray.getBucketAddress(0, bucketOffset);

            assertThat(bucketBufferArray.getBucketDepth(bucketAddress)).isEqualTo(i);
            assertThat(bucketBufferArray.getBucketId(bucketAddress)).isEqualTo(i);
            assertThat(bucketBufferArray.getBucketLength(bucketAddress)).isEqualTo(BUCKET_DATA_OFFSET);
        }
        assertThat(bucketBufferArray.getBucketDepth(newBucketAddress)).isEqualTo(0xFF);
        assertThat(bucketBufferArray.getBucketId(newBucketAddress)).isEqualTo(0xFF);
        assertThat(bucketBufferArray.getBucketLength(newBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET);
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(0);
    }

    @Test
    public void shouldIncreaseAddressArrayOnCreatingBuckets()
    {
        // given
        // default address array is 32 - so in the begin we can create 32 bucket buffers after that the
        // array will be doubled
        for (int i = 0; i < 32 * 32; i++)
        {
            bucketBufferArray.allocateNewBucket(i, i);
        }

        assertThat(bucketBufferArray.getBucketBufferCount()).isEqualTo(32);

        // when
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(0xFF, 0xFF);

        // then address array is increased so we can create new bucket buffer
        assertThat(newBucketAddress).isEqualTo(getBucketAddress(32, BUCKET_BUFFER_HEADER_LENGTH));
        assertThat(bucketBufferArray.getBucketBufferCount()).isEqualTo(33);
        assertThat(bucketBufferArray.getBucketCount()).isEqualTo(32 * 32 + 1);
    }

    @Test
    public void shouldClearBucketArray()
    {
        // given bucketBufferArray
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);

        // when
        bucketBufferArray.clear();

        // then only count and overflow pointers are set to zero
        assertThat(bucketBufferArray.getBucketCount()).isEqualTo(0);
        assertThat(bucketBufferArray.getCapacity()).isEqualTo(BUCKET_BUFFER_HEADER_LENGTH + ALLOCATION_FACTOR * bucketBufferArray.getMaxBucketLength());
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(0);
    }

    @Test
    public void shouldCreateBlock()
    {
        // given
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);

        // when
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[bucketBufferArray.getBlockLength()]);
        buffer.putLong(0, 10L);
        buffer.putLong(MAX_KEY_LEN, 0xFFL);
        final boolean wasAdded = bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
        final int newBlockOffset = bucketBufferArray.getFirstBlockOffset();

        // then
        assertThat(wasAdded).isTrue();
        assertThat(newBlockOffset).isEqualTo(BUCKET_DATA_OFFSET);
        assertThat(bucketBufferArray.getBucketCount()).isEqualTo(1);
        assertThat(bucketBufferArray.getCapacity()).isEqualTo(BUCKET_BUFFER_HEADER_LENGTH + ALLOCATION_FACTOR * bucketBufferArray.getMaxBucketLength());
        assertThat(bucketBufferArray.getBucketDepth(newBucketAddress)).isEqualTo(1);
        assertThat(bucketBufferArray.getBucketId(newBucketAddress)).isEqualTo(1);
        assertThat(bucketBufferArray.getBucketLength(newBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET + getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));
        assertThat(bucketBufferArray.getBucketFillCount(newBucketAddress)).isEqualTo(1);
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(1);

        assertThat(bucketBufferArray.getBlockLength()).isEqualTo(getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));

        
        buffer = new UnsafeBuffer(new byte[MAX_KEY_LEN]);
        buffer.putLong(0, 10l);
                
        final boolean keyEquals = bucketBufferArray.keyEquals(newBucketAddress, newBlockOffset, buffer.byteArray());
        assertThat(keyEquals).isTrue();

        bucketBufferArray.readKey(newBucketAddress, newBlockOffset, buffer.byteArray());
        assertThat(buffer.getLong(0)).isEqualTo(10);

        bucketBufferArray.readValue(newBucketAddress, newBlockOffset, buffer.byteArray());
        assertThat(buffer.getLong(0)).isEqualTo(0xFF);
    }
//
    @Test
    public void shouldRemoveBlock()
    {
        // given
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[bucketBufferArray.getBlockLength()]);
        buffer.putLong(0, 10L);
        buffer.putLong(MAX_KEY_LEN, 0xFFL);
        final boolean wasAdded = bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
        final int newBlockOffset = bucketBufferArray.getFirstBlockOffset();

        // when
        bucketBufferArray.removeBlock(newBucketAddress, newBlockOffset);

        // then
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(0);
        assertThat(bucketBufferArray.getBucketFillCount(newBucketAddress)).isEqualTo(0);
        assertThat(bucketBufferArray.getBucketLength(newBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET);
    }
//
    @Test
    public void shouldAddMoreBlocksThanMinimalFitSize()
    {
        // given bucket array with 2 blocks
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
        
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[bucketBufferArray.getBlockLength()]);
        buffer.putLong(0, 10L);
        buffer.putLong(MAX_KEY_LEN, 0xFFL);
        boolean wasAdded = bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
        assertThat(wasAdded).isTrue();
        wasAdded = bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
        assertThat(wasAdded).isTrue();

//        // when
        wasAdded = bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());

        // then
        assertThat(wasAdded).isFalse();
        assertThat(bucketBufferArray.getBucketCount()).isEqualTo(1);
        assertThat(bucketBufferArray.getBucketLength(newBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET + 2 * getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));
        assertThat(bucketBufferArray.getBucketFillCount(newBucketAddress)).isEqualTo(2);
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(2);
    }

    @Test
    public void shouldUpdateBlockValue()
    {
        // given
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[bucketBufferArray.getBlockLength()]);
        buffer.putLong(0, 10L);
        buffer.putLong(MAX_KEY_LEN, 0xFFL);
        bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
        final int newBlockOffset = bucketBufferArray.getFirstBlockOffset();

        // when
        UnsafeBuffer valueBuffer = new UnsafeBuffer(new byte[MAX_VALUE_LEN]);
        valueBuffer.putLong(0, 0xAA);
        
        bucketBufferArray.updateValue(newBucketAddress, newBlockOffset, valueBuffer.byteArray());

        // then
        assertThat(bucketBufferArray.getBlockLength()).isEqualTo(getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));

        
        UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[MAX_VALUE_LEN]);
        keyBuffer.putLong(0, 10L);
        final boolean keyEquals = bucketBufferArray.keyEquals(newBucketAddress, newBlockOffset, keyBuffer.byteArray());
        assertThat(keyEquals).isTrue();

        bucketBufferArray.readKey(newBucketAddress, newBlockOffset, keyBuffer.byteArray());
        assertThat(keyBuffer.getLong(0)).isEqualTo(10);

        bucketBufferArray.readValue(newBucketAddress, newBlockOffset, valueBuffer.byteArray());
        assertThat(valueBuffer.getLong(0)).isEqualTo(0xAA);
    }

    @Test
    public void shouldUpdateBlockValueWithSmallerValue()
    {
        // given bucket array with 1 bucket and two blocks
        bucketBufferArray = new BucketBufferArray(MIN_BLOCK_COUNT, MAX_KEY_LEN, MAX_VALUE_LEN);
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);        
        
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[bucketBufferArray.getBlockLength()]);
        buffer.putLong(0, 10L);
        buffer.putStringWithoutLengthAscii(MAX_KEY_LEN, "hallo");
        bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
        
        buffer.putLong(0, 11L);
        bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
        final int firstBlockOffset = bucketBufferArray.getFirstBlockOffset();

        // when update first block
        
        UnsafeBuffer valueBuffer = new UnsafeBuffer(new byte[MAX_VALUE_LEN]);
        valueBuffer.putStringWithoutLengthAscii(0, "new");
        bucketBufferArray.updateValue(newBucketAddress, firstBlockOffset, valueBuffer.byteArray());

        // then updated block
        assertThat(bucketBufferArray.getBlockLength()).isEqualTo(getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));
        
        UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[MAX_KEY_LEN]);
        keyBuffer.putLong(0, 10L);
        
        boolean keyEquals = bucketBufferArray.keyEquals(newBucketAddress, firstBlockOffset, keyBuffer.byteArray());
        assertThat(keyEquals).isTrue();
        bucketBufferArray.readKey(newBucketAddress, firstBlockOffset, keyBuffer.byteArray());
        assertThat(keyBuffer.getLong(0)).isEqualTo(10);
        bucketBufferArray.readValue(newBucketAddress, firstBlockOffset, valueBuffer.byteArray());
        assertThat(valueBuffer.getStringWithoutLengthUtf8(0, 3)).isEqualTo("new");

        // old block
        final int oldBlockOffset = firstBlockOffset + bucketBufferArray.getBlockLength();
        assertThat(bucketBufferArray.getBlockLength()).isEqualTo(getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));

        keyBuffer.putLong(0, 11L);
        keyEquals = bucketBufferArray.keyEquals(newBucketAddress, oldBlockOffset, keyBuffer.byteArray());
        assertThat(keyEquals).isTrue();
        
        bucketBufferArray.readKey(newBucketAddress, oldBlockOffset, keyBuffer.byteArray());
        assertThat(keyBuffer.getLong(0)).isEqualTo(11);

        
        bucketBufferArray.readValue(newBucketAddress, oldBlockOffset, valueBuffer.byteArray());
        assertThat(valueBuffer.getStringWithoutLengthUtf8(0, 5)).isEqualTo("hallo");
    }

    @Test
    public void shouldUpdateBlockValueWithLargerValue()
    {
        // given bucket array with 1 bucket and two blocks
        bucketBufferArray = new BucketBufferArray(MIN_BLOCK_COUNT, MAX_KEY_LEN, MAX_VALUE_LEN);        
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[bucketBufferArray.getBlockLength()]);
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
        buffer.putLong(0, 10L);
        buffer.putStringWithoutLengthAscii(MAX_KEY_LEN, "old");
        bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
        
        buffer.putLong(0, 11L);
        bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
        final int newBlockOffset = bucketBufferArray.getFirstBlockOffset();

        // when
        UnsafeBuffer valueBuffer = new UnsafeBuffer(new byte[MAX_VALUE_LEN]);
        valueBuffer.putStringWithoutLengthAscii(0, "hallo");
        bucketBufferArray.updateValue(newBucketAddress, newBlockOffset, valueBuffer.byteArray());
        
        // then updated block
        assertThat(bucketBufferArray.getBlockLength()).isEqualTo(getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));

        UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[MAX_KEY_LEN]);
        keyBuffer.putLong(0, 10L);
        boolean keyEquals = bucketBufferArray.keyEquals(newBucketAddress, newBlockOffset, keyBuffer.byteArray());
        assertThat(keyEquals).isTrue();
        bucketBufferArray.readKey(newBucketAddress, newBlockOffset, keyBuffer.byteArray());
        assertThat(keyBuffer.getLong(0)).isEqualTo(10L);
        
        bucketBufferArray.readValue(newBucketAddress, newBlockOffset, valueBuffer.byteArray());
        assertThat(valueBuffer.getStringWithoutLengthUtf8(0, 5)).isEqualTo("hallo");

        // old block
        final int oldBlockOffset = newBlockOffset + bucketBufferArray.getBlockLength();
        assertThat(bucketBufferArray.getBlockLength()).isEqualTo(getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));

        keyBuffer.putLong(0, 11L);
        keyEquals = bucketBufferArray.keyEquals(newBucketAddress, oldBlockOffset, keyBuffer.byteArray());
        assertThat(keyEquals).isTrue();
        bucketBufferArray.readKey(newBucketAddress, oldBlockOffset, keyBuffer.byteArray());
        assertThat(keyBuffer.getLong(0)).isEqualTo(11);

        bucketBufferArray.readValue(newBucketAddress, oldBlockOffset, valueBuffer.byteArray());
        assertThat(valueBuffer.getStringWithoutLengthUtf8(0, 3)).isEqualTo("old");
    }

    @Test
    public void shouldThrowOnUpdateBlockValueWithTooLargerValue()
    {
        // given bucket array with 1 bucket and two blocks
        bucketBufferArray = new BucketBufferArray(MIN_BLOCK_COUNT, MAX_KEY_LEN, MAX_VALUE_LEN);
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
        
        
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[bucketBufferArray.getBlockLength()]);
        buffer.putLong(0, 10L);
        buffer.putStringWithoutLengthAscii(MAX_KEY_LEN, "old");
        bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
        
        buffer.putLong(0, 11L);
        bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
        final int newBlockOffset = bucketBufferArray.getFirstBlockOffset();

        // expect
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Value can't exceed the max value length of 8");

        // when
        UnsafeBuffer valueBuffer = new UnsafeBuffer(new byte[256]);
        buffer.putStringWithoutLengthAscii(0, "toLargeValue1234");
        bucketBufferArray.updateValue(newBucketAddress, newBlockOffset, valueBuffer.byteArray());
    }
    
    @Test
    public void shouldRelocateBlock()
    {
        // given
        final long firstBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(2, 1);        
        
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[bucketBufferArray.getBlockLength()]);
        buffer.putLong(0, 10L);
        buffer.putLong(MAX_KEY_LEN, 0xFF);
        bucketBufferArray.addBlock(firstBucketAddress, buffer.byteArray());
        final int newBlockOffset = bucketBufferArray.getFirstBlockOffset();

        // when
        bucketBufferArray.relocateBlock(firstBucketAddress, newBlockOffset, newBucketAddress);

        // then
        assertThat(bucketBufferArray.getBucketLength(firstBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET);
        assertThat(bucketBufferArray.getBucketFillCount(firstBucketAddress)).isEqualTo(0);

        assertThat(bucketBufferArray.getBucketLength(newBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET + getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));
        assertThat(bucketBufferArray.getBucketFillCount(newBucketAddress)).isEqualTo(1);
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(1);

        assertThat(bucketBufferArray.getBlockLength()).isEqualTo(getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));

        UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[MAX_KEY_LEN]);
        keyBuffer.putLong(0, 10L);
        final boolean keyEquals = bucketBufferArray.keyEquals(newBucketAddress, newBlockOffset, keyBuffer.byteArray());
        assertThat(keyEquals).isTrue();

        bucketBufferArray.readKey(newBucketAddress, newBlockOffset, keyBuffer.byteArray());
        assertThat(keyBuffer.getLong(0)).isEqualTo(10);

        UnsafeBuffer valueBuffer = new UnsafeBuffer(new byte[MAX_VALUE_LEN]);
        bucketBufferArray.readValue(newBucketAddress, newBlockOffset, valueBuffer.byteArray());
        assertThat(valueBuffer.getLong(0)).isEqualTo(0xFF);
    }

    @Test
    public void shouldRelocateBlockToBucketWhichIsHalfFull()
    {
        // given
        final LongKeyHandler keyHandler = new LongKeyHandler();
        final LongValueHandler valueHandler = new LongValueHandler();
        final long firstBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
        keyHandler.theKey = 10;
        valueHandler.theValue = 0xFF;
        
        
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[bucketBufferArray.getBlockLength()]);
        buffer.putLong(0, 10L);
        buffer.putLong(MAX_KEY_LEN, 0xFF);
        bucketBufferArray.addBlock(firstBucketAddress, keyHandler, valueHandler);
        keyHandler.theKey = 11;
        bucketBufferArray.addBlock(newBucketAddress, keyHandler, valueHandler);
        int newBlockOffset = bucketBufferArray.getFirstBlockOffset();

        // when
        bucketBufferArray.relocateBlock(firstBucketAddress, newBlockOffset, newBucketAddress);

        // then
        assertThat(bucketBufferArray.getBucketLength(firstBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET);
        assertThat(bucketBufferArray.getBucketFillCount(firstBucketAddress)).isEqualTo(0);

        assertThat(bucketBufferArray.getBucketLength(newBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET + 2 * getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));
        assertThat(bucketBufferArray.getBucketFillCount(newBucketAddress)).isEqualTo(2);
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(2);

        final int blockLength = bucketBufferArray.getBlockLength();
        assertThat(blockLength).isEqualTo(getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));

        boolean keyEquals = bucketBufferArray.keyEquals(keyHandler, newBucketAddress, newBlockOffset);
        assertThat(keyEquals).isTrue();

        bucketBufferArray.readKey(keyHandler, newBucketAddress, newBlockOffset);
        assertThat(keyHandler.theKey).isEqualTo(11);

        bucketBufferArray.readValue(valueHandler, newBucketAddress, newBlockOffset);
        assertThat(valueHandler.theValue).isEqualTo(0xFF);

        newBlockOffset += blockLength;
        assertThat(blockLength).isEqualTo(getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));

        keyHandler.theKey = 10;
        keyEquals = bucketBufferArray.keyEquals(keyHandler, newBucketAddress, newBlockOffset);
        assertThat(keyEquals).isTrue();

        bucketBufferArray.readKey(keyHandler, newBucketAddress, newBlockOffset);
        assertThat(keyHandler.theKey).isEqualTo(10);

        bucketBufferArray.readValue(valueHandler, newBucketAddress, newBlockOffset);
        assertThat(valueHandler.theValue).isEqualTo(0xFF);
    }
//
//    @Test
//    public void shouldOverflowBucketOnRelocate()
//    {
//        // given
//        final LongKeyHandler keyHandler = new LongKeyHandler();
//        final LongValueHandler valueHandler = new LongValueHandler();
//        final long firstBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
//        final long newBucketAddress = bucketBufferArray.allocateNewBucket(2, 2);
//        keyHandler.theKey = 10;
//        valueHandler.theValue = 0xFF;
//        bucketBufferArray.addBlock(firstBucketAddress, keyHandler, valueHandler);
//        keyHandler.theKey = 11;
//        bucketBufferArray.addBlock(newBucketAddress, keyHandler, valueHandler);
//        keyHandler.theKey = 12;
//        bucketBufferArray.addBlock(newBucketAddress, keyHandler, valueHandler);
//        final int newBlockOffset = bucketBufferArray.getFirstBlockOffset();
//
//        // when
//        bucketBufferArray.relocateBlock(firstBucketAddress, newBlockOffset, newBucketAddress);
//
//        // then new bucket overflows
//        final long bucketOverflowPointer = bucketBufferArray.getBucketOverflowPointer(newBucketAddress);
//        assertThat(bucketOverflowPointer).isGreaterThan(0);
//        assertThat(bucketBufferArray.getBucketDepth(bucketOverflowPointer)).isEqualTo(0);
//        assertThat(bucketBufferArray.getBucketId(bucketOverflowPointer)).isEqualTo(BucketBufferArray.OVERFLOW_BUCKET_ID);
//
//        assertThat(bucketBufferArray.getBucketFillCount(newBucketAddress)).isEqualTo(2);
//        assertThat(bucketBufferArray.getBucketFillCount(bucketOverflowPointer)).isEqualTo(1);
//        assertThat(bucketBufferArray.getBucketFillCount(firstBucketAddress)).isEqualTo(0);
//
//        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(3);
//
//        // and block is equal to
//        keyHandler.theKey = 10;
//        final boolean keyEquals = bucketBufferArray.keyEquals(keyHandler, bucketOverflowPointer, newBlockOffset);
//        assertThat(keyEquals).isTrue();
//
//        bucketBufferArray.readKey(keyHandler, bucketOverflowPointer, newBlockOffset);
//        assertThat(keyHandler.theKey).isEqualTo(10);
//
//        bucketBufferArray.readValue(valueHandler, bucketOverflowPointer, newBlockOffset);
//        assertThat(valueHandler.theValue).isEqualTo(0xFF);
//    }
//
//    @Test
//    public void shouldOverflowBucket()
//    {
//        // given
//        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
//
//        // when
//        final long overflowBucketAddress = bucketBufferArray.overflow(newBucketAddress);
//
//        // then
//        assertThat(bucketBufferArray.getBucketOverflowPointer(newBucketAddress)).isEqualTo(overflowBucketAddress);
//
//        assertThat(bucketBufferArray.getBucketOverflowPointer(overflowBucketAddress)).isEqualTo(0);
//        assertThat(bucketBufferArray.getBucketCount()).isEqualTo(2);
//        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(0);
//        assertThat(bucketBufferArray.getBucketDepth(overflowBucketAddress)).isEqualTo(0);
//        assertThat(bucketBufferArray.getBucketId(overflowBucketAddress)).isEqualTo(BucketBufferArray.OVERFLOW_BUCKET_ID);
//    }
//
//    @Test
//    public void shouldUseOverflowBucketIfAddBlockOnFullBucket()
//    {
//        // given
//        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
//        final long overflowBucketAddress = bucketBufferArray.overflow(newBucketAddress);
//        final LongKeyHandler keyHandler = new LongKeyHandler();
//        final LongValueHandler valueHandler = new LongValueHandler();
//        keyHandler.theKey = 10;
//        valueHandler.theValue = 0xFF;
//        bucketBufferArray.addBlock(newBucketAddress, keyHandler, valueHandler);
//        keyHandler.theKey = 11;
//        bucketBufferArray.addBlock(newBucketAddress, keyHandler, valueHandler);
//
//        // when
//        keyHandler.theKey = 12;
//        valueHandler.theValue = 0xFF;
//        final boolean wasAdded = bucketBufferArray.addBlock(newBucketAddress, keyHandler, valueHandler);
//        final int newBlockOffset = bucketBufferArray.getFirstBlockOffset();
//
//        // then
//        assertThat(wasAdded).isTrue();
//        assertThat(newBlockOffset).isEqualTo(BUCKET_DATA_OFFSET);
//
//        assertThat(bucketBufferArray.getBucketLength(newBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET + 2 * getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));
//        assertThat(bucketBufferArray.getBucketFillCount(newBucketAddress)).isEqualTo(2); // full
//
//        assertThat(bucketBufferArray.getBucketLength(overflowBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET + getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));
//        assertThat(bucketBufferArray.getBucketFillCount(overflowBucketAddress)).isEqualTo(1);
//        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(3);
//
//        final boolean keyEquals = bucketBufferArray.keyEquals(keyHandler, overflowBucketAddress, newBlockOffset);
//        assertThat(keyEquals).isTrue();
//
//        bucketBufferArray.readKey(keyHandler, overflowBucketAddress, newBlockOffset);
//        assertThat(keyHandler.theKey).isEqualTo(12);
//
//        bucketBufferArray.readValue(valueHandler, overflowBucketAddress, newBlockOffset);
//        assertThat(valueHandler.theValue).isEqualTo(0xFF);
//    }
//
//
    @Test
    public void shouldAddBlockOnOverflowBucket()
    {
        // given
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
        final long overflowBucketAddress = bucketBufferArray.overflow(newBucketAddress);
        final LongKeyHandler keyHandler = new LongKeyHandler();
        final LongValueHandler valueHandler = new LongValueHandler();

        // when
        keyHandler.theKey = 10;
        valueHandler.theValue = 0xFF;
        final boolean wasAdded = bucketBufferArray.addBlock(overflowBucketAddress, keyHandler, valueHandler);
        final int newBlockOffset = bucketBufferArray.getFirstBlockOffset();

        // then
        assertThat(wasAdded).isTrue();
        assertThat(newBlockOffset).isEqualTo(BUCKET_DATA_OFFSET);

        assertThat(bucketBufferArray.getBucketLength(newBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET);
        assertThat(bucketBufferArray.getBucketFillCount(newBucketAddress)).isEqualTo(0);

        assertThat(bucketBufferArray.getBucketLength(overflowBucketAddress)).isEqualTo(BUCKET_DATA_OFFSET + getBlockLength(MAX_KEY_LEN, MAX_VALUE_LEN));
        assertThat(bucketBufferArray.getBucketFillCount(overflowBucketAddress)).isEqualTo(1);
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(1);

        final boolean keyEquals = bucketBufferArray.keyEquals(keyHandler, overflowBucketAddress, newBlockOffset);
        assertThat(keyEquals).isTrue();

        bucketBufferArray.readKey(keyHandler, overflowBucketAddress, newBlockOffset);
        assertThat(keyHandler.theKey).isEqualTo(10);

        bucketBufferArray.readValue(valueHandler, overflowBucketAddress, newBlockOffset);
        assertThat(valueHandler.theValue).isEqualTo(0xFF);
    }
//
    @Test
    public void shouldOverflowOverflowBucket()
    {
        // given
        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
        final long overflowBucketAddress = bucketBufferArray.overflow(newBucketAddress);

        // when
        final long newOverflowBucketAddress = bucketBufferArray.overflow(newBucketAddress);

        // then
        assertThat(bucketBufferArray.getBucketCount()).isEqualTo(3);
        assertThat(bucketBufferArray.getBlockCount()).isEqualTo(0);
        assertThat(bucketBufferArray.getBucketOverflowPointer(newBucketAddress)).isEqualTo(overflowBucketAddress);

        assertThat(bucketBufferArray.getBucketDepth(overflowBucketAddress)).isEqualTo(0);
        assertThat(bucketBufferArray.getBucketId(overflowBucketAddress)).isEqualTo(BucketBufferArray.OVERFLOW_BUCKET_ID);
        assertThat(bucketBufferArray.getBucketOverflowPointer(overflowBucketAddress)).isEqualTo(newOverflowBucketAddress);

        assertThat(bucketBufferArray.getBucketDepth(newOverflowBucketAddress)).isEqualTo(0);
        assertThat(bucketBufferArray.getBucketId(newOverflowBucketAddress)).isEqualTo(BucketBufferArray.OVERFLOW_BUCKET_ID);
        assertThat(bucketBufferArray.getBucketOverflowPointer(newOverflowBucketAddress)).isEqualTo(0);
    }
//
//    @Test
//    public void shouldCalculateOverflowBucketCount()
//    {
//        // given
//        final long newBucketAddress = bucketBufferArray.allocateNewBucket(1, 1);
//        final long overflowBucketAddress = bucketBufferArray.overflow(newBucketAddress);
//        final long newOverflowBucketAddress = bucketBufferArray.overflow(newBucketAddress);
//
//        // when
//        final int bucketOverflowCount = bucketBufferArray.getBucketOverflowCount(newBucketAddress);
//
//        // then
//        assertThat(bucketOverflowCount).isEqualTo(2);
//    }

    @Test
    public void shouldCalculateLoadFactor()
    {
        // given

        // when
        int blockCount = 0;

        UnsafeBuffer buffer = new UnsafeBuffer(new byte[bucketBufferArray.getBlockLength()]);
        buffer.putLong(0, 10L);
        buffer.putLong(MAX_KEY_LEN, 0xFFL);
        for (int i = 0; i < 1000; i++)
        {
            final float expectedLoadFactor = getExpectedLoadFactor(blockCount, i);

            assertThat(bucketBufferArray.getLoadFactor()).isEqualTo(expectedLoadFactor);
            final long newBucketAddress = bucketBufferArray.allocateNewBucket(i, i);

            bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
            blockCount++;
            assertThat(bucketBufferArray.getLoadFactor())
                .isEqualTo(getExpectedLoadFactor(blockCount, i + 1));

            bucketBufferArray.addBlock(newBucketAddress, buffer.byteArray());
            blockCount++;
            assertThat(bucketBufferArray.getLoadFactor())
                .isEqualTo(getExpectedLoadFactor(blockCount, i + 1));
        }
    }

    private float getExpectedLoadFactor(float blockCount, int bucketCount)
    {
        return bucketCount == 0
            ? 0
            : blockCount / ((float) bucketCount * 2);
    }


}
