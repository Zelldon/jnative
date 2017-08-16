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

import static io.zeebe.map.BucketBufferArrayDescriptor.*;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;

import io.zeebe.map.BucketBufferArrayDescriptor;

/**
 *
 */
public class BucketBufferArray implements AutoCloseable
{
    static
    {
        System.loadLibrary("nativeMap");
    }

    protected native long allocate(long size);

    protected native void free(long address);

    public static final int ALLOCATION_FACTOR = 32;
//    public static final int OVERFLOW_BUCKET_ID = -1;
//    private static final long INVALID_ADDRESS = 0;

    private static final String FAIL_MSG_TO_READ_BUCKET_BUFFER = "Failed to read bucket buffer array, managed to read %d bytes.";

    private final long instanceAddress;

    public BucketBufferArray(int maxBucketBlockCount, int maxKeyLength, int maxValueLength)
    {
        final int maxBucketLength =
            addExact(BUCKET_DATA_OFFSET,
                     multiplyExact(maxBucketBlockCount, BucketBufferArrayDescriptor.getBlockLength(maxKeyLength, maxValueLength)));

        final int maxBucketBufferLength;
        try
        {
            maxBucketBufferLength = addExact(BUCKET_BUFFER_HEADER_LENGTH, multiplyExact(ALLOCATION_FACTOR, maxBucketLength));
        }
        catch (ArithmeticException ae)
        {
            throw new IllegalArgumentException("Maximum bucket buffer length exceeds integer maximum value.", ae);
        }

        instanceAddress = createInstance(maxBucketLength, maxBucketBlockCount, maxKeyLength, maxValueLength, maxBucketBufferLength);
    }

    private native long createInstance(int maxBucketLength, int maxBucketBlockCount,
                                         int maxKeyLength, int maxValueLength, int maxBucketBufferLength);

    public void clear()
    {
        close();
        clearInternal();
    }

    private native void clearInternal();

    protected void allocateNewBucketBuffer(int newBucketBufferId)
    {
//        if (newBucketBufferId >= realAddresses.length)
//        {
//            final long newAddressTable[] = new long[realAddresses.length * 2];
//            System.arraycopy(realAddresses, 0, newAddressTable, 0, realAddresses.length);
//            realAddresses = newAddressTable;
//        }
//
//        capacity += maxBucketBufferLength;
//        realAddresses[newBucketBufferId] =
//            allocateNewBucketBuffer(bucketBufferHeaderAddress, maxBucketBufferLength, maxBucketLength);

        allocateNewBucketBuffer(instanceAddress, newBucketBufferId);
    }

    private native void allocateNewBucketBuffer(long instanceAddress, int newBucketBufferID);


    // BUCKET BUFFER ARRAY ///////////////////////////////////////////////////////////////////////////
    
//
//    protected static long getBucketAddress(int bucketBufferId, int bucketOffset)
//    {
//        long bucketAddress = 0;
//        bucketAddress += (long) bucketBufferId << 32;
//        bucketAddress += bucketOffset;
//        return bucketAddress;
//    }
//
//    private long getRealAddress(final long bucketAddress)
//    {
//        final int bucketBufferId = (int) (bucketAddress >> 32);
//        final int bucketOffset = (int) bucketAddress;
//        return getRealAddress(bucketBufferId, bucketOffset);
//    }
//
//    private long getRealAddress(int bucketBufferId, int offset)
//    {
//        if (offset < 0 || offset >= maxBucketBufferLength)
//        {
//            throw new IllegalArgumentException("Can't access " + offset + " max bucket buffer length is: " + maxBucketBufferLength);
//        }
//
//        return realAddresses[bucketBufferId] + offset;
//    }

    private native int readInt(long address);
    private native long readLong(long address);

//
//    private void setBucketBufferCount(int newBucketBufferCount)
//    {
//        UNSAFE.putInt(bucketBufferHeaderAddress + MAIN_BUFFER_COUNT_OFFSET, newBucketBufferCount);
//    }
//
//    private void setBucketCount(int newBucketCount)
//    {
//        UNSAFE.putInt(bucketBufferHeaderAddress + MAIN_BUCKET_COUNT_OFFSET, newBucketCount);
//    }
//
//    private void setBlockCount(long newBlockCount)
//    {
//        UNSAFE.putLong(bucketBufferHeaderAddress + MAIN_BLOCK_COUNT_OFFSET, newBlockCount);
//    }
//
       
    
    public int getBucketBufferCount()
    {
//        return readInt(bucketBufferHeaderAddress + MAIN_BUFFER_COUNT_OFFSET);
        return getBucketBufferCount(instanceAddress);
    }
    
    private native int getBucketBufferCount(long instanceAddress);

    public int getBucketCount()
    {
//        return readInt(bucketBufferHeaderAddress + MAIN_BUCKET_COUNT_OFFSET);
        return getBucketCount(instanceAddress);
    }
    
    private native int getBucketCount(long instanceAddress);

    public long getBlockCount()
    {
//        return readLong(bucketBufferHeaderAddress + MAIN_BLOCK_COUNT_OFFSET);
        return getBlockCount(instanceAddress);
    }
    
    private native long getBlockCount(long instanceAddress);
    
    
//
    @Override
    public void close()
    {
//        free(bucketBufferHeaderAddress);
//        for (long realAddress : realAddresses)
//        {
//            if (realAddress != INVALID_ADDRESS)
//            {
//                free(realAddress);
//            }
//        }
    }
//
//    public int getFirstBucketOffset()
//    {
//        return BUCKET_BUFFER_HEADER_LENGTH;
//    }

    public long getCapacity()
    {
        return getCapacity(instanceAddress);
    }
    
    private native long getCapacity(long instanceAddress);

    protected long getCountOfUsedBytes()
    {
        return getCountOfUsedBytes(instanceAddress);
    }

    protected native long getCountOfUsedBytes(long instanceAddress);
        
//
    public long size()
    {
//        return MAIN_BUCKET_BUFFER_HEADER_LEN + countOfUsedBytes;
        return size(instanceAddress);
    }
    
    private native long size(long instanceAddress);
//
    public int getMaxBucketBufferLength()
    {
//        return maxBucketBufferLength;
        return getMaxBucketBufferLength(instanceAddress);
    }
    
    private native int getMaxBucketBufferLength(long instanceAddress);
//
    public float getLoadFactor()
    {
        return getLoadFactor(instanceAddress);
    }

    private native float getLoadFactor(long instanceAddress);
//
    public int getMaxBucketLength()
    {
        return getMaxBucketLength(instanceAddress);
    }
    
    private native int getMaxBucketLength(long instanceAddress);
//    

    // BUCKET BUFFER ///////////////////////////////////////////////////////////////
//
//    public int getBucketCount(int bucketBufferId)
//    {
//        return readInt(getRealAddress(bucketBufferId, BUCKET_BUFFER_BUCKET_COUNT_OFFSET));
//    }
//
//    private void setBucketCount(int bucketBufferId, int blockCount)
//    {
//        UNSAFE.putInt(getRealAddress(bucketBufferId, BUCKET_BUFFER_BUCKET_COUNT_OFFSET), blockCount);
//    }
//
//
//    // BUCKET //////////////////////////////////////////////////////////////////////
//
//    public int getBucketFillCount(long bucketAddress)
//    {
//        return UNSAFE.getInt(getRealAddress(bucketAddress) + BUCKET_FILL_COUNT_OFFSET);
//    }
//
//    private void initBucketFillCount(int bucketBufferId, int bucketOffset)
//    {
//        UNSAFE.putInt(getRealAddress(bucketBufferId, bucketOffset) + BUCKET_FILL_COUNT_OFFSET, 0);
//    }
//
//    private void setBucketFillCount(long bucketAddress, int blockFillCount)
//    {
//        UNSAFE.putInt(getRealAddress(bucketAddress) + BUCKET_FILL_COUNT_OFFSET, blockFillCount);
//    }
//
//    public int getBucketLength(long bucketAddress)
//    {
//        return getBucketFillCount(bucketAddress) * (maxKeyLength + maxValueLength) + BUCKET_HEADER_LENGTH;
//    }
//
//    public long getBucketOverflowPointer(long bucketAddress)
//    {
//        return UNSAFE.getLong(getRealAddress(bucketAddress) + BUCKET_OVERFLOW_POINTER_OFFSET);
//    }
//
//    private void clearBucketOverflowPointer(int bucketBufferId, int bucketOffset)
//    {
//        UNSAFE.putLong(getRealAddress(bucketBufferId, bucketOffset) + BUCKET_OVERFLOW_POINTER_OFFSET, 0L);
//    }
//
//    private void setBucketOverflowPointer(long bucketAddress, long overflowPointer)
//    {
//        UNSAFE.putLong(getRealAddress(bucketAddress) + BUCKET_OVERFLOW_POINTER_OFFSET, overflowPointer);
//    }
//
//    public int getBucketOverflowCount(long bucketAddress)
//    {
//        long address = getBucketOverflowPointer(bucketAddress);
//        int count = 0;
//        while (address != 0)
//        {
//            address = getBucketOverflowPointer(address);
//            count++;
//        }
//        return count;
//    }
//
//    public int getFirstBlockOffset()
//    {
//        return BUCKET_DATA_OFFSET;
//    }
//
//    // BLOCK ///////////////////////////////////////////////////////////////////////////////////////////
//
//    public int getBlockLength()
//    {
//        return BucketBufferArrayDescriptor.getBlockLength(maxKeyLength, maxValueLength);
//    }
//
//    public boolean keyEquals(KeyHandler keyHandler, long bucketAddress, int blockOffset)
//    {
//        return keyHandler.keyEquals(getRealAddress(bucketAddress) + blockOffset + BLOCK_KEY_OFFSET);
//    }
//
//    public void readKey(KeyHandler keyHandler, long bucketAddress, int blockOffset)
//    {
//        keyHandler.readKey(getRealAddress(bucketAddress) + blockOffset + BLOCK_KEY_OFFSET);
//    }
//
//    public void readValue(ValueHandler valueHandler, long bucketAddress, int blockOffset)
//    {
//        final long valueOffset = getBlockValueOffset(getRealAddress(bucketAddress) + blockOffset, maxKeyLength);
//        valueHandler.readValue(valueOffset, maxValueLength);
//    }
//
//    public void updateValue(ValueHandler valueHandler, long bucketAddress, int blockOffset)
//    {
//        final int valueLength = valueHandler.getValueLength();
//        if (valueLength > maxValueLength)
//        {
//            throw new IllegalArgumentException("Value can't exceed the max value length of " + maxValueLength);
//        }
//
//        final long blockAddress = getRealAddress(bucketAddress) + blockOffset;
//        valueHandler.writeValue(getBlockValueOffset(blockAddress, maxKeyLength));
//    }
//
//    public boolean addBlock(long bucketAddress, KeyHandler keyHandler, ValueHandler valueHandler)
//    {
//        final int bucketFillCount = getBucketFillCount(bucketAddress);
//        final boolean canAddRecord = bucketFillCount < maxBucketBlockCount;
//
//        if (canAddRecord)
//        {
//            final int blockOffset = getBucketLength(bucketAddress);
//            final int blockLength = BucketBufferArrayDescriptor.getBlockLength(maxKeyLength, maxValueLength);
//
//            final long blockAddress = getRealAddress(bucketAddress) + blockOffset;
//
//            keyHandler.writeKey(blockAddress + BLOCK_KEY_OFFSET);
//            valueHandler.writeValue(getBlockValueOffset(blockAddress, maxKeyLength));
//
//            setBucketFillCount(bucketAddress, bucketFillCount + 1);
//            setBlockCount(getBlockCount() + 1);
//        }
//        else
//        {
//            final long overflowBucketAddress = getBucketOverflowPointer(bucketAddress);
//            if (overflowBucketAddress > 0)
//            {
//                return addBlock(overflowBucketAddress, keyHandler, valueHandler);
//            }
//        }
//
//        return canAddRecord;
//    }
//
//    public void removeBlock(long bucketAddress, int blockOffset)
//    {
//        removeBlockFromBucket(bucketAddress, blockOffset);
//        setBlockCount(getBlockCount() - 1);
//    }
//
//    private void removeBlockFromBucket(long bucketAddress, int blockOffset)
//    {
//        final int blockLength = getBlockLength();
//        final int nextBlockOffset = blockOffset + blockLength;
//
//        moveRemainingMemory(bucketAddress, nextBlockOffset, -blockLength);
//
//        setBucketFillCount(bucketAddress, getBucketFillCount(bucketAddress) - 1);
//    }
//
//    private void setBucketId(int bucketBufferId, int bucketOffset, int newBlockId)
//    {
//        UNSAFE.putInt(getRealAddress(bucketBufferId, bucketOffset) + BUCKET_ID_OFFSET, newBlockId);
//    }
//
//    public int getBucketId(long bucketAddress)
//    {
//        return UNSAFE.getInt(getRealAddress(bucketAddress) + BUCKET_ID_OFFSET);
//    }
//
//    private void setBucketDepth(int bucketBufferId, int bucketOffset, int newBlockDepth)
//    {
//        UNSAFE.putInt(getRealAddress(bucketBufferId, bucketOffset) + BUCKET_DEPTH_OFFSET, newBlockDepth);
//    }
//
//    protected void setBucketDepth(long bucketAddress, int newBlockDepth)
//    {
//        UNSAFE.putInt(getRealAddress(bucketAddress) + BUCKET_DEPTH_OFFSET, newBlockDepth);
//    }
//
//    public int getBucketDepth(long bucketAddress)
//    {
//        return UNSAFE.getInt(getRealAddress(bucketAddress) + BUCKET_DEPTH_OFFSET);
//    }
//
//    public long overflow(long bucketAddress)
//    {
//        final long currentOverflowBucketAddress = getBucketOverflowPointer(bucketAddress);
//        if (currentOverflowBucketAddress > 0)
//        {
//            return overflow(currentOverflowBucketAddress);
//        }
//        else
//        {
//            final long overflowBucketAddress = allocateNewBucket(OVERFLOW_BUCKET_ID, 0);
//            setBucketOverflowPointer(bucketAddress, overflowBucketAddress);
//            return overflowBucketAddress;
//        }
//    }
//
    /**
     * Allocates new bucket and returns the bucket start address.
     *
     * @param newBucketId the new block id
     * @param newBucketDepth the new block depth
     * @return the new block address
     */
    public long allocateNewBucket(int newBucketId, int newBucketDepth)
    {
//        long newUsed = countOfUsedBytes + maxBucketLength;
//        int bucketBufferId = getBucketBufferCount() - 1;
//        int bucketCountInBucketBuffer = getBucketCount(bucketBufferId);
//
//        if (bucketCountInBucketBuffer >= ALLOCATION_FACTOR)
//        {
//            allocateNewBucketBuffer(++bucketBufferId);
//            bucketCountInBucketBuffer = 0;
//            newUsed += BUCKET_BUFFER_HEADER_LENGTH;
//        }
//
//        final int bucketOffset = BUCKET_BUFFER_HEADER_LENGTH + bucketCountInBucketBuffer * maxBucketLength;
//        final long bucketAddress = getBucketAddress(bucketBufferId, bucketOffset);
//
//        countOfUsedBytes = newUsed;

        // low level

//        setBucketId(bucketBufferId, bucketOffset, newBucketId);
//        setBucketDepth(bucketBufferId, bucketOffset, newBucketDepth);
//        initBucketFillCount(bucketBufferId, bucketOffset);
//
//        setBucketCount(bucketBufferId, bucketCountInBucketBuffer + 1);
//        setBucketCount(getBucketCount() + 1);


//        allocateNewBucket(bucketBufferHeaderAddress, realAddresses[bucketBufferId], bucketOffset, newBucketId, newBucketDepth);

//        return bucketAddress;
        
        return allocateNewBucket(instanceAddress, newBucketId, newBucketDepth);
    }

    private native long allocateNewBucket(long instanceAddress, int newBucketId, int newBucketDepth);

//    public void relocateBlock(long bucketAddress, int blockOffset, long newBucketAddress)
//    {
//        final int destBucketFillCount = getBucketFillCount(newBucketAddress);
//
//        if (destBucketFillCount >= maxBucketBlockCount)
//        {
//            // overflow
//            final long overflowBucketAddress = overflow(newBucketAddress);
//            relocateBlock(bucketAddress, blockOffset, overflowBucketAddress);
//        }
//        else
//        {
//            final long srcBlockAddress = getRealAddress(bucketAddress) + blockOffset;
//            final int destBucketLength = getBucketLength(newBucketAddress);
//            final long destBlockAddress = getRealAddress(newBucketAddress) + destBucketLength;
//
//            final int blockLength = getBlockLength();
//
//            // copy to new block
//            UNSAFE.copyMemory(srcBlockAddress, destBlockAddress, blockLength);
//            setBucketFillCount(newBucketAddress, destBucketFillCount + 1);
//
//            // remove from this block (compacts this block)
//            removeBlockFromBucket(bucketAddress, blockOffset);
//
//            // TODO remove overflow buckets
//        }
//    }


//
//
//
//    // HELPER METHODS ////////////////////////
//
//    private void moveRemainingMemory(long bucketAddress, int srcOffset, int moveBytes)
//    {
//        final int bucketLength = getBucketLength(bucketAddress);
//
//        if (srcOffset < bucketLength)
//        {
//            final long srcAddress = getRealAddress(bucketAddress) + srcOffset;
//            final int remainingBytes = bucketLength - srcOffset;
//
//            UNSAFE.copyMemory(srcAddress, srcAddress + moveBytes, remainingBytes);
//        }
//    }
//

//
//    public String toString()
//    {
//        final StringBuilder builder = new StringBuilder();
//
//        for (int i = 0; i < realAddresses.length; i++)
//        {
//            if (realAddresses[i] != 0)
//            {
//                final int bucketCount = getBucketCount(i);
//                for (int j = 0; j < bucketCount; j++)
//                {
//                    final int offset = BUCKET_BUFFER_HEADER_LENGTH + j * getMaxBucketLength();
//                    builder.append(toString(getBucketAddress(i, offset)))
//                           .append("\n");
//                }
//            }
//            else
//            {
//                break;
//            }
//        }
//        return builder.toString();
//    }
//
//    private String toString(long bucketAddress)
//    {
//        final StringBuilder builder = new StringBuilder();
//        int bucketFillCount = getBucketFillCount(bucketAddress);
//        long address = getBucketOverflowPointer(bucketAddress);
//        int count = 0;
//        while (address != 0)
//        {
//            bucketFillCount += getBucketFillCount(address);
//            address = getBucketOverflowPointer(address);
//            count++;
//        }
//
//        builder.append("Bucket-")
//               .append(getBucketId(bucketAddress))
//               .append(" contains ")
//               .append(getBlockCount() == 0 ? 0 : ((double) bucketFillCount / (double) getBlockCount()) * 100D)
//               .append(" % of all blocks")
//               .append(":[ Blocks:")
//               .append(bucketFillCount)
//               .append(" ,Overflow:")
//               .append(count)
//               .append("]");
//        return builder.toString();
//    }





}
