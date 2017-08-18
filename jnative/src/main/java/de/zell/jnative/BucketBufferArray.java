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

    public static final int ALLOCATION_FACTOR = 32;
    public static final int OVERFLOW_BUCKET_ID = -1;
//    private static final long INVALID_ADDRESS = 0;

    private static final String FAIL_MSG_TO_READ_BUCKET_BUFFER = "Failed to read bucket buffer array, managed to read %d bytes.";

    private long instanceAddress;
    private final int maxKeyLength;
    private final int maxValueLength;
    private final int maxBucketLength;
    private final int maxBucketBufferLength;

    public BucketBufferArray(int maxBucketBlockCount, int maxKeyLength, int maxValueLength)
    {
        this.maxBucketLength =
            addExact(BUCKET_DATA_OFFSET,
                     multiplyExact(maxBucketBlockCount, BucketBufferArrayDescriptor.getBlockLength(maxKeyLength, maxValueLength)));

        try
        {
            maxBucketBufferLength = addExact(BUCKET_BUFFER_HEADER_LENGTH, multiplyExact(ALLOCATION_FACTOR, maxBucketLength));
        }
        catch (ArithmeticException ae)
        {
            throw new IllegalArgumentException("Maximum bucket buffer length exceeds integer maximum value.", ae);
        }
        
        this.maxKeyLength = maxKeyLength;
        this.maxValueLength = maxValueLength;
               

        instanceAddress = createInstance(maxBucketLength, maxBucketBlockCount, maxKeyLength, maxValueLength, maxBucketBufferLength);
    }

    private native long createInstance(int maxBucketLength, int maxBucketBlockCount,
                                         int maxKeyLength, int maxValueLength, int maxBucketBufferLength);

    public void clear()
    {
        instanceAddress = clearInternal(instanceAddress);        
    }

    private native long clearInternal(long instanceAddress);

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


    public static native long readLong(long address);
    public static native void writeLong(long address, long value);

    // BUCKET BUFFER ARRAY ///////////////////////////////////////////////////////////////////////////
    
//
    protected static long getBucketAddress(int bucketBufferId, int bucketOffset)
    {
        long bucketAddress = 0;
        bucketAddress += (long) bucketBufferId << 32;
        bucketAddress += bucketOffset;
        return bucketAddress;
    }
    
//    public native void test(long bucketAddress);
    
    
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
        if (instanceAddress != 0)
        {
            closeInternal(instanceAddress);
            instanceAddress = 0;            
        }
    }
    
    private native void closeInternal(long instanceAddress);
//
    public int getFirstBucketOffset()
    {
        return BUCKET_BUFFER_HEADER_LENGTH;
    }

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
        return size(instanceAddress);
    }
    
    private native long size(long instanceAddress);
//
    public int getMaxBucketBufferLength()
    {
        return maxBucketBufferLength;
    }    
//
    public float getLoadFactor()
    {
        return getLoadFactor(instanceAddress);
    }

    private native float getLoadFactor(long instanceAddress);
//
    public int getMaxBucketLength()
    {
        return maxBucketLength;
    }
    
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
    public int getBucketFillCount(long bucketAddress)
    {
//        return UNSAFE.getInt(getRealAddress(bucketAddress) + BUCKET_FILL_COUNT_OFFSET);
        return getBucketFillCount(instanceAddress, bucketAddress);
    }
    
    private native int getBucketFillCount(long instanceAddress, long bucketAddress);
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
    public int getBucketLength(long bucketAddress)
    {
//        return getBucketFillCount(bucketAddress) * (maxKeyLength + maxValueLength) + BUCKET_HEADER_LENGTH;
        return getBucketLength(instanceAddress, bucketAddress);
    }
    
    private native int getBucketLength(long instanceAddress, long bucketAddress);
//
    public long getBucketOverflowPointer(long bucketAddress)
    {
//        return UNSAFE.getLong(getRealAddress(bucketAddress) + BUCKET_OVERFLOW_POINTER_OFFSET);
        return getBucketOverflowPointer(instanceAddress, bucketAddress);
    }
    
    private native long getBucketOverflowPointer(long instanceAddress, long bucketAddress);
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
    public int getBucketOverflowCount(long bucketAddress)
    {
        long address = getBucketOverflowPointer(bucketAddress);
        int count = 0;
        while (address != 0)
        {
            address = getBucketOverflowPointer(address);
            count++;
        }
        return count;
    }
//
    public int getFirstBlockOffset()
    {
        return BUCKET_DATA_OFFSET;
    }
//
//    // BLOCK ///////////////////////////////////////////////////////////////////////////////////////////
//
    public int getBlockLength()
    {
        return BucketBufferArrayDescriptor.getBlockLength(maxKeyLength, maxValueLength);
    }
//
    public boolean keyEquals(KeyHandler keyhandler, long bucketAddress, int blockOffset)
    {
//        return keyHandler.keyEquals(getRealAddress(bucketAddress) + blockOffset + BLOCK_KEY_OFFSET);


//        return keyEquals(instanceAddress, bucketAddress, blockOffset, keyhandler.getbytes());

        final long blockAddress = getBlockAddress(instanceAddress, bucketAddress, blockOffset);
        return keyhandler.keyEquals(blockAddress);
    }
    
//    private native boolean keyEquals(long instanceAddress, long bucketAddress, int blockOffset, byte[] key);
//
    public void readKey(KeyHandler keyHandler,long bucketAddress, int blockOffset)
    {
//        keyHandler.readKey(getRealAddress(bucketAddress) + blockOffset + BLOCK_KEY_OFFSET);

//        keyHandler.wrap(keyBuffer);
//        readKey(instanceAddress, bucketAddress, blockOffset, keyBuffer.byteArray());

        final long blockAddress = getBlockAddress(instanceAddress, bucketAddress, blockOffset);
        keyHandler.readKey(blockAddress);
    }
    
//    private native void readKey(long instanceAddress, long bucketAddress, int blockOffset, byte[] keyBuffer);

    
    public void readValue(ValueHandler valueHandler, long bucketAddress, int blockOffset)
    {
//        final long valueOffset = getBlockValueOffset(getRealAddress(bucketAddress) + blockOffset, maxKeyLength);
//        valueHandler.readValue(valueOffset, maxValueLength);
//

//        valueHandler.wrap(valueBuffer);
//        readValue(instanceAddress, bucketAddress, blockOffset, valueBuffer.byteArray());

        final long blockAddress = getBlockAddress(instanceAddress, bucketAddress, blockOffset);
        valueHandler.readValue(blockAddress + maxKeyLength);

    }
    
//    private native void readValue(long instanceAddress, long bucketAddress, int blockOffset, byte[] valueBuffer);
//
    public void updateValue(ValueHandler handler, long bucketAddress, int blockOffset)
    {
        final int valueLength = handler.getValueLength();
        if (valueLength > maxValueLength)
        {
            throw new IllegalArgumentException("Value can't exceed the max value length of " + maxValueLength);
        }
//
//        final long blockAddress = getRealAddress(bucketAddress) + blockOffset;
//        valueHandler.writeValue(getBlockValueOffset(blockAddress, maxKeyLength));
//        updateValue(instanceAddress, bucketAddress, blockOffset, handler.getBytes());

        final long blockAddress = getBlockAddress(instanceAddress, bucketAddress, blockOffset);
        handler.writeValue(blockAddress + maxKeyLength);
        //        final long blockAddress = getBlockAddress(instanceAddress, bucketAddress, blockOffset);
//        
//        handler.writeValue(blockAddress);
    }
    
//    private native void updateValue(long instanceAddress, long bucketAddress, int blockOffset, byte [] newValue);


    private native long getBlockAddress(long instanceAddress, long bucketAddress, int blockOffset);
    
    public boolean addBlock(long bucketAddress, KeyHandler keyhandler, ValueHandler valueHandler)
    {
//        UnsafeBuffer buffer = new UnsafeBuffer(new byte[keyhandler.getKeyLength() + valueHandler.getValueLength()]);
//        buffer.putBytes(0, keyhandler.getbytes());
//        buffer.putBytes(maxKeyLength, valueHandler.getBytes());


        final long blockAddress = addBlock(instanceAddress, bucketAddress);

        boolean foundFreeBlock = blockAddress != -1;
        if (foundFreeBlock)
        {
            keyhandler.writeKey(blockAddress);
            valueHandler.writeValue(blockAddress + maxKeyLength);
        }

        return foundFreeBlock;
    }
    
    private native long addBlock(long instanceAddress, long bucketAddress);
//
    public void removeBlock(long bucketAddress, int blockOffset)
    {
        removeBlock(instanceAddress, bucketAddress, blockOffset);
    }
    
    private native void removeBlock(long instanceAddress, long bucketAddress, int blockOffset);
//    private native void removeBlockFromBucket(long instanceAddress, long bucketAddress, int blockOffset);
 
    
//
//    private void setBucketId(int bucketBufferId, int bucketOffset, int newBlockId)
//    {
//        UNSAFE.putInt(getRealAddress(bucketBufferId, bucketOffset) + BUCKET_ID_OFFSET, newBlockId);
//    }
//
    public int getBucketId(long bucketAddress)
    {
//        return UNSAFE.getInt(getRealAddress(bucketAddress) + BUCKET_ID_OFFSET);
        return getBucketId(instanceAddress, bucketAddress);
    }
    
    private native int getBucketId(long instanceAddress, long bucketAddress);
//
//    private void setBucketDepth(int bucketBufferId, int bucketOffset, int newBlockDepth)
//    {
//        UNSAFE.putInt(getRealAddress(bucketBufferId, bucketOffset) + BUCKET_DEPTH_OFFSET, newBlockDepth);
//    }
//
    protected void setBucketDepth(long bucketAddress, int newBlockDepth)
    {
//        UNSAFE.putInt(getRealAddress(bucketAddress) + BUCKET_DEPTH_OFFSET, newBlockDepth);
        setBucketDepth(instanceAddress, bucketAddress, newBlockDepth);
    }
    
    private native void setBucketDepth(long instanceAddress, long bucketAddress, int newBlockDepth);
//
    
    public int getBucketDepth(long bucketAddress)
    {
//        return UNSAFE.getInt(getRealAddress(bucketAddress) + BUCKET_DEPTH_OFFSET);
        return getBucketDepth(instanceAddress, bucketAddress);
    }
    
    private native int getBucketDepth(long instanceAddress, long bucketAddress);
//
    public long overflow(long bucketAddress)
    {
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
        return overflow(instanceAddress, bucketAddress);
    }
    
    private native long overflow(long instanceAddress, long bucketAddress);
    
    /**
     * Allocates new bucket and returns the bucket start address.
     *
     * @param newBucketId the new block id
     * @param newBucketDepth the new block depth
     * @return the new block address
     */
    public long allocateNewBucket(int newBucketId, int newBucketDepth)
    {        
        return allocateNewBucket(instanceAddress, newBucketId, newBucketDepth);
    }

    private native long allocateNewBucket(long instanceAddress, int newBucketId, int newBucketDepth);

    public void relocateBlock(long bucketAddress, int blockOffset, long newBucketAddress)
    {
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
        relocateBlock(instanceAddress, bucketAddress, blockOffset, newBucketAddress);
    }
    
    private native void relocateBlock(long instanceAddress, long bucketAddress, int blockOffset, long newBucketAddress);



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
