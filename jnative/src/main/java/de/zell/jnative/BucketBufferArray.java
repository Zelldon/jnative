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
        allocateNewBucketBuffer(instanceAddress, newBucketBufferId);
    }

    private native void allocateNewBucketBuffer(long instanceAddress, int newBucketBufferID);


    public static native long readLong(long address);
    public static native void writeLong(long address, long value);

    // BUCKET BUFFER ARRAY ///////////////////////////////////////////////////////////////////////////
    
    protected static long getBucketAddress(int bucketBufferId, int bucketOffset)
    {
        long bucketAddress = 0;
        bucketAddress += (long) bucketBufferId << 32;
        bucketAddress += bucketOffset;
        return bucketAddress;
    }       
    
    public int getBucketBufferCount()
    {
        return getBucketBufferCount(instanceAddress);
    }
    
    private native int getBucketBufferCount(long instanceAddress);

    public int getBucketCount()
    {
        return getBucketCount(instanceAddress);
    }
    
    private native int getBucketCount(long instanceAddress);

    public long getBlockCount()
    {
        return getBlockCount(instanceAddress);
    }
    
    private native long getBlockCount(long instanceAddress);
    
    
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
        
    public long size()
    {
        return size(instanceAddress);
    }
    
    private native long size(long instanceAddress);

    public int getMaxBucketBufferLength()
    {
        return maxBucketBufferLength;
    }    

    public float getLoadFactor()
    {
        return getLoadFactor(instanceAddress);
    }

    private native float getLoadFactor(long instanceAddress);

    public int getMaxBucketLength()
    {
        return maxBucketLength;
    }
    
    
    private native int getBucketBufferBucketCount(long instanceAddress, int bucketBufferId);

    // BUCKET //////////////////////////////////////////////////////////////////////
    public int getBucketFillCount(long bucketAddress)
    {
        return getBucketFillCount(instanceAddress, bucketAddress);
    }
    
    private native int getBucketFillCount(long instanceAddress, long bucketAddress);
    
    public int getBucketLength(long bucketAddress)
    {
        return getBucketLength(instanceAddress, bucketAddress);
    }
    
    private native int getBucketLength(long instanceAddress, long bucketAddress);
//
    public long getBucketOverflowPointer(long bucketAddress)
    {
        return getBucketOverflowPointer(instanceAddress, bucketAddress);
    }
    
    private native long getBucketOverflowPointer(long instanceAddress, long bucketAddress);
    
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

    public int getFirstBlockOffset()
    {
        return BUCKET_DATA_OFFSET;
    }
    
    // BLOCK ///////////////////////////////////////////////////////////////////////////////////////////
    
    public int getBlockLength()
    {
        return BucketBufferArrayDescriptor.getBlockLength(maxKeyLength, maxValueLength);
    }

    public boolean keyEquals(KeyHandler keyhandler, long bucketAddress, int blockOffset)
    {
        final long blockAddress = getBlockAddress(instanceAddress, bucketAddress, blockOffset);
        return keyhandler.keyEquals(blockAddress);
    }
    
    public void readKey(KeyHandler keyHandler,long bucketAddress, int blockOffset)
    {
//        final long blockAddress = getBlockAddress(instanceAddress, bucketAddress, blockOffset);
//        keyHandler.readKey(blockAddress);

        keyHandler.readKey(readKey(instanceAddress, bucketAddress, blockOffset));
    }

    private native long readKey(long instanceAddress, long bucketAddress, int blockOffset);
    
    
    public void readValue(ValueHandler valueHandler, long bucketAddress, int blockOffset)
    {
//        final long blockAddress = getBlockAddress(instanceAddress, bucketAddress, blockOffset);
//        valueHandler.readValue(blockAddress + maxKeyLength);

        valueHandler.readValue(readValue(instanceAddress, bucketAddress, blockOffset));
    }

    private native long readValue(long instanceAddress, long bucketAddress, int blockOffset);
    
    public void updateValue(ValueHandler handler, long bucketAddress, int blockOffset)
    {
        final int valueLength = handler.getValueLength();
        if (valueLength > maxValueLength)
        {
            throw new IllegalArgumentException("Value can't exceed the max value length of " + maxValueLength);
        }

        final long blockAddress = getBlockAddress(instanceAddress, bucketAddress, blockOffset);
        handler.writeValue(blockAddress + maxKeyLength);
    }
    

    private native long getBlockAddress(long instanceAddress, long bucketAddress, int blockOffset);
    
    public boolean addBlock(long bucketAddress, KeyHandler keyhandler, ValueHandler valueHandler)
    {

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

    public void removeBlock(long bucketAddress, int blockOffset)
    {
        removeBlock(instanceAddress, bucketAddress, blockOffset);
    }
    
    private native void removeBlock(long instanceAddress, long bucketAddress, int blockOffset);
    
    public int getBucketId(long bucketAddress)
    {
        return getBucketId(instanceAddress, bucketAddress);
    }
    
    private native int getBucketId(long instanceAddress, long bucketAddress);
    
    protected void setBucketDepth(long bucketAddress, int newBlockDepth)
    {
        setBucketDepth(instanceAddress, bucketAddress, newBlockDepth);
    }
    
    private native void setBucketDepth(long instanceAddress, long bucketAddress, int newBlockDepth);
//
    
    public int getBucketDepth(long bucketAddress)
    {
        return getBucketDepth(instanceAddress, bucketAddress);
    }
    
    private native int getBucketDepth(long instanceAddress, long bucketAddress);
//
    public long overflow(long bucketAddress)
    {
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
        relocateBlock(instanceAddress, bucketAddress, blockOffset, newBucketAddress);
    }
    
    private native void relocateBlock(long instanceAddress, long bucketAddress, int blockOffset, long newBucketAddress);

    public String toString()
    {
        final StringBuilder builder = new StringBuilder();
        final long bucketBufferCount = getBucketBufferCount();
        for (int i = 0; i < bucketBufferCount; i++)
        {
            final int bucketCount = getBucketBufferBucketCount(instanceAddress, i);
            for (int j = 0; j < bucketCount; j++)
            {
                final int offset = BUCKET_BUFFER_HEADER_LENGTH + j * getMaxBucketLength();
                builder.append(toString(getBucketAddress(i, offset)))
                       .append("\n");
            }
        }
        return builder.toString();
    }

    private String toString(long bucketAddress)
    {
        final StringBuilder builder = new StringBuilder();
        int bucketFillCount = getBucketFillCount(bucketAddress);
        long address = getBucketOverflowPointer(bucketAddress);
        int count = 0;
        while (address != 0)
        {
            bucketFillCount += getBucketFillCount(address);
            address = getBucketOverflowPointer(address);
            count++;
        }

        builder.append("Bucket-")
               .append(getBucketId(bucketAddress))
               .append(" contains ")
               .append(getBlockCount() == 0 ? 0 : ((double) bucketFillCount / (double) getBlockCount()) * 100D)
               .append(" % of all blocks")
               .append(":[ Blocks:")
               .append(bucketFillCount)
               .append(" ,Overflow:")
               .append(count)
               .append("]");
        return builder.toString();
    }
}
