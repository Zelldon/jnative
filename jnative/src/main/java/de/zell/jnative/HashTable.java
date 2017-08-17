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
import static org.agrona.BufferUtil.ARRAY_BASE_OFFSET;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.agrona.BitUtil;
import org.agrona.UnsafeAccess;
import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class HashTable implements Closeable
{
    private static final Unsafe UNSAFE = UnsafeAccess.UNSAFE;

    private int length;
    private long realAddress;

    public HashTable(int tableSize)
    {
        length = Math.multiplyExact(tableSize, SIZE_OF_LONG);
        realAddress = UNSAFE.allocateMemory(length);
        clear();
    }

    @Override
    public void close() throws IOException
    {
        UNSAFE.freeMemory(realAddress);
    }

    public void clear()
    {
        UNSAFE.setMemory(realAddress, length, (byte) 0);
    }

    public int getLength()
    {
        return length;
    }

    public void resize(int tableSize)
    {
        tableSize = BitUtil.findNextPositivePowerOfTwo(tableSize);
        final int newLength = Math.multiplyExact(tableSize, SIZE_OF_LONG);
        if (newLength > length)
        {
            final int oldLength = length;
            length = newLength;
            realAddress = UNSAFE.reallocateMemory(realAddress, length);
            // hash table was duplicated the new indices should point to the
            // same corresponding buckets like there counter-part
            UNSAFE.copyMemory(realAddress, realAddress + oldLength, length - oldLength);
        }
    }

    // data

    public long getBucketAddress(int bucketId)
    {
        return UNSAFE.getLong(realAddress + (bucketId * SIZE_OF_LONG));
    }

    public void setBucketAddress(int bucketId, long address)
    {
        UNSAFE.putLong(realAddress + (bucketId * SIZE_OF_LONG), address);
    }

    // de-/serialize

    public void writeToStream(OutputStream outputStream, byte[] buffer) throws IOException
    {
        for (int offset = 0; offset < length; offset += buffer.length)
        {
            final int copyLength = Math.min(buffer.length, length - offset);
            UNSAFE.copyMemory(null, realAddress + offset, buffer, ARRAY_BASE_OFFSET, copyLength);
            outputStream.write(buffer, 0, copyLength);
        }
    }

    public void readFromStream(InputStream inputStream, byte[] buffer) throws IOException
    {
        int bytesRead = 0;
        for (int offset = 0; offset < length; offset += bytesRead)
        {
            final int readLength = Math.min(buffer.length, length - offset);
            bytesRead = inputStream.read(buffer, 0, readLength);

            if (bytesRead > 0)
            {
                UNSAFE.copyMemory(buffer, ARRAY_BASE_OFFSET, null, realAddress + offset, bytesRead);
            }
            else
            {
                throw new IOException("Unable to read full map buffer from input stream. " +
                    "Only read " + offset + " bytes but expected " + length + " bytes.");
            }
        }
    }
}
