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
package de.zell.jnative.types;


import de.zell.jnative.BucketBufferArray;
import de.zell.jnative.KeyHandler;
import org.agrona.BitUtil;

@SuppressWarnings("restriction")
public class LongKeyHandler implements KeyHandler
{

    public long theKey;

    @Override
    public void setKeyLength(int keyLength)
    {
        // ignore
    }

    @Override
    public long keyHashCode()
    {
        return Long.hashCode(theKey);
    }

    @Override
    public String toString()
    {
        return Long.valueOf(theKey).toString();
    }

    @Override
    public int getKeyLength()
    {
        return BitUtil.SIZE_OF_LONG;
    }

    @Override
    public void readKey(long keyAddr)
    {
        theKey = keyAddr;
    }

    @Override
    public void writeKey(long keyAddr)
    {
        BucketBufferArray.writeLong(keyAddr, theKey);
    }

    @Override
    public boolean keyEquals(long keyAddr)
    {
        return BucketBufferArray.readLong(keyAddr) == theKey;
    }

    @Override
    public long getKey() {
        return theKey;
    }
    
    
}
