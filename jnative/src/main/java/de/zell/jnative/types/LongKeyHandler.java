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

import de.zell.jnative.KeyHandler;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

@SuppressWarnings("restriction")
public class LongKeyHandler implements KeyHandler
{
    
    private MutableDirectBuffer unsafeBuffer = new UnsafeBuffer(new byte[BitUtil.SIZE_OF_LONG]);

    @Override
    public void setKeyLength(int keyLength)
    {
        // ignore
    }

    @Override
    public long keyHashCode()
    {
        return Long.hashCode(unsafeBuffer.getLong(0));
    }

    @Override
    public String toString()
    {
        return Long.valueOf(unsafeBuffer.getLong(0)).toString();
    }

    @Override
    public int getKeyLength()
    {
        return BitUtil.SIZE_OF_LONG;
    }

    @Override
    public void setKey(long key)
    {
        unsafeBuffer.putLong(0, key);
    }

    @Override
    public long getKey()
    {
        return unsafeBuffer.getLong(0);
    }

    @Override
    public boolean keyEquals(long keyAddr)
    {
        return false;
    }

    @Override
    public byte[] getbytes() {
        return unsafeBuffer.byteArray();
    }

    @Override
    public void wrap(MutableDirectBuffer buffer) {
        unsafeBuffer = buffer;
    }
    
    
}
