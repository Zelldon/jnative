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

import de.zell.jnative.ValueHandler;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

@SuppressWarnings("restriction")
public class LongValueHandler implements ValueHandler
{

    private MutableDirectBuffer directBuffer = new UnsafeBuffer(new byte[BitUtil.SIZE_OF_LONG]);
    
    @Override
    public int getValueLength()
    {
        return BitUtil.SIZE_OF_LONG;
    }

    @Override
    public long getValue()
    {
        return directBuffer.getLong(0);
    }

    @Override
    public void setValue(long value)
    {
        directBuffer.putLong(0, value);
    }

    @Override
    public byte[] getBytes() {
        return directBuffer.byteArray();
    }

    @Override
    public void wrap(MutableDirectBuffer buffer) {
        directBuffer = buffer;
    }
    
    

}