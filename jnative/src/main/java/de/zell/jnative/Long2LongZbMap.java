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

import de.zell.jnative.types.LongKeyHandler;
import de.zell.jnative.types.LongValueHandler;
import static org.agrona.BitUtil.SIZE_OF_LONG;



public class Long2LongZbMap extends ZbMap<LongKeyHandler, LongValueHandler>
{

    public Long2LongZbMap()
    {
        super(SIZE_OF_LONG, SIZE_OF_LONG);
    }

    public Long2LongZbMap(
            int tableSize,
            int blocksPerBucket)
    {
        super(tableSize, blocksPerBucket, SIZE_OF_LONG, SIZE_OF_LONG);
    }

    public long get(long key, long missingValue)
    {
        keyHandler.setKey(key);
        valueHandler.setValue(missingValue);
        get();
        return valueHandler.getValue();
    }

    public boolean put(long key, long value)
    {
        keyHandler.setKey(key);
        valueHandler.setValue(value);
        return put();
    }

    public long remove(long key, long missingValue)
    {        
        keyHandler.setKey(key);
        valueHandler.setValue(missingValue);
        remove();
        return valueHandler.getValue();
    }


}
