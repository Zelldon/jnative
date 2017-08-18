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

import de.zell.jnative.types.LongKeyHandler;
import de.zell.jnative.types.LongValueHandler;
import org.agrona.BitUtil;

/**
 *
 */
public class ZbMapMain
{

    public static final int DATA_COUNT = 20_000_000;
    
    public static void main(String [] args)
    {

        // given
        final Long2LongZbMap zbMap = new Long2LongZbMap();

        // when
        for (int i = 0; i < DATA_COUNT; i++)
        {
            zbMap.put(i, i);
        }

        // then
        for (int i = 0; i < DATA_COUNT; i++)
        {
            if (zbMap.get(i, -1) == -1)
            {
                throw new IllegalStateException("Missing value for key: " + i);
            }
        }
    }
}
