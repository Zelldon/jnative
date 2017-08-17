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
package de.zell.jnative.benchmarks;

import java.util.Random;

import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
public class RandomKeysSupplier
{
    long[] keys = new long[Benchmarks.DATA_SET_SIZE];

    @Setup
    public void generateKeys()
    {
        final Random random = new Random();

        for (int k = 0; k < keys.length; k++)
        {
            keys[k] = Math.abs(random.nextLong());
        }
    }


}
