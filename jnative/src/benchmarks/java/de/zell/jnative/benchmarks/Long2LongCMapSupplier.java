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


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Christopher Zell <zelldon91@gmail.com>
 */
    
package de.zell.jnative.benchmarks;

import java.io.IOException;
import de.zell.jnative.Long2LongZbMap;
import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
public class Long2LongCMapSupplier
{
    Long2LongZbMap map;

    @Setup(Level.Iteration)
    public void createmap() throws IOException
    {
        map = new Long2LongZbMap();
    }

    @TearDown(Level.Iteration)
    public void closemap()
    {
        map.close();
    }

}
