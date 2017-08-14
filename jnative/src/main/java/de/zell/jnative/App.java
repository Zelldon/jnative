/*
 * Copyright © ${project.inceptionYear} camunda services GmbH (info@camunda.com)
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

/**
 * Hello world!
 *
 */
public class App
{
    static
    {
        System.loadLibrary("nativeMap");
    }

    public static void main(String[] args)
    {
        final BucketBufferArray bucketBufferArray = new BucketBufferArray();
        final long address = bucketBufferArray.allocate(256);
        System.out.println("Allocated 256 bytes on " + address);

        bucketBufferArray.free(address);
        System.out.println("Free 256 bytes on " + address);
    }
}
