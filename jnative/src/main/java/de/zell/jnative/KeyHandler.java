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

public interface KeyHandler
{
    void setKeyLength(int keyLength);

    long keyHashCode();

    boolean keyEquals(long keyAddr);
    
    void readKey(long blockAddr);
    
    void writeKey(long blockAddr);
//
//    long getKey();
//
//    void setKey(long key);

    int getKeyLength();
//    
//    byte[] getbytes();
//    
//    void wrap(MutableDirectBuffer buffer);

}
