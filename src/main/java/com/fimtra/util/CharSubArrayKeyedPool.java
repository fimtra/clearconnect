/*
 * Copyright (c) 2017 Ramon Servadei
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.util;

/**
 * Object pool keyed by {@link CharSubArray} instances
 * 
 * @author Ramon Servadei
 */
public abstract class CharSubArrayKeyedPool<T>
{
    /** The pool of objects keyed by the {@link CharSubArray} */
    final KeyedObjectPool<CharSubArray, T> pool;
    /** Reference to the backing object pool that holds the objects */
    final KeyedObjectPool<String, T> objectPool;

    public CharSubArrayKeyedPool(String name, int poolSize, KeyedObjectPool<String, T> objectPool)
    {
        this.pool = new KeyedObjectPool<>(name, poolSize);
        this.objectPool = objectPool;
    }

    public T get(char[] chars, int offset, int count)
    {
        // Note on concurrency: if multiple threads hit this method for the same string with
        // different char sub arrays, its ok, the end state is that the interned string will be
        // canonical held against a semantically equal CharSubArray

        final CharSubArray charArrRef = new CharSubArray(chars, offset, count);
        T object = this.pool.get(charArrRef);
        if (object == null)
        {
            final String string = new String(chars, offset, count);
            object = this.objectPool.intern(string, newInstance(string));
            // VERY IMPORTANT make a new, exact size, copy of the char array
            final char[] charArray = new char[count];
            System.arraycopy(chars, offset, charArray, 0, count);
            object = this.pool.intern(new CharSubArray(charArray, 0, charArray.length), object);
        }
        return object;
    }

    public abstract T newInstance(String string);
}
