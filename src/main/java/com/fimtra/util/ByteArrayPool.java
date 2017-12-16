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

import com.fimtra.util.ReusableObjectPool.IReusableObjectBuilder;
import com.fimtra.util.ReusableObjectPool.IReusableObjectFinalizer;
import com.fimtra.util.UtilProperties.Names;

/**
 * A pool of re-usable byte[] instances. The instances are held in pools in an internal array. Each
 * pool is at the index of the size of arrays it manages. An internal pool manages byte[] instances
 * of the same size and all sizes in the {@link ByteArrayPool} are powers of 2. Each pool of byte[]
 * is limited to a fixed size.
 * 
 * @see Names#BYTE_ARRAY_POOL_SIZE
 * @author Ramon Servadei
 */
public class ByteArrayPool
{
    @SuppressWarnings("unchecked")
    static final ReusableObjectPool<byte[]>[] POOLS = new ReusableObjectPool[2048 + 1];

    /**
     * Get a byte[] that can hold at least the specified size.
     * <p>
     * NOTE: the returned array size will be sized to a power of 2 that is enough to hold the
     * requested size.
     * 
     * @param size
     *            the size needed for the array
     * @return an array sized to the next power of 2 beyond the size. If requested size is beyond
     *         the limits of the pool (2048) then a new byte[] is returned with the exact size.
     */
    public static byte[] get(final int size)
    {
        final int index = getIndex(size);
        if (index < POOLS.length)
        {
            ReusableObjectPool<byte[]> pool = POOLS[index];
            if (pool == null)
            {
                pool = new ReusableObjectPool<byte[]>("byte[" + index + "]", new IReusableObjectBuilder<byte[]>()
                {
                    @Override
                    public byte[] newInstance()
                    {
                        return new byte[index];
                    }
                }, new IReusableObjectFinalizer<byte[]>()
                {
                    @Override
                    public void reset(byte[] instance)
                    {
                    }
                }, UtilProperties.Values.BYTE_ARRAY_POOL_SIZE, ReusableObjectPool.MULTI_THREADED);
                POOLS[index] = pool;
            }
            return pool.get();
        }
        return new byte[size];
    }

    /**
     * Return an array to the pool. If the array size is not a power of 2, it is discarded.
     */
    public static void offer(byte[] array)
    {
        if (array.length < POOLS.length)
        {
            final ReusableObjectPool<byte[]> pool = POOLS[array.length];
            if (pool != null)
            {
                pool.offer(array);
            }
        }
    }

    static int getIndex(int size)
    {
        // find a power of 2 that is greater than size
        int index = 1;
        do
        {
            index <<= 1;
        }
        while ((size >>= 1) > 0);

        return index;
    }

}