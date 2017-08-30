/*
 * Copyright (c) 2015 Ramon Servadei 
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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * A pool for holding canonical versions of objects held against a key.
 * <p>
 * Thread safe and equal by object reference.
 * 
 * @author Ramon Servadei
 */
public class KeyedObjectPool<K, T>
{
    final static List<KeyedObjectPool<?, ?>> pools = new LowGcLinkedList<KeyedObjectPool<?, ?>>();
    static
    {
        ThreadUtils.UTILS_EXECUTOR.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                synchronized (pools)
                {
                    for (KeyedObjectPool<?, ?> pool : pools)
                    {
                        Log.log(KeyedObjectPool.class, pool.toString());
                    }
                }
            }
        }, 1, UtilProperties.Values.OBJECT_POOL_SIZE_LOG_PERIOD_MINS, TimeUnit.MINUTES);
    }

    private final String name;
    private final ConcurrentMap<K, T> pool;
    private final LowGcLinkedList<T> order;
    private final int maxSize;

    /**
     * Construct with unlimited size
     */
    public KeyedObjectPool(String name)
    {
        this(name, 0);
    }

    /**
     * Construct with a maximum size. When the maximum size is reached, the oldest entry in the pool
     * is released to make room.
     */
    public KeyedObjectPool(String name, int maxSize)
    {
        this.name = name;
        this.maxSize = maxSize;
        this.pool = new ConcurrentHashMap<K, T>();
        this.order = (maxSize > 0 ? new LowGcLinkedList<T>() : null);
        synchronized (pools)
        {
            pools.add(this);
        }
    }

    public final void destroy()
    {
        synchronized (pools)
        {
            pools.remove(this);
        }
    }

    /**
     * Intern the object into the pool. If the pool is limited in size and the pool does not contain
     * the argument, the oldest pool entry is evicted and the argument added as the newest member of
     * the pool.
     * 
     * @return the pooled version of the object (the same object if the object is the first instance
     *         of itself in the pool).
     */
    public final T intern(K k, T t)
    {
        if (t == null)
        {
            return t;
        }
        final T putIfAbsent = this.pool.putIfAbsent(k, t);
        if (putIfAbsent == null)
        {
            if (this.order != null)
            {
                synchronized (this.order)
                {
                    this.order.add(t);
                    if (this.order.size > this.maxSize)
                    {
                        this.pool.remove(this.order.removeFirst());
                    }
                }
            }
            return t;
        }
        else
        {
            return putIfAbsent;
        }
    }

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + "[" + this.name + ", " + this.pool.size() + "/"
            + (this.maxSize == 0 ? "inf" : Integer.toString(this.maxSize)) + "]";
    }

    public final T get(K k)
    {
        return this.pool.get(k);
    }
}
