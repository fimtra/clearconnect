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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A pool for holding canonical versions of objects.
 * <p>
 * Thread safe and equal by object reference.
 * 
 * @author Ramon Servadei
 */
public final class ObjectPool<T>
{
    private final String name;
    private final ConcurrentMap<T, T> pool;
    private final LowGcLinkedList<T> order;
    private final int maxSize;

    /**
     * Construct with unlimited size
     */
    public ObjectPool(String name)
    {
        this(name, 0);
    }

    /**
     * Construct with a maximum size. When the maximum size is reached, the oldest entry in the pool
     * is released to make room.
     */
    public ObjectPool(String name, int maxSize)
    {
        this.name = name;
        this.maxSize = maxSize;
        this.pool = new ConcurrentHashMap<T, T>();
        this.order = (maxSize > 0 ? new LowGcLinkedList<T>() : null);
    }

    /**
     * Intern the object into the pool. If the pool is limited in size and the pool does not contain
     * the argument, the oldest pool entry is evicted and the argument added as the newest member of
     * the pool.
     * 
     * @return the pooled version of the object (the same object if the object is the first instance
     *         of itself in the pool).
     */
    public T intern(T t)
    {
        if (t == null)
        {
            return t;
        }
        final T putIfAbsent = this.pool.putIfAbsent(t, t);
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
        return "ObjectPool[" + this.name + ", " + this.order.size() + "/" + this.maxSize + "]";
    }
}
