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

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A pool of re-usable objects.
 * <p>
 * Thread-safe and equal by object reference.
 * 
 * @author Ramon Servadei
 */
public final class ReusableObjectPool<T>
{
    /**
     * Builds a new instance of a reusable object for a pool
     * 
     * @author Ramon Servadei
     * @param <T>
     */
    public static interface IReusableObjectBuilder<T>
    {
        T newInstance();
    }

    /**
     * Resets a re-usable object when it is returned to the pool
     * 
     * @author Ramon Servadei
     * @param <T>
     */
    public static interface IReusableObjectFinalizer<T>
    {
        void reset(T instance);
    }

    final static List<WeakReference<ReusableObjectPool<?>>> pools =
        new LowGcLinkedList<WeakReference<ReusableObjectPool<?>>>();
    static
    {
        ThreadUtils.UTILS_EXECUTOR.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                synchronized (pools)
                {
                    for (Iterator<WeakReference<ReusableObjectPool<?>>> iterator =
                        pools.iterator(); iterator.hasNext();)
                    {
                        final WeakReference<ReusableObjectPool<?>> weakReference = iterator.next();
                        final ReusableObjectPool<?> object = weakReference.get();
                        if (object == null)
                        {
                            // GC'ed as no other references so remove
                            iterator.remove();
                        }
                        else
                        {
                            Log.log(ReusableObjectPool.class, object.toString());
                        }
                    }
                }
            }
        }, 1, UtilProperties.Values.OBJECT_POOL_SIZE_LOG_PERIOD_MINS, TimeUnit.MINUTES);
    }

    private final String name;
    private final LowGcLinkedList<T> pool;
    private final int maxSize;
    private final IReusableObjectBuilder<T> builder;
    private final IReusableObjectFinalizer<T> finalizer;
    private final WeakReference<ReusableObjectPool<?>> weakRef;

    /**
     * Construct with unlimited size
     */
    public ReusableObjectPool(String name, IReusableObjectBuilder<T> builder, IReusableObjectFinalizer<T> finalizer)
    {
        this(name, builder, finalizer, 0);
    }

    public ReusableObjectPool(String name, IReusableObjectBuilder<T> builder, IReusableObjectFinalizer<T> finalizer,
        int maxSize)
    {
        this.name = name;
        this.maxSize = maxSize;
        this.pool = new LowGcLinkedList<T>(maxSize);
        this.builder = builder;
        this.finalizer = finalizer;
        this.weakRef = new WeakReference<ReusableObjectPool<?>>(this);
        synchronized (pools)
        {
            pools.add(this.weakRef);
        }
    }

    public final void destroy()
    {
        synchronized (this)
        {
            this.pool.clear();
        }

        synchronized (pools)
        {
            pools.remove(this.weakRef);
        }
    }

    /**
     * Get a re-usable object from this pool, creating one if there is none
     */
    public T get()
    {
        final T instance;
        synchronized (this)
        {
            instance = this.pool.poll();
        }
        if (instance != null)
        {
            return instance;
        }
        return this.builder.newInstance();
    }

    /**
     * Return a re-usable object back to this pool (if the pool has space).
     * <p>
     * The object is reset regardless of whether it is added to the pool.
     * 
     * @see IReusableObjectFinalizer
     */
    public void offer(T instance)
    {
        this.finalizer.reset(instance);
        synchronized (this)
        {
            if (this.maxSize == 0 || this.pool.size < this.maxSize)
            {
                this.pool.addLast(instance);
            }
        }
    }

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + "[" + this.name + ", " + this.pool.size() + "/"
            + (this.maxSize == 0 ? "inf" : Integer.toString(this.maxSize)) + "]";
    }
}
