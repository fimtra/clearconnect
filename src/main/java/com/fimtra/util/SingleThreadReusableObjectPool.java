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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A pool of re-usable objects that assumes its only accessed by a single thread.
 * <p>
 * <b>NOT THREAD SAFE</b>
 * 
 * @author Ramon Servadei
 * @param <T>
 */
public final class SingleThreadReusableObjectPool<T> extends AbstractReusableObjectPool<T>
{
    public SingleThreadReusableObjectPool(String name, IReusableObjectBuilder<T> builder,
        IReusableObjectFinalizer<T> finalizer, int maxSize)
    {
        super(name, builder, finalizer, maxSize);
    }

    @Override
    public T get()
    {
        final T instance = doGet();
        if (instance != null)
        {
            return instance;
        }
        return this.builder.newInstance();
    }

    @Override
    public void offer(T instance)
    {
        doOffer(instance);
    }
}

/**
 * Base-class for a pool of re-usable objects.
 * <p>
 * 
 * @author Ramon Servadei
 */
abstract class AbstractReusableObjectPool<T>
{
    final static List<WeakReference<AbstractReusableObjectPool<?>>> pools =
        new LowGcLinkedList<WeakReference<AbstractReusableObjectPool<?>>>();
    static
    {
        ThreadUtils.UTILS_EXECUTOR.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                final TreeMap<String, AbstractReusableObjectPool<?>> ordered =
                    new TreeMap<String, AbstractReusableObjectPool<?>>();
                synchronized (pools)
                {
                    for (Iterator<WeakReference<AbstractReusableObjectPool<?>>> iterator =
                        pools.iterator(); iterator.hasNext();)
                    {
                        final WeakReference<AbstractReusableObjectPool<?>> weakReference = iterator.next();
                        final AbstractReusableObjectPool<?> object = weakReference.get();
                        if (object == null)
                        {
                            // GC'ed as no other references so remove
                            iterator.remove();
                        }
                        else
                        {
                            ordered.put(object.name, object);
                        }
                    }

                    Log.log(AbstractReusableObjectPool.class,
                        "TYPE[name get/offer available/size/max-size]");
                    for (Iterator<Map.Entry<String, AbstractReusableObjectPool<?>>> it =
                        ordered.entrySet().iterator(); it.hasNext();)
                    {
                        Log.log(AbstractReusableObjectPool.class, it.next().getValue().toString());
                    }
                }
            }
        }, 1, UtilProperties.Values.OBJECT_POOL_SIZE_LOG_PERIOD_MINS, TimeUnit.MINUTES);
    }

    final Lock lock;
    final IReusableObjectBuilder<T> builder;
    final IReusableObjectFinalizer<T> finalizer;
    final String name;
    private final WeakReference<AbstractReusableObjectPool<?>> weakRef;
    private final Object[] pool;
    private final int poolLimit;

    long getCount;
    long offerCount;
    int highest;
    int poolPtr;

    AbstractReusableObjectPool(String name, IReusableObjectBuilder<T> builder, IReusableObjectFinalizer<T> finalizer,
        int maxSize)
    {
        this.name = name;
        this.pool = new Object[maxSize];
        this.poolLimit = this.pool.length - 2;
        this.builder = builder;
        this.finalizer = finalizer;
        this.lock = new ReentrantLock();
        this.weakRef = new WeakReference<AbstractReusableObjectPool<?>>(this);
        synchronized (pools)
        {
            pools.add(this.weakRef);
        }
    }

    public final void destroy()
    {
        this.lock.lock();
        try
        {
            for (int i = 0; i < this.pool.length; i++)
            {
                this.pool[i] = null;
            }
        }
        finally
        {
            this.lock.unlock();
        }

        synchronized (pools)
        {
            pools.remove(this.weakRef);
        }
    }

    /**
     * Get a re-usable object from this pool, creating one if there is none
     */
    public abstract T get();

    /**
     * Return a re-usable object back to this pool (if the pool has space).
     * <p>
     * The object is reset regardless of whether it is added to the pool.
     * 
     * @see IReusableObjectFinalizer
     */
    public abstract void offer(T instance);

    public final int getSize()
    {
        return this.pool.length;
    }

    @Override
    public final String toString()
    {
        // note: we add 1 to the poolPtr and highest to represent these as 1-based (not as 0-based
        // for the array that they operate on).
        final String string = this.getClass().getSimpleName() + "[" + this.name + " " + this.getCount + "/"
            + this.offerCount + " " + (this.poolPtr + 1) + "/" + (this.highest + 1) + "/"
            + (this.pool.length) + "]";
        this.getCount = 0;
        this.offerCount = 0;
        return string;
    }

    final void doOffer(T instance)
    {
        this.offerCount++;
        if (this.pool[this.poolPtr] == null)
        {
            this.finalizer.reset(instance);
            this.pool[this.poolPtr] = instance;
        }
        else
        {
            if (this.poolPtr <= this.poolLimit)
            {
                this.finalizer.reset(instance);
                this.pool[++this.poolPtr] = instance;
                if (this.poolPtr > this.highest)
                {
                    this.highest = this.poolPtr;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    final T doGet()
    {
        this.getCount++;
        final T instance = (T) this.pool[this.poolPtr];
        if (instance != null)
        {
            this.pool[this.poolPtr] = null;
            if (this.poolPtr != 0)
            {
                this.poolPtr--;
            }
            return instance;
        }
        return null;
    }
}
