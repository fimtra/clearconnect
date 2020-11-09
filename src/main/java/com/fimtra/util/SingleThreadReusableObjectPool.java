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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * A pool of re-usable objects that assumes its only accessed by a single thread.
 * <p>
 * <b>NOT THREAD SAFE</b>
 *
 * @param <T>
 * @author Ramon Servadei
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
    final static List<WeakReference<AbstractReusableObjectPool<?>>> pools = new LowGcLinkedList<>();

    static
    {
        ThreadUtils.scheduleAtFixedRate(AbstractReusableObjectPool.class, () -> {
            final TreeMap<String, AbstractReusableObjectPool<?>> ordered = new TreeMap<>();
            synchronized (pools)
            {
                for (Iterator<WeakReference<AbstractReusableObjectPool<?>>> iterator =
                     pools.iterator(); iterator.hasNext(); )
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

                Log.log(AbstractReusableObjectPool.class, "TYPE[name get/offer available/size/max-size]");
                for (Map.Entry<String, AbstractReusableObjectPool<?>> entry : ordered.entrySet())
                {
                    Log.log(AbstractReusableObjectPool.class, entry.getValue().toString());
                }
            }
        }, 1, UtilProperties.Values.OBJECT_POOL_SIZE_LOG_PERIOD_MINS, TimeUnit.MINUTES);
    }

    final Object lock;
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

    AbstractReusableObjectPool(String name, IReusableObjectBuilder<T> builder,
            IReusableObjectFinalizer<T> finalizer, int maxSize)
    {
        this.name = name;
        this.pool = new Object[maxSize];
        this.poolLimit = this.pool.length - 2;
        this.builder = builder;
        this.finalizer = finalizer;
        this.lock = new Object();
        this.weakRef = new WeakReference<>(this);
        synchronized (pools)
        {
            pools.add(this.weakRef);
        }
    }

    public final void destroy()
    {
        synchronized (this.lock)
        {
            Arrays.fill(this.pool, null);
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
