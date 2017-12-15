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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A pool of re-usable objects.
 * <p>
 * The pool is constructed with an {@link IThreadLogic} instance which determines whether it is
 * thread-safe.
 * 
 * @see #SINGLE_THREADED
 * @see #MULTI_THREADED
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

    /**
     * Encapsulates threading logic for accessing a pool.
     * 
     * @author Ramon Servadei
     */
    private static interface IThreadLogic
    {
        <T> T get(ReusableObjectPool<T> target);

        <T> void offer(ReusableObjectPool<T> target, T instance);
    }

    /**
     * No thread-safety, used for pools that are only accessed by a single thread.
     */
    public static final IThreadLogic SINGLE_THREADED = new IThreadLogic()
    {
        @Override
        public <T> T get(ReusableObjectPool<T> target)
        {
            final T instance = target.doGet();
            if (instance != null)
            {
                return instance;
            }
            return target.builder.newInstance();
        }

        @Override
        public <T> void offer(ReusableObjectPool<T> target, T instance)
        {
            target.finalizer.reset(instance);
            target.doOffer(instance);
        }
    };

    /**
     * Provides thread-safe multi-thread access to a pool.
     */
    public static final IThreadLogic MULTI_THREADED = new IThreadLogic()
    {
        @Override
        public <T> T get(ReusableObjectPool<T> target)
        {
            target.lock.lock();
            try
            {
                final T instance = target.doGet();
                if (instance != null)
                {
                    return instance;
                }
            }
            finally
            {
                target.lock.unlock();
            }
            return target.builder.newInstance();
        }

        @Override
        public <T> void offer(ReusableObjectPool<T> target, T instance)
        {
            target.finalizer.reset(instance);
            target.lock.lock();
            try
            {
                target.doOffer(instance);
            }
            finally
            {
                target.lock.unlock();
            }
        }
    };

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

    final Lock lock;
    final IReusableObjectBuilder<T> builder;
    final IReusableObjectFinalizer<T> finalizer;
    private final String name;
    private final IThreadLogic threadLogic;
    private final WeakReference<ReusableObjectPool<?>> weakRef;
    private final Object[] pool;

    private int highest;
    private int poolPtr;

    public ReusableObjectPool(String name, IReusableObjectBuilder<T> builder, IReusableObjectFinalizer<T> finalizer,
        int maxSize, IThreadLogic threadLogic)
    {
        this.name = name;
        this.pool = new Object[maxSize];
        this.builder = builder;
        this.finalizer = finalizer;
        this.lock = new ReentrantLock();
        this.weakRef = new WeakReference<ReusableObjectPool<?>>(this);
        synchronized (pools)
        {
            pools.add(this.weakRef);
        }
        this.threadLogic = threadLogic;
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
    public T get()
    {
        return this.threadLogic.get(this);
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
        this.threadLogic.offer(this, instance);
    }

    public int getSize()
    {
        return this.pool.length;
    }

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + "[" + this.name + ", " + this.poolPtr + "/" + this.highest + "/"
            + (this.pool.length) + "]";
    }

    void doOffer(T instance)
    {
        if (this.pool[this.poolPtr] == null)
        {
            this.pool[this.poolPtr] = instance;
        }
        else
        {
            if (this.poolPtr < this.pool.length - 2)
            {
                this.pool[++this.poolPtr] = (instance);
            }
            if (this.poolPtr > this.highest)
            {
                this.highest = this.poolPtr;
            }
        }
    }

    @SuppressWarnings("unchecked")
    T doGet()
    {
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
