/*
 * Copyright (c) 2013 Ramon Servadei 
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.fimtra.util.LazyObject.IDestructor;

/**
 * Utility that caches data and notifies listeners of a specific type when data is added or removed.
 * Listeners are notified either synchronously or asynchronously, depending on which constructor is
 * used.
 * <p>
 * This maintains an internal cache of the data that has been added/removed. The
 * {@link #getCacheSnapshot()} method returns a <b>clone</b> of the cache data so is expensive to
 * call.
 * <p>
 * A notifying cache should be equal by object reference only.
 * 
 * @author Ramon Servadei
 */
public abstract class NotifyingCache<LISTENER_CLASS, DATA>
{
    final static Executor IMAGE_NOTIFIER =
        new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
            ThreadUtils.newDaemonThreadFactory("image-notifier"), new ThreadPoolExecutor.DiscardPolicy());
    
    static final Executor SYNCHRONOUS_EXECUTOR = new Executor()
    {
        @Override
        public void execute(Runnable command)
        {
            command.run();
        }
    };

    final Map<String, DATA> cache;
    final Executor executor;
    final Lock readLock;
    final Lock writeLock;
    List<LISTENER_CLASS> listeners;
    final IDestructor<NotifyingCache<LISTENER_CLASS, DATA>> destructor;

    /**
     * Construct a <b>synchronously</b> updating instance
     */
    public NotifyingCache()
    {
        this(new IDestructor<NotifyingCache<LISTENER_CLASS, DATA>>()
        {
            @Override
            public void destroy(NotifyingCache<LISTENER_CLASS, DATA> ref)
            {
                // noop
            }
        }, SYNCHRONOUS_EXECUTOR);
    }
    
    /**
     * Construct a <b>synchronously</b> updating instance
     */
    public NotifyingCache(IDestructor<NotifyingCache<LISTENER_CLASS, DATA>> destructor)
    {
        this(destructor, SYNCHRONOUS_EXECUTOR);
    }
    
    /**
     * Construct an <b>asynchronously</b> updating instance using the passed in executor. <b>The
     * executor MUST be a single threaded executor. A multi-threaded executor may produce
     * out-of-sequence updates.</b>
     */
    public NotifyingCache(Executor executor)
    {
        this(new IDestructor<NotifyingCache<LISTENER_CLASS, DATA>>()
        {
            @Override
            public void destroy(NotifyingCache<LISTENER_CLASS, DATA> ref)
            {
                // noop
            }
        }, executor);
    }
    
    /**
     * Construct an <b>asynchronously</b> updating instance using the passed in executor. <b>The
     * executor MUST be a single threaded executor. A multi-threaded executor may produce
     * out-of-sequence updates.</b>
     */
    public NotifyingCache(IDestructor<NotifyingCache<LISTENER_CLASS, DATA>> destructor, Executor executor)
    {
        this.destructor = destructor;
        this.cache = new LinkedHashMap<String, DATA>(2);
        this.listeners = new ArrayList<LISTENER_CLASS>(1);
        this.executor = executor;
        final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        this.readLock = reentrantReadWriteLock.readLock();
        this.writeLock = reentrantReadWriteLock.writeLock();
    }

    /**
     * @param key
     *            the key for the data to retrieve
     * @return the data held in the cache for the key, may be null
     */
    public final DATA get(String key)
    {
        this.readLock.lock();
        try
        {
            return this.cache.get(key);
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    /**
     * @return a <b>copy</b> of the set of keys within the cache
     */
    public final Set<String> keySet()
    {
        this.readLock.lock();
        try
        {
            return new HashSet<String>(this.cache.keySet());
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    /**
     * @param key
     *            the key to look for
     * @return <code>true</code> if the cache contains an entry for the key
     */
    public final boolean containsKey(String key)
    {
        this.readLock.lock();
        try
        {
            return this.cache.containsKey(key);
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    /**
     * @return a <b>cloned</b> version of the data
     */
    public final Map<String, DATA> getCacheSnapshot()
    {
        this.readLock.lock();
        try
        {
            return new LinkedHashMap<String, DATA>(this.cache);
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    /**
     * Add the listener and notify it with the current data using the internal executor
     * 
     * @return <code>true</code> if the listener was added, <code>false</code> otherwise
     */
    public final boolean addListener(final LISTENER_CLASS listener)
    {
        final AtomicBoolean result = new AtomicBoolean(false);
        try
        {
            final CountDownLatch latch = new CountDownLatch(1);

            final Runnable addTask = new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        if (listener == null || NotifyingCache.this.listeners.contains(listener))
                        {
                            return;
                        }

                        final Map<String, DATA> clone;

                        // hold the lock and add the listener in the task to ensure the listener is
                        // added and notified without clashing with a concurrent update - this is
                        // mainly for the synchronous executor uses
                        NotifyingCache.this.writeLock.lock();
                        try
                        {
                            final List<LISTENER_CLASS> copy =
                                new ArrayList<LISTENER_CLASS>(NotifyingCache.this.listeners);
                            result.set(copy.add(listener));
                            NotifyingCache.this.listeners = copy;

                            latch.countDown();

                            if (result.get())
                            {
                                clone = new LinkedHashMap<String, DATA>(NotifyingCache.this.cache);
                            }
                            else
                            {
                                clone = null;
                            }

                            if (result.get())
                            {
                                Map.Entry<String, DATA> entry = null;
                                for (@SuppressWarnings("null")
                                Iterator<Map.Entry<String, DATA>> it = clone.entrySet().iterator(); it.hasNext();)
                                {
                                    entry = it.next();
                                    try
                                    {
                                        notifyListenerDataAdded(listener, entry.getKey(), entry.getValue());
                                    }
                                    catch (Exception e)
                                    {
                                        Log.log(NotifyingCache.this,
                                            "Could not notify " + ObjectUtils.safeToString(listener)
                                                + " with initial image " + ObjectUtils.safeToString(entry),
                                            e);
                                    }
                                }
                            }
                        }
                        finally
                        {
                            NotifyingCache.this.writeLock.unlock();
                        }
                    }
                    finally
                    {
                        latch.countDown();
                    }
                }
            };

            if (this.executor == SYNCHRONOUS_EXECUTOR)
            {
                // use the image-notifier executor (unbounded threads) to handle initial image
                // notification - prevents any chance of stalling due to any deadlock in the alien
                // method notifyListenerDataAdded
                IMAGE_NOTIFIER.execute(addTask);
            }
            else
            {
                this.executor.execute(addTask);
            }

            latch.await();
        }
        catch (InterruptedException e)
        {
            // ignored
        }
        return result.get();
    }

    public final boolean removeListener(LISTENER_CLASS listener)
    {
        this.writeLock.lock();
        try
        {
            if (!this.listeners.contains(listener))
            {
                return false;
            }

            List<LISTENER_CLASS> copy = new ArrayList<LISTENER_CLASS>(this.listeners);
            final boolean removed = copy.remove(listener);
            // take another copy so we have the correct size
            copy = new ArrayList<LISTENER_CLASS>(copy);
            this.listeners = copy;

            return removed;
        }
        finally
        {
            this.writeLock.unlock();
        }
    }

    /**
     * Notify all registered listeners with the new data. The notification is done using the
     * internal executor.
     * 
     * @return <code>true</code> if the data was added (it was not already contained),
     *         <code>false</code> if it was already in the cache (no listeners are notified in this
     *         case).
     */
    public final boolean notifyListenersDataAdded(final String key, final DATA data)
    {
        final boolean added;
        final List<LISTENER_CLASS> listenersToNotify;
        this.writeLock.lock();
        try
        {
            added = !is.eq(this.cache.put(key, data), data);
            listenersToNotify = this.listeners;
            if (added)
            {
                this.executor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for (LISTENER_CLASS listener : listenersToNotify)
                        {
                            try
                            {
                                notifyListenerDataAdded(listener, key, data);
                            }
                            catch (Exception e)
                            {
                                Log.log(NotifyingCache.this, "Could not notify " + ObjectUtils.safeToString(listener)
                                    + " with ADD " + ObjectUtils.safeToString(data), e);
                            }
                        }
                    }
                });
            }
        }
        finally
        {
            this.writeLock.unlock();
        }

        return added;
    }

    /**
     * Notify all registered listeners that the data for this key is removed. The notification is
     * done using the internal executor.
     * 
     * @param the
     *            key for the data that is removed
     * @return <code>true</code> if the data was found and removed, <code>false</code> if it was not
     *         found (no listeners are notified in this case)
     */
    public final boolean notifyListenersDataRemoved(final String key)
    {
        final boolean removed;
        final List<LISTENER_CLASS> listenersToNotify;
        this.writeLock.lock();
        try
        {
            final DATA removedData = this.cache.remove(key);
            removed = removedData != null;
            listenersToNotify = this.listeners;
            if (removed)
            {
                this.executor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for (LISTENER_CLASS listener : listenersToNotify)
                        {
                            try
                            {
                                notifyListenerDataRemoved(listener, key, removedData);
                            }
                            catch (Exception e)
                            {
                                Log.log(NotifyingCache.this, "Could not notify " + ObjectUtils.safeToString(listener)
                                    + " with REMOVE " + ObjectUtils.safeToString(removedData), e);
                            }
                        }
                    }
                });
            }
        }
        finally
        {
            this.writeLock.unlock();
        }

        return removed;
    }

    public final void destroy()
    {
        this.destructor.destroy(this);
        this.listeners.clear();
        this.cache.clear();
    }

    @Override
    public final String toString()
    {
        return "NotifyingCache [cache.size=" + this.cache.size() + ", listener.size=" + this.listeners.size() + "]";
    }

    /**
     * Called after the data has been added to the cache.
     * 
     * @param key
     *            the key for the data that was added
     * @param data
     *            the data that was added
     */
    protected abstract void notifyListenerDataAdded(LISTENER_CLASS listener, String key, DATA data);

    /**
     * Called after the data has been removed from the cache.
     * 
     * @param key
     *            the key for the data that was removed
     * @param data
     *            the data that was removed
     */
    protected abstract void notifyListenerDataRemoved(LISTENER_CLASS listener, String key, DATA data);
}
