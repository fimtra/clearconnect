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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Utility that caches data and notifies listeners of a specific type when data is added or removed.
 * Listeners are notified either synchronously or asynchronously, depending on which constructor is
 * used.
 * <p>
 * The notifier maintains a cache of the data that has been added/removed and, as such, can be used
 * as the data cache; the {@link #getCacheSnapshot()} method returns a <b>read-only</b> view of the
 * cache data.
 * <p>
 * A notifying cache should be equal by object reference only.
 * 
 * @author Ramon Servadei
 */
public abstract class NotifyingCache<LISTENER_CLASS, DATA>
{
    final Map<String, DATA> cache;
    final Executor executor;
    final Lock readLock;
    final Lock writeLock;
    List<LISTENER_CLASS> listeners;

    /**
     * Construct a <b>synchronously</b> updating instance
     */
    public NotifyingCache()
    {
        this(new Executor()
        {
            @Override
            public void execute(Runnable command)
            {
                command.run();
            }
        });
    }

    /**
     * Construct an <b>asynchronously</b> updating instance using the passed in executor. <b>The
     * executor MUST be a single threaded executor. A multi-threaded executor may produce
     * out-of-sequence updates.</b>
     */
    public NotifyingCache(Executor executor)
    {
        this.cache = new LinkedHashMap<String, DATA>();
        this.listeners = new ArrayList<LISTENER_CLASS>();
        this.executor = executor;
        final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        this.readLock = reentrantReadWriteLock.readLock();
        this.writeLock = reentrantReadWriteLock.writeLock();
    }

    /**
     * @return a <b>cloned</b> version of the data
     */
    public Map<String, DATA> getCacheSnapshot()
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
    public boolean addListener(final LISTENER_CLASS listener)
    {
        this.writeLock.lock();
        try
        {
            if (listener == null || this.listeners.contains(listener))
            {
                return false;
            }

            List<LISTENER_CLASS> copy = new ArrayList<LISTENER_CLASS>(this.listeners);
            final boolean added = copy.add(listener);
            this.listeners = copy;

            if (added)
            {
                final Map<String, DATA> clone = new LinkedHashMap<String, DATA>(this.cache);
                this.executor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {

                        Map.Entry<String, DATA> entry = null;
                        for (Iterator<Map.Entry<String, DATA>> it = clone.entrySet().iterator(); it.hasNext();)
                        {
                            entry = it.next();
                            try
                            {
                                notifyListenerDataAdded(listener, entry.getKey(), entry.getValue());
                            }
                            catch (Exception e)
                            {
                                Log.log(NotifyingCache.this, "Could not notify " + ObjectUtils.safeToString(listener)
                                    + " with ADD " + ObjectUtils.safeToString(entry), e);
                            }
                        }
                    }
                });
            }
            return added;
        }
        finally
        {
            this.writeLock.unlock();
        }
    }

    public boolean removeListener(LISTENER_CLASS listener)
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
     * Notify all registered listeners with the new data using the internal executor
     * 
     * @return <code>true</code> if the data was added (it was not already contained),
     *         <code>false</code> if it was already in the cache (no listeners are notified in this
     *         case).
     */
    public boolean notifyListenersDataAdded(final String key, final DATA data)
    {
        this.writeLock.lock();
        try
        {
            final boolean added = !is.eq(this.cache.put(key, data), data);
            if (added)
            {
                final List<LISTENER_CLASS> listenersToNotify = this.listeners;
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
            return added;
        }
        finally
        {
            this.writeLock.unlock();
        }
    }

    /**
     * Notify all registered listeners with the data that was removed using the internal executor
     * 
     * @return <code>true</code> if the data was found and removed, <code>false</code> if it was not
     *         found (no listeners are notified in this case)
     */
    public boolean notifyListenersDataRemoved(final String key, final DATA data)
    {
        this.writeLock.lock();
        try
        {
            final boolean removed = this.cache.remove(key) != null;
            if (removed)
            {
                final List<LISTENER_CLASS> listenersToNotify = this.listeners;
                this.executor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for (LISTENER_CLASS listener : listenersToNotify)
                        {
                            try
                            {
                                notifyListenerDataRemoved(listener, key, data);
                            }
                            catch (Exception e)
                            {
                                Log.log(NotifyingCache.this, "Could not notify " + ObjectUtils.safeToString(listener)
                                    + " with REMOVE " + ObjectUtils.safeToString(data), e);
                            }
                        }
                    }
                });
            }
            return removed;
        }
        finally
        {
            this.writeLock.unlock();
        }
    }

    public void destroy()
    {
        this.listeners.clear();
        this.cache.clear();
    }

    @Override
    public String toString()
    {
        return "ListenerNotifier [data.size=" + this.cache.size() + ", listener.size=" + this.listeners.size() + "]";
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
