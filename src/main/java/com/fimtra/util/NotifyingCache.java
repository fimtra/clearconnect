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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.fimtra.util.LazyObject.IDestructor;

/**
 * Utility that caches data and notifies listeners of a specific type when data is added or removed.
 * Listeners are notified asynchronously and with different threads; initial images occur on
 * separate threads to updates. There will be no concurrent or duplicate updates received.
 * <p>
 * This maintains an internal cache of the data that has been added/removed. The
 * {@link #getCacheSnapshot()} method returns a <b>clone</b> of the cache data so is expensive to
 * call.
 * <p>
 * <b>Threading:</b> all listeners are notified with initial images using an internal
 * {@link NotifyingCache#IMAGE_NOTIFIER} executor. After this, any additions/removals to/from the
 * cache are notified using the respective thread model used for cache construction.
 * <p>
 * A notifying cache should be equal by object reference only.
 * 
 * @author Ramon Servadei
 */
public abstract class NotifyingCache<LISTENER_CLASS, DATA>
{
    private static final Executor IMAGE_NOTIFIER =
        new ThreadPoolExecutor(1, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue<>(),
            ThreadUtils.newDaemonThreadFactory("image-notifier"), new ThreadPoolExecutor.DiscardPolicy());

    private static final Executor UPDATE_NOTIFIER =
        new ThreadPoolExecutor(1, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue<>(),
            ThreadUtils.newDaemonThreadFactory("update-notifier"), new ThreadPoolExecutor.DiscardPolicy());

    @SuppressWarnings("rawtypes")
    private static final IDestructor NOOP_DESTRUCTOR = (ref) -> {
        // noop
    };

    private static boolean isLatest(String key, Long sequence, Map<String, Long> notifySequences)
    {
        if (notifySequences != null && sequence != null)
        {
            final Long existing = notifySequences.get(key);
            if (existing == null || sequence.longValue() > existing.longValue())
            {
                notifySequences.put(key, sequence);
                return true;
            }
        }
        return false;
    }

    final Map<String, DATA> cache;
    /** Tracks the change sequence per data item in the cache */
    final Map<String, Long> sequences;
    final Executor executor;
    final Lock readLock;
    final Lock writeLock;
    List<LISTENER_CLASS> listeners;
    final IDestructor<NotifyingCache<LISTENER_CLASS, DATA>> destructor;
    /**
     * This tracks the notification sequence number per data key per listener.
     * <p>
     * This is populated when a listener is registered and prevents duplicate updates being sent to
     * the listener during its phase of receiving initial images whilst any concurrent updates may
     * also be occurring.
     */
    final Map<LISTENER_CLASS, Map<String, Long>> listenerSequences;

    /**
     * Holds the order for notifying tasks - ensures the executor can be multi-threaded and still
     * not lose update order
     */
    final List<Runnable> notifyTasks;
    final Runnable notifyingTasksRunner;
    /** Allows one update thread - blocks all images */
    final Lock updateLock;
    /** Allows multiple image notifications - blocks updates */
    final Lock imageLock;

    /**
     * Standard contructor
     */
    @SuppressWarnings("unchecked")
    public NotifyingCache()
    {
        this(NOOP_DESTRUCTOR, UPDATE_NOTIFIER);
    }

    /**
     * Construct with specific destructor
     */
    public NotifyingCache(IDestructor<NotifyingCache<LISTENER_CLASS, DATA>> destructor)
    {
        this(destructor, UPDATE_NOTIFIER);
    }

    /**
     * Construct using the passed in executor
     */
    @SuppressWarnings("unchecked")
    public NotifyingCache(Executor executor)
    {
        this(NOOP_DESTRUCTOR, executor);
    }

    /**
     * Construct using the passed in executor and destructor
     */
    public NotifyingCache(IDestructor<NotifyingCache<LISTENER_CLASS, DATA>> destructor, Executor executor)
    {
        this.destructor = destructor;
        this.cache = new LinkedHashMap<>(2);
        this.sequences = new HashMap<>();
        this.listeners = new ArrayList<LISTENER_CLASS>(1);
        this.notifyTasks = Collections.synchronizedList(new LowGcLinkedList<>());
        final Object runnerLock = new Object();
        this.notifyingTasksRunner = () -> {
            if (this.notifyTasks.size() > 0)
            {
                // lock to ensure only 1 task runs at any time (ensures ordering if the
                // executor is multi-threaded)
                synchronized (runnerLock)
                {
                    final Runnable task = this.notifyTasks.remove(0);
                    if (task != null)
                    {
                        try
                        {
                            task.run();
                        }
                        catch (Exception e)
                        {
                            Log.log(this, "Could not execute notification", e);
                        }
                    }
                }
            }
        };
        this.listenerSequences = Collections.synchronizedMap(new HashMap<>());
        this.executor = executor;

        final boolean fairLock = UtilProperties.Values.NOTIFYING_CACHE_FAIR_LOCK_POLICY;

        final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(fairLock);
        this.readLock = reentrantReadWriteLock.readLock();
        this.writeLock = reentrantReadWriteLock.writeLock();

        final ReentrantReadWriteLock notificationLock = new ReentrantReadWriteLock(fairLock);
        this.updateLock = notificationLock.writeLock();
        this.imageLock = notificationLock.readLock();
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
            return new HashSet<>(this.cache.keySet());
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
            return new LinkedHashMap<>(this.cache);
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

            final Runnable addTask = () -> {
                final Map<String, DATA> cacheSnapshot;
                final Map<String, Long> sequencesSnapshot;
                final Map<String, Long> notifySequence;

                // hold the lock and add the listener in the task to ensure the listener is
                // added and notified without clashing with a concurrent update
                this.writeLock.lock();
                try
                {
                    if (listener == null || this.listeners.contains(listener))
                    {
                        return;
                    }

                    notifySequence = new HashMap<>(this.cache.size());
                    this.listenerSequences.put(listener, notifySequence);

                    // add the listener
                    final List<LISTENER_CLASS> copy = new ArrayList<LISTENER_CLASS>(this.listeners);
                    result.set(copy.add(listener));
                    this.listeners = copy;

                    cacheSnapshot = new LinkedHashMap<>(this.cache);
                    sequencesSnapshot = new HashMap<>(this.sequences);
                }
                finally
                {
                    latch.countDown();
                    this.writeLock.unlock();
                }

                // NOTE: we hold the lock to ensure that after identifying the update is the latest,
                // the update is notified to the listener - prevents this scenario for thread
                // scheduling:
                // T1 - seq=4, is latest, will notify with value for seq=4
                // T2 - updates to seq=5
                // T2 - notifies listener with seq=5
                // T1 - notifies listener with seq=4 (!!!)
                this.imageLock.lock();
                try
                {
                    // notify the listener with the initial images - this will skip any that have
                    // been updated since the writeLock was released - in this case, the listener
                    // will have the latest version already notified
                    String key;
                    for (Map.Entry<String, DATA> entry : cacheSnapshot.entrySet())
                    {
                        key = entry.getKey();
                        if (isLatest(key, sequencesSnapshot.get(key), notifySequence))
                        {
                            safeNotifyAdd(key, entry.getValue(), listener, "INITIAL IMAGE");
                        }
                    }
                }
                finally
                {
                    this.imageLock.unlock();
                }
            };

            // use the image-notifier executor (unbounded threads) to handle initial image
            // notification - prevents any chance of stalling due to any deadlock in the alien
            // method notifyListenerDataAdded
            IMAGE_NOTIFIER.execute(addTask);

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

            List<LISTENER_CLASS> copy = new ArrayList<>(this.listeners);
            final boolean removed = copy.remove(listener);
            // take another copy so we have the correct size
            copy = new ArrayList<LISTENER_CLASS>(copy);
            this.listeners = copy;

            this.listenerSequences.remove(listener);

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
        Runnable command = null;
        this.writeLock.lock();
        try
        {
            added = !is.eq(this.cache.put(key, data), data);
            if (added)
            {
                final Long sequence = Long.valueOf(System.nanoTime());
                this.sequences.put(key, sequence);

                final List<LISTENER_CLASS> listenersSnapshot = this.listeners;

                command = () -> {
                    long start = System.nanoTime();
                    do
                    {
                        // we need to try for the lock - we must not lock out any imageLock
                        // operations until its free
                        if (this.updateLock.tryLock())
                        {
                            try
                            {
                                final List<LISTENER_CLASS> listenersToNotify = new LinkedList<>();
                                // find listeners that have not been notified with this update
                                // sequence - THIS PREVENTS DUPLICATE NOTIFICATIONS, typically with
                                // concurrent addListener calls and also skips redundant updates if
                                // the same key is updated in quick succession
                                synchronized (this.listenerSequences)
                                {
                                    for (LISTENER_CLASS listener : listenersSnapshot)
                                    {
                                        if (isLatest(key, sequence, this.listenerSequences.get(listener)))
                                        {
                                            listenersToNotify.add(listener);
                                        }
                                    }
                                }

                                for (LISTENER_CLASS listener : listenersToNotify)
                                {
                                    safeNotifyAdd(key, data, listener, "ADD");
                                }
                            }
                            finally
                            {
                                this.updateLock.unlock();
                            }
                            break;
                        }
                        else
                        {
                            start = updateLockFailed(key, start);
                        }
                    }
                    while (true);
                };

                // add the notifying task whilst holding the lock to ensure order of execution
                this.notifyTasks.add(command);
            }
        }
        finally
        {
            this.writeLock.unlock();
        }

        if (added)
        {
            this.executor.execute(this.notifyingTasksRunner);
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
        Runnable command = null;
        this.writeLock.lock();
        try
        {
            final DATA removedData = this.cache.remove(key);
            removed = removedData != null;
            if (removed)
            {
                this.sequences.remove(key);

                final List<LISTENER_CLASS> listenersToNotify = this.listeners;

                command = () -> {
                    long start = System.nanoTime();
                    do
                    {
                        if (this.updateLock.tryLock())
                        {
                            try
                            {
                                // remove the data key from all notification sequences held for all
                                // listeners
                                final Collection<Map<String, Long>> notificationSequences;
                                synchronized (this.listenerSequences)
                                {
                                    notificationSequences = new ArrayList<>(this.listenerSequences.values());
                                }
                                for (Map<String, Long> map : notificationSequences)
                                {
                                    // note: the map is only mutated whilst holding either the
                                    // updateLock or
                                    // imageLock
                                    map.remove(key);
                                }

                                for (LISTENER_CLASS listener : listenersToNotify)
                                {
                                    safeNotifyRemove(key, removedData, listener, "REMOVE");
                                }
                            }
                            finally
                            {
                                this.updateLock.unlock();
                            }
                            break;
                        }
                        else
                        {
                            start = updateLockFailed(key, start);
                        }
                    }
                    while (true);
                };

                // add the notifying task whilst holding the lock to ensure order of execution
                this.notifyTasks.add(command);
            }
        }
        finally

        {
            this.writeLock.unlock();
        }

        if (removed)
        {
            this.executor.execute(this.notifyingTasksRunner);
        }

        return removed;
    }

    private long updateLockFailed(final String key, long start)
    {
        LockSupport.parkNanos(10);

        if ((System.nanoTime() - start) / 1_000_000 > 1000)
        {
            Log.log(this, "Waiting > 1 second for updateLock to notify " + key);
            return System.nanoTime();
        }
        return start;
    }

    private final void safeNotifyAdd(final String key, final DATA data, LISTENER_CLASS listener, String action)
    {
        try
        {
            notifyListenerDataAdded(listener, key, data);
        }
        catch (Exception e)
        {
            handleException(key, data, listener, e, action);
        }
    }

    private final void safeNotifyRemove(final String key, final DATA data, LISTENER_CLASS listener, String action)
    {
        try
        {
            notifyListenerDataRemoved(listener, key, data);
        }
        catch (Exception e)
        {
            handleException(key, data, listener, e, action);
        }
    }

    private void handleException(String key, DATA data, LISTENER_CLASS listener, Exception e, String operation)
    {
        Log.log(this, "Could not notify " + ObjectUtils.safeToString(listener) + " with " + operation + " " + key + "="
            + ObjectUtils.safeToString(data), e);
    }

    public final void destroy()
    {
        try
        {
            this.destructor.destroy(this);
        }
        finally
        {
            this.writeLock.lock();
            try
            {
                // remove all data from the cache and trigger listeners
                for (String key : new HashSet<>(this.cache.keySet()))
                {
                    notifyListenersDataRemoved(key);
                }
                this.listeners = Collections.emptyList();
                this.cache.clear();
            }
            finally
            {
                this.writeLock.unlock();
            }
        }
    }

    @Override
    public final String toString()
    {
        return "NotifyingCache [cache.size=" + this.cache.size() + ", listener.size=" + this.listeners.size() + "]";
    }

    /**
     * Called after the data has been added to the cache.
     * <p>
     * <b>This can be called by multiple threads concurrently and must be thread-safe.</b>
     * 
     * @param key
     *            the key for the data that was added
     * @param data
     *            the data that was added
     */
    protected abstract void notifyListenerDataAdded(LISTENER_CLASS listener, String key, DATA data);

    /**
     * Called after the data has been removed from the cache.
     * <p>
     * <b>This can be called by multiple threads concurrently and must be thread-safe.</b>
     * 
     * @param key
     *            the key for the data that was removed
     * @param data
     *            the data that was removed
     */
    protected abstract void notifyListenerDataRemoved(LISTENER_CLASS listener, String key, DATA data);
}
