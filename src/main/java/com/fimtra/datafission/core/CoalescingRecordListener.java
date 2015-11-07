/*
 * Copyright (c) 2014 Ramon Servadei 
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
package com.fimtra.datafission.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IPublisherContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue;
import com.fimtra.thimble.ICoalescingRunnable;
import com.fimtra.thimble.ThimbleExecutor;

/**
 * Provides coalescing behaviour for record updates to solve fast-producer scenarios.
 * <p>
 * Not all data types can be coalesced; data types that have atomic updates that need discrete
 * processing CANNOT have this pattern applied to them. In these scenarios, the producer must be
 * throttled (by controlling the rate at which calls to
 * {@link IPublisherContext#publishAtomicChange(IRecord)} are made).
 * <p>
 * Be careful when attaching an instance of this listener to multiple sources and using the same
 * executor. In these cases, the coalescing context used to construct the listener must express the
 * combination of {source,record} otherwise unexpected coalescing from the other sources can occur.
 * <p>
 * For situations where the data changes are coalescable (combinable) this is used as the listener
 * in a call to {@link IObserverContext#addObserver(IRecordListener, String...)} and will coalesce
 * all record updates (based on the record name) and notify its delegate listener with an aggregated
 * {@link AtomicChange} from all received changes up until the member {@link ThimbleExecutor}
 * executes.
 * <p>
 * The listener needs to keep a cache of record image 'snapshots'. This can cause a memory buildup
 * so the listener can be constructed with a cache policy of keeping the images or removing them
 * when coalsecing. Removing them will mean no memory is wasted for records that don't update
 * anylonger but the trade-off is that when an update occurs and there is no image, the image has to
 * be cloned from the record in the {@link IRecordListener#onChange(IRecord, IRecordChange)} method;
 * for large records this can be counter productive, especially for high-frequency updating large
 * records.
 * 
 * @author Ramon Servadei
 */
public class CoalescingRecordListener implements IRecordListener
{
    public enum CachePolicyEnum
    {
        /**
         * Keep the cached image after coalescing - this will provide better performance at the cost
         * of memory allocated
         */
        KEEP_ON_COALESCE,
        /**
         * Remove the cached image after coalescing - saves memory but can incur performance
         * penalties if records are large and update frequently
         */
        REMOVE_ON_COALESCE;

        IRecord handle(ConcurrentMap<String, IRecord> cachedImages, String name)
        {
            switch(this)
            {
                case KEEP_ON_COALESCE:
                    return cachedImages.get(name);
                case REMOVE_ON_COALESCE:
                    return cachedImages.remove(name);
            }
            throw new UnsupportedOperationException("Unhandled policy type " + this);
        }
    }

    /**
     * Handles the logic to coalesce multiple {@link AtomicChange} objects and notify a single
     * {@link IRecordListener}
     * 
     * @author Ramon Servadei
     */
    final class CoalescingRecordUpdateRunnable implements ICoalescingRunnable
    {
        final String name;
        final Object coalescingContext;

        CoalescingRecordUpdateRunnable(String name, Object coalescingContext)
        {
            this.name = name;
            this.coalescingContext = coalescingContext;
        }

        @Override
        public void run()
        {
            final IRecord image =
                CoalescingRecordListener.this.cachePolicy.handle(CoalescingRecordListener.this.cachedImages, this.name);
            if (image != null)
            {
                final List<IRecordChange> changes;
                CoalescingRecordListener.this.lock.lock();
                try
                {
                    changes = CoalescingRecordListener.this.cachedAtomicChanges.remove(this.name);
                }
                finally
                {
                    CoalescingRecordListener.this.lock.unlock();
                }
                if (changes != null)
                {
                    final Map<String, IValue> putEntries = new HashMap<String, IValue>();
                    final Map<String, IValue> overwrittenEntries = new HashMap<String, IValue>();
                    final Map<String, IValue> removedEntries = new HashMap<String, IValue>();
                    final AtomicChange mergedAtomicChange =
                        new AtomicChange(this.name, putEntries, overwrittenEntries, removedEntries);

                    mergedAtomicChange.coalesce(changes);
                    mergedAtomicChange.applyCompleteAtomicChangeToRecord(image);
                    CoalescingRecordListener.this.delegate.onChange(image, mergedAtomicChange);
                }
            }
        }

        @Override
        public Object context()
        {
            return this.coalescingContext;
        }
    }

    final Lock lock;
    final Object coalescingContext;
    final ThimbleExecutor executor;
    final IRecordListener delegate;
    final CachePolicyEnum cachePolicy;
    final ConcurrentMap<String, IRecord> cachedImages;
    final ConcurrentMap<String, List<IRecordChange>> cachedAtomicChanges;

    /**
     * Construct an instance with a cache policy of {@link CachePolicyEnum#KEEP_ON_COALESCE}
     * 
     * @param coalescingExecutor
     *            the {@link ThimbleExecutor} to use to coalesce updates
     * @param delegate
     *            the delegate record listener that will be notified using the executor
     * @param coalescingContext
     *            the context to coalesce on - this can be the record name but for multi-source
     *            updates, the context should identify the source and record name
     */
    public CoalescingRecordListener(ThimbleExecutor coalescingExecutor, IRecordListener delegate,
        Object coalescingContext)
    {
        this(coalescingExecutor, delegate, coalescingContext, CachePolicyEnum.KEEP_ON_COALESCE);
    }

    /**
     * @param coalescingExecutor
     *            the {@link ThimbleExecutor} to use to coalesce updates
     * @param delegate
     *            the delegate record listener that will be notified using the executor
     * @param coalescingContext
     *            the context to coalesce on - this can be the record name but for multi-source
     *            updates, the context should identify the source and record name
     * @param cachePolicy
     *            the cache policy, see {@link CachePolicyEnum#KEEP_ON_COALESCE} and
     *            {@link CachePolicyEnum#REMOVE_ON_COALESCE}
     */
    public CoalescingRecordListener(ThimbleExecutor coalescingExecutor, IRecordListener delegate,
        Object coalescingContext, CachePolicyEnum cachePolicy)
    {
        super();
        this.cachedImages = new ConcurrentHashMap<String, IRecord>(2);
        this.cachedAtomicChanges = new ConcurrentHashMap<String, List<IRecordChange>>(2);
        this.executor = coalescingExecutor;
        this.delegate = delegate;
        this.coalescingContext = coalescingContext;
        this.cachePolicy = cachePolicy;
        this.lock = new ReentrantLock();
    }

    @Override
    public void onChange(final IRecord imageCopy, final IRecordChange atomicChange)
    {
        final String name = imageCopy.getName();
        this.lock.lock();
        try
        {
            List<IRecordChange> list = this.cachedAtomicChanges.get(name);
            if (list == null)
            {
                list = new ArrayList<IRecordChange>(1);
                this.cachedAtomicChanges.put(name, list);
            }
            list.add(atomicChange);
            if (!this.cachedImages.containsKey(name))
            {
                this.cachedImages.put(name, Record.snapshot(imageCopy));
            }
        }
        finally
        {
            this.lock.unlock();
        }

        this.executor.execute(new CoalescingRecordUpdateRunnable(name, this.coalescingContext));
    }

    @Override
    public String toString()
    {
        return "CoalescingRecordListener [coalescingContext=" + this.coalescingContext + ", cachePolicy="
            + this.cachePolicy + ", cacheSize=" + this.cachedImages.size() + ", delegate=" + this.delegate + "]";
    }
}