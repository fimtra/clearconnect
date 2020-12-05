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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IPublisherContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.executors.ContextExecutorFactory;
import com.fimtra.executors.ICoalescingRunnable;
import com.fimtra.executors.IContextExecutor;

/**
 * Provides coalescing behaviour for record updates to solve fast-producer scenarios.
 * <p>
 * Not all data types can be coalesced; data types that have atomic updates that need discrete processing
 * CANNOT have this pattern applied to them. In these scenarios, the producer must be throttled (by
 * controlling the rate at which calls to {@link IPublisherContext#publishAtomicChange(IRecord)} are made).
 * <p>
 * For situations where the data changes are coalescable (combinable) this is used as the listener in a call
 * to {@link IObserverContext#addObserver(IRecordListener, String...)} and will coalesce all record updates
 * (based on the record name) and notify its delegate listener with an aggregated {@link AtomicChange} from
 * all received changes up until the member {@link IContextExecutor} executes.
 * <p>
 * The listener has a "cache policy" that defines how to manage images (see {@link CachePolicyEnum} ). If the
 * policy specifies an image is needed then the listener needs to keep a cache of record image 'snapshots'.
 * This can cause a memory buildup so the listener can be constructed with a cache policy of keeping the
 * images or removing them when coalescing. Removing them will mean no memory is wasted for records that don't
 * update any longer but the trade-off is that when an update occurs and there is no image, the image has to
 * be cloned from the record in the {@link IRecordListener#onChange(IRecord, IRecordChange)} method; for large
 * records this can be counter productive, especially for high-frequency updating large records. If the {@link
 * CachePolicyEnum#NO_IMAGE_NEEDED} policy is used, then no image is stored and there is no memory concern;
 * the trade-off is that the delegate {@link IRecordListener} will never get an image, only the changes.
 * <p>
 * Depending on the constructor, the coalescing strategy will either be:
 * <ul>
 * <li>Context-based: coalescing will happen as quickly as possible based on the "context" of the
 * update. The delegate listener will receive updates as fast as the {@link IContextExecutor} can
 * process coalescing of pending updates.
 * <li>Time-based: coalescing of updates will happen at a guaranteed rate for all updates to each
 * record.
 * </ul>
 * <p>
 * A note about context-based coalescing: be careful when attaching a context-based coalescing
 * listener, that shares its {@link IContextExecutor} with other coalescing listeners, to multiple
 * {@link IObserverContext}s. In these cases, the coalescing context used to construct the listener
 * must express the combination of {observer-context,record} otherwise unexpected coalescing from
 * the other IObserverContexts can occur (this is due to the single {@link IContextExecutor} used to
 * coalesce all the records based on their "context").
 *
 * @author Ramon Servadei
 */
public class CoalescingRecordListener implements IRecordListener {

    private static final Function<String, List<IRecordChange>> createArrayList = k -> new ArrayList<>(1);

    private static Object getDefaultCoalescingContext()
    {
        return UUID.randomUUID();
    }

    private static IContextExecutor getDefaultCoalescingExecutor()
    {
        return ContextUtils.CORE_EXECUTOR;
    }

    /**
     * A strategy for handling coalescing of record updates
     *
     * @author Ramon Servadei
     */
    public interface ICoalescingStrategy {

        /**
         * Handle the update to the record
         *
         * @param name               the record name that has had a coalescable event
         * @param coalescingListener reference to the coalescing listener that holds pending updates for the
         *                           record
         */
        void handle(String name, CoalescingRecordListener coalescingListener);

    }

    /**
     * Coalescing of events at a specified period using the passed in {@link ScheduledExecutorService}
     *
     * @author Ramon Servadei
     */
    public static class TimedCoalescingStrategy extends ContextCoalescingStrategy {
        final long periodMillis;
        final Set<String> pending;

        TimedCoalescingStrategy(long periodMillis, IContextExecutor executor, Object coalescingContext)
        {
            super(executor, coalescingContext);
            this.periodMillis = periodMillis;
            this.pending = new HashSet<>();
        }

        @Override
        public void handle(final String name, final CoalescingRecordListener coalescingListener)
        {
            synchronized (this.pending)
            {
                if (this.pending.add(name))
                {
                    ContextExecutorFactory.get(TimedCoalescingStrategy.class).schedule(() -> {
                        synchronized (this.pending)
                        {
                            this.pending.remove(name);
                        }
                        super.handle(name, coalescingListener);
                    }, this.periodMillis, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    /**
     * Coalescing of record events as-fast-as-possible.
     *
     * @author Ramon Servadei
     */
    public static class ContextCoalescingStrategy implements ICoalescingStrategy {
        final Object coalescingContext;
        final IContextExecutor executor;

        ContextCoalescingStrategy(IContextExecutor executor, Object coalescingContext)
        {
            super();
            this.coalescingContext = coalescingContext;
            this.executor = executor;
        }

        @Override
        public void handle(String name, CoalescingRecordListener coalescingListener)
        {
            this.executor.execute(
                    coalescingListener.new CoalescingRecordUpdateRunnable(name, this.coalescingContext));
        }
    }

    public enum CachePolicyEnum {
        /**
         * Keep the cached image after coalescing - this will provide better performance at the cost of memory
         * allocated
         */
        KEEP_IMAGE_ON_COALESCE,
        /**
         * Remove the cached image after coalescing - saves memory but can incur performance penalties if
         * records are large and update frequently
         */
        REMOVE_IMAGE_ON_COALESCE,
        /**
         * No caching of image needed - only used if the delegate {@link IRecordListener} for the {@link
         * CoalescingRecordListener} does not need the {@link IRecord} image argument in the {@link
         * IRecordListener#onChange(IRecord, IRecordChange)}
         */
        NO_IMAGE_NEEDED;

        Record getImage(Map<String, Record> cachedImages, String name)
        {
            switch(this)
            {
                case KEEP_IMAGE_ON_COALESCE:
                    return cachedImages.get(name);
                case REMOVE_IMAGE_ON_COALESCE:
                    return cachedImages.remove(name);
                case NO_IMAGE_NEEDED:
                    return null;
            }
            throw new UnsupportedOperationException("Unhandled policy type " + this);
        }
    }

    /**
     * Handles the logic to coalesce multiple {@link AtomicChange} objects and notify a single {@link
     * IRecordListener}
     *
     * @author Ramon Servadei
     */
    final class CoalescingRecordUpdateRunnable implements ICoalescingRunnable {
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
            Record image = null;
            if (CoalescingRecordListener.this.cachePolicy == CachePolicyEnum.NO_IMAGE_NEEDED || (image =
                    CoalescingRecordListener.this.cachePolicy.getImage(
                            CoalescingRecordListener.this.cachedImages, this.name)) != null)
            {
                final List<IRecordChange> changes;
                synchronized (CoalescingRecordListener.this.lock)
                {
                    changes = CoalescingRecordListener.this.cachedAtomicChanges.remove(this.name);
                }
                if (changes != null)
                {
                    final AtomicChange mergedAtomicChange = new AtomicChange(this.name);

                    mergedAtomicChange.coalesce(changes);
                    if (image != null)
                    {
                        image.applyChangeAndSetSequence(mergedAtomicChange);
                    }
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

    final Object lock;
    final IRecordListener delegate;
    final CachePolicyEnum cachePolicy;
    final ICoalescingStrategy strategy;
    final Map<String, Record> cachedImages;
    final Map<String, List<IRecordChange>> cachedAtomicChanges;

    /**
     * Construct a "context-based" coalescing record listener instance with a cache policy of {@link
     * CachePolicyEnum#KEEP_IMAGE_ON_COALESCE} and default coalescing components.
     *
     * @param delegate the delegate record listener that will be notified using the executor
     */
    public CoalescingRecordListener(IRecordListener delegate)
    {
        this(getDefaultCoalescingExecutor(), delegate, getDefaultCoalescingContext(),
                CachePolicyEnum.KEEP_IMAGE_ON_COALESCE);
    }

    /**
     * Construct a "context-based" coalescing record listener instance with a cache policy of {@link
     * CachePolicyEnum#KEEP_IMAGE_ON_COALESCE}
     *
     * @param coalescingExecutor the {@link IContextExecutor} to use to coalesce updates
     * @param delegate           the delegate record listener that will be notified using the executor
     * @param coalescingContext  the context to coalesce on - this can be the record name but for multi-source
     *                           updates, the context should identify the source and record name
     */
    public CoalescingRecordListener(IContextExecutor coalescingExecutor, IRecordListener delegate,
            Object coalescingContext)
    {
        this(coalescingExecutor, delegate, coalescingContext, CachePolicyEnum.KEEP_IMAGE_ON_COALESCE);
    }

    /**
     * Construct a "context-based" coalescing record listener.
     *
     * @param coalescingExecutor the {@link IContextExecutor} to use to coalesce updates
     * @param delegate           the delegate record listener that will be notified using the executor
     * @param coalescingContext  the context to coalesce on - this can be the record name but for multi-source
     *                           updates, the context should identify the source and record name
     * @param cachePolicy        the cache policy, see {@link CachePolicyEnum#KEEP_IMAGE_ON_COALESCE}, {@link
     *                           CachePolicyEnum#REMOVE_IMAGE_ON_COALESCE} and {@link
     *                           CachePolicyEnum#NO_IMAGE_NEEDED}
     */
    public CoalescingRecordListener(IContextExecutor coalescingExecutor, IRecordListener delegate,
            Object coalescingContext, CachePolicyEnum cachePolicy)
    {
        this(delegate, new ContextCoalescingStrategy(coalescingExecutor, coalescingContext), cachePolicy);
    }

    /**
     * Construct a "time-based" coalescing record listener instance with a cache policy of {@link
     * CachePolicyEnum#KEEP_IMAGE_ON_COALESCE} using default coalescing components.
     *
     * @param periodMillis the coalescing period
     * @param delegate     the delegate record listener that will be notified using the executor
     */
    public CoalescingRecordListener(long periodMillis, IRecordListener delegate)
    {
        this(periodMillis, delegate, getDefaultCoalescingExecutor(), getDefaultCoalescingContext());
    }

    /**
     * Construct a "time-based" coalescing record listener instance with a cache policy of {@link
     * CachePolicyEnum#KEEP_IMAGE_ON_COALESCE}
     *
     * @param periodMillis       the coalescing period
     * @param delegate           the delegate record listener that will be notified using the executor
     * @param coalescingExecutor the {@link IContextExecutor} to use to coalesce updates
     * @param coalescingContext  the context to coalesce on - this can be the record name but for
     *                           multi-source
     */
    public CoalescingRecordListener(long periodMillis, IRecordListener delegate,
            IContextExecutor coalescingExecutor, Object coalescingContext)
    {
        this(periodMillis, delegate, coalescingExecutor, coalescingContext,
                CachePolicyEnum.KEEP_IMAGE_ON_COALESCE);
    }

    /**
     * Construct a "time-based" coalescing record listener instance using default coalescing components.
     *
     * @param periodMillis the coalescing period
     * @param delegate     the delegate record listener that will be notified using the executor
     * @param cachePolicy  the cache policy, see {@link CachePolicyEnum#KEEP_IMAGE_ON_COALESCE}, {@link
     *                     CachePolicyEnum#REMOVE_IMAGE_ON_COALESCE} and {@link CachePolicyEnum#NO_IMAGE_NEEDED}
     */
    public CoalescingRecordListener(long periodMillis, IRecordListener delegate, CachePolicyEnum cachePolicy)
    {
        this(periodMillis, delegate, getDefaultCoalescingExecutor(), getDefaultCoalescingContext(),
                CachePolicyEnum.KEEP_IMAGE_ON_COALESCE);
    }

    /**
     * Construct a "time-based" coalescing record listener instance.
     *
     * @param periodMillis       the coalescing period
     * @param delegate           the delegate record listener that will be notified using the executor
     * @param coalescingExecutor the {@link IContextExecutor} to use to coalesce updates
     * @param coalescingContext  the context to coalesce on - this can be the record name but for multi-source
     *                           updates, the context should identify the source and record name
     * @param cachePolicy        the cache policy, see {@link CachePolicyEnum#KEEP_IMAGE_ON_COALESCE}, {@link
     *                           CachePolicyEnum#REMOVE_IMAGE_ON_COALESCE} and {@link
     *                           CachePolicyEnum#NO_IMAGE_NEEDED}
     */
    public CoalescingRecordListener(long periodMillis, IRecordListener delegate,
            IContextExecutor coalescingExecutor, Object coalescingContext, CachePolicyEnum cachePolicy)
    {
        this(delegate, new TimedCoalescingStrategy(periodMillis, coalescingExecutor, coalescingContext),
                cachePolicy);
    }

    /**
     * Construct a coalescing record instance using the given strategy for coalescing.
     *
     * @param delegate    the delegate record listener that will be notified using the executor
     * @param strategy    the strategy for coalescing
     * @param cachePolicy the cache policy, see {@link CachePolicyEnum#KEEP_IMAGE_ON_COALESCE}, {@link
     *                    CachePolicyEnum#REMOVE_IMAGE_ON_COALESCE} and {@link CachePolicyEnum#NO_IMAGE_NEEDED}
     */
    @SuppressWarnings("unchecked")
    public CoalescingRecordListener(IRecordListener delegate, ICoalescingStrategy strategy,
            CachePolicyEnum cachePolicy)
    {
        super();
        this.cachedImages = cachePolicy == CachePolicyEnum.NO_IMAGE_NEEDED ? Collections.EMPTY_MAP :
                new ConcurrentHashMap<>(2);
        this.cachedAtomicChanges = new ConcurrentHashMap<>(2);
        this.delegate = delegate;
        this.strategy = strategy;
        this.cachePolicy = cachePolicy;
        this.lock = new Object();
    }

    @Override
    public void onChange(final IRecord imageCopy, final IRecordChange atomicChange)
    {
        final String name = imageCopy.getName();
        synchronized (this.lock)
        {
            this.cachedAtomicChanges.computeIfAbsent(name, createArrayList).add(atomicChange);

            switch(this.cachePolicy)
            {
                case KEEP_IMAGE_ON_COALESCE:
                case REMOVE_IMAGE_ON_COALESCE:
                    if (!this.cachedImages.containsKey(name))
                    {
                        this.cachedImages.put(name, Record.snapshot(imageCopy));
                    }
                    break;
                case NO_IMAGE_NEEDED:
                    break;
            }
        }
        this.strategy.handle(name, this);
    }

    @Override
    public String toString()
    {
        return "CoalescingRecordListener [strategy=" + this.strategy + ", cachePolicy=" + this.cachePolicy
                + ", cacheSize=" + this.cachedImages.size() + ", delegate=" + this.delegate + "]";
    }
}