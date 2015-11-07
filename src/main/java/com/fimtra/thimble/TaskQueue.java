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
package com.fimtra.thimble;

import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fimtra.util.CollectionUtils;

/**
 * An unbounded queue that internally manages the order of {@link Runnable} tasks that are submitted
 * via the {@link #offer_callWhilstHoldingLock(Runnable)} method. The
 * {@link #poll_callWhilstHoldingLock()} method will return the most appropriate task.
 * <p>
 * Generally, runnables are returned from the queue in a FIFO manner. Runnables extending
 * {@link ISequentialRunnable} or {@link ICoalescingRunnable} will be returned from the queue in a
 * manner that fits the interface description, e.g. for {@link ISequentialRunnable}, the runnable is
 * only returned if no other runnable (for the same context) is executing, thus maintaining the
 * contract of sequentially executing tasks.
 * <p>
 * <b>This is thread safe.</b>
 * 
 * @author Ramon Servadei
 */
final class TaskQueue
{
    /** By default, the queue tracks task statistics at a queue level, not at a per-context level */
    private final static boolean GENERATE_STATISTICS_PER_CONTEXT =
        Boolean.getBoolean("thimble.generateStatisticsPerContext");

    private interface InternalTaskQueue<T> extends Runnable
    {
        void offer(T t);
    }

    /**
     * Holds all {@link ISequentialRunnable} objects for the same execution context. Each call to
     * {@link #run()} will dequeue the next runnable from an internal list and execute it.
     * 
     * @author Ramon Servadei
     */
    final class SequentialTasks implements InternalTaskQueue<ISequentialRunnable>
    {
        private final Deque<ISequentialRunnable> sequentialTasks = new LinkedBlockingDeque<ISequentialRunnable>();
        private final Object context;
        private final TaskStatistics stats;

        SequentialTasks(Object context, TaskStatistics stats)
        {
            super();
            this.context = context;
            this.stats = stats;
        }

        @Override
        public void run()
        {
            ISequentialRunnable item = null;
            try
            {
                try
                {
                    item = this.sequentialTasks.removeFirst();
                }
                catch (NoSuchElementException e)
                {
                    // empty tasks
                }
                if (item != null)
                {
                    item.run();
                }
            }
            finally
            {
                TaskQueue.this.lock.lock();
                try
                {
                    this.stats.itemExecuted();
                    if (this.sequentialTasks.size() > 0)
                    {
                        TaskQueue.this.queue.offer(this);
                    }
                    else
                    {
                        TaskQueue.this.sequentialTasksInQueue.remove(this);
                        TaskQueue.this.sequentialTasksPerContext.remove(this.context);
                    }
                }
                finally
                {
                    TaskQueue.this.lock.unlock();
                }
            }
        }

        @Override
        public void offer(ISequentialRunnable latest)
        {
            this.stats.itemSubmitted();
            this.sequentialTasks.offer(latest);
        }

        @Override
        public String toString()
        {
            return "SequentialTasks [" + this.context + ", size=" + this.sequentialTasks.size() + "]";
        }
    }

    /**
     * Holds all {@link ICoalescingRunnable} objects for the same execution context. Each call to
     * {@link #run()} will execute the latest {@link ICoalescingRunnable} for the context.
     * 
     * @author Ramon Servadei
     */
    final class CoalescingTasks implements InternalTaskQueue<ICoalescingRunnable>
    {
        private final AtomicReference<ICoalescingRunnable> latestTask;
        private final Object context;
        private final TaskStatistics stats;

        CoalescingTasks(Object context, TaskStatistics stats)
        {
            this.context = context;
            this.stats = stats;
            this.latestTask = new AtomicReference<ICoalescingRunnable>();
        }

        @Override
        public void run()
        {
            try
            {
                this.latestTask.getAndSet(null).run();
            }
            finally
            {
                TaskQueue.this.lock.lock();
                try
                {
                    this.stats.itemExecuted();
                    if (this.latestTask.get() != null)
                    {
                        TaskQueue.this.queue.offer(this);
                    }
                    else
                    {
                        TaskQueue.this.coalesingTasksInQueue.remove(this);
                        TaskQueue.this.coalescingTasksPerContext.remove(this.context);
                    }
                }
                finally
                {
                    TaskQueue.this.lock.unlock();
                }
            }
        }

        @Override
        public void offer(ICoalescingRunnable latest)
        {
            this.stats.itemSubmitted();
            this.latestTask.set(latest);
        }

        @Override
        public String toString()
        {
            return "CoalescingTasks [" + this.context + "]";
        }
    }

    final Queue<Runnable> queue = CollectionUtils.newDeque();
    final Map<Object, SequentialTasks> sequentialTasksPerContext = new HashMap<Object, SequentialTasks>();
    final Map<Object, CoalescingTasks> coalescingTasksPerContext = new HashMap<Object, CoalescingTasks>();
    final Map<Object, TaskStatistics> sequentialTaskStatsPerContext = new ConcurrentHashMap<Object, TaskStatistics>();
    final Map<Object, TaskStatistics> coalescingTaskStatsPerContext = new ConcurrentHashMap<Object, TaskStatistics>();
    final Set<SequentialTasks> sequentialTasksInQueue = new HashSet<SequentialTasks>();
    final Set<CoalescingTasks> coalesingTasksInQueue = new HashSet<CoalescingTasks>();
    final TaskStatistics allCoalescingStats;
    final TaskStatistics allSequentialStats;
    final Lock lock = new ReentrantLock();

    {

        this.allCoalescingStats = new TaskStatistics("Coalescing" + ThimbleExecutor.QUEUE_LEVEL_STATS);
        this.coalescingTaskStatsPerContext.put(ThimbleExecutor.QUEUE_LEVEL_STATS, this.allCoalescingStats);
        this.allSequentialStats = new TaskStatistics("Sequential" + ThimbleExecutor.QUEUE_LEVEL_STATS);
        this.sequentialTaskStatsPerContext.put(ThimbleExecutor.QUEUE_LEVEL_STATS, this.allSequentialStats);
    }

    /**
     * Add the runnable to the queue, ordering it as appropriate for any annotation present on the
     * runnable.
     */
    void offer_callWhilstHoldingLock(Runnable runnable)
    {

        if (runnable instanceof ISequentialRunnable)
        {
            final ISequentialRunnable sequentialRunnable = (ISequentialRunnable) runnable;
            final Object context = sequentialRunnable.context();
            SequentialTasks sequentialTasks = this.sequentialTasksPerContext.get(context);
            if (sequentialTasks == null)
            {
                TaskStatistics stats;
                if (GENERATE_STATISTICS_PER_CONTEXT)
                {
                    stats = this.sequentialTaskStatsPerContext.get(context);
                    if (stats == null)
                    {
                        stats = new TaskStatistics(context);
                        this.sequentialTaskStatsPerContext.put(context, stats);
                    }
                }
                else
                {
                    stats = this.allSequentialStats;
                }
                sequentialTasks = new SequentialTasks(context, stats);
                this.sequentialTasksPerContext.put(context, sequentialTasks);
            }

            sequentialTasks.offer((ISequentialRunnable) runnable);
            if (this.sequentialTasksInQueue.add(sequentialTasks))
            {
                this.queue.offer(sequentialTasks);
            }
        }
        else
        {
            if (runnable instanceof ICoalescingRunnable)
            {
                final ICoalescingRunnable coalescingRunnable = (ICoalescingRunnable) runnable;
                final Object context = coalescingRunnable.context();
                CoalescingTasks coalescingTasks = this.coalescingTasksPerContext.get(context);
                if (coalescingTasks == null)
                {
                    TaskStatistics stats;
                    if (GENERATE_STATISTICS_PER_CONTEXT)
                    {
                        stats = this.coalescingTaskStatsPerContext.get(context);
                        if (stats == null)
                        {
                            stats = new TaskStatistics(context);
                            this.coalescingTaskStatsPerContext.put(context, stats);
                        }
                    }
                    else
                    {
                        stats = this.allCoalescingStats;
                    }
                    coalescingTasks = new CoalescingTasks(context, stats);
                    this.coalescingTasksPerContext.put(context, coalescingTasks);
                }

                coalescingTasks.offer((ICoalescingRunnable) runnable);
                if (this.coalesingTasksInQueue.add(coalescingTasks))
                {
                    this.queue.offer(coalescingTasks);
                }
            }
            else
            {
                this.queue.offer(runnable);
                return;
            }
        }
    }

    /**
     * @see Queue#poll()
     */
    Runnable poll_callWhilstHoldingLock()
    {
        return this.queue.poll();
    }

    /**
     * @return a copy of the internal statistics per sequential task context
     */
    Map<Object, TaskStatistics> getSequentialTaskStatistics()
    {
        HashMap<Object, TaskStatistics> stats =
            new HashMap<Object, TaskStatistics>(this.sequentialTaskStatsPerContext.size());
        Map.Entry<Object, TaskStatistics> entry = null;
        Object key = null;
        TaskStatistics value = null;
        for (Iterator<Map.Entry<Object, TaskStatistics>> it = this.sequentialTaskStatsPerContext.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            key = entry.getKey();
            value = entry.getValue();
            stats.put(key, value.intervalFinished());
        }
        return stats;
    }

    /**
     * @return a copy of the internal statistics per coalescing task context
     */
    Map<Object, TaskStatistics> getCoalescingTaskStatistics()
    {
        HashMap<Object, TaskStatistics> stats =
            new HashMap<Object, TaskStatistics>(this.coalescingTaskStatsPerContext.size());
        Map.Entry<Object, TaskStatistics> entry = null;
        Object key = null;
        TaskStatistics value = null;
        for (Iterator<Map.Entry<Object, TaskStatistics>> it = this.coalescingTaskStatsPerContext.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            key = entry.getKey();
            value = entry.getValue();
            stats.put(key, value.intervalFinished());
        }
        return stats;
    }
}
