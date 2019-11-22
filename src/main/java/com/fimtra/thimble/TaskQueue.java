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
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import com.fimtra.util.CollectionUtils;
import com.fimtra.util.LowGcLinkedList;
import com.fimtra.util.SingleThreadReusableObjectPool;

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

    private final static int SEQUENTIAL_TASKS_MAX_POOL_SIZE =
        Integer.parseInt(System.getProperty("thimble.sequentialTasksMaxPoolSize", "1000"));
    private final static int COALESCING_TASKS_MAX_POOL_SIZE =
        Integer.parseInt(System.getProperty("thimble.coalescingTasksMaxPoolSize", "1000"));

    static interface InternalTaskQueue<T> extends Runnable
    {
        void offer(T t);

        void onTaskFinished();
    }

    /**
     * Holds all {@link ISequentialRunnable} objects for the same execution context. Each call to
     * {@link #run()} will dequeue the next runnable from an internal list and execute it.
     * 
     * @author Ramon Servadei
     */
    final class SequentialTasks implements InternalTaskQueue<ISequentialRunnable>
    {
        final Deque<ISequentialRunnable> sequentialTasks = new LowGcLinkedList<>(2);
        int size;
        Object context;
        TaskStatistics stats;
        boolean active;

        SequentialTasks()
        {
            super();
        }

        /**
         * @return <code>true</code> if it was activated, <code>false</code> if it is already active
         */
        boolean activate()
        {
            if (!this.active)
            {
                this.active = true;
                return true;
            }
            return false;
        }

        @Override
        public void run()
        {
            ISequentialRunnable item = null;
            try
            {
                synchronized (this.sequentialTasks)
                {
                    item = this.sequentialTasks.removeFirst();
                    if (item != null)
                    {
                        this.size--;
                    }
                }
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

        @Override
        public void onTaskFinished()
        {
            this.stats.itemExecuted();
            if (this.size > 0)
            {
                TaskQueue.this.queue.offer(this);
            }
            else
            {
                TaskQueue.this.sequentialTasksPerContext.remove(this.context);
                TaskQueue.this.sequentialTasksPool.offer(this);
            }
        }

        // NOTE: offer is called by a thread that synchronizes on TaskQueue.this.lock
        @Override
        public void offer(ISequentialRunnable latest)
        {
            this.stats.itemSubmitted();
            synchronized (this.sequentialTasks)
            {
                this.sequentialTasks.offer(latest);
                this.size++;
            }
        }

        @Override
        public String toString()
        {
            synchronized (this.sequentialTasks)
            {
                return "SequentialTasks [" + this.context + ", size=" + this.size + "]";
            }
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
        ICoalescingRunnable latestTask;
        Object context;
        TaskStatistics stats;
        boolean active;

        CoalescingTasks()
        {
            super();
        }

        /**
         * @return <code>true</code> if it was activated, <code>false</code> if it is already active
         */
        boolean activate()
        {
            if (!this.active)
            {
                this.active = true;
                return true;
            }
            return false;
        }

        @Override
        public void run()
        {
            ICoalescingRunnable task;
            synchronized (this)
            {
                task = this.latestTask;
                this.latestTask = null;
            }
            task.run();
        }

        @Override
        public void onTaskFinished()
        {
            this.stats.itemExecuted();
            if (this.latestTask != null)
            {
                TaskQueue.this.queue.offer(this);
            }
            else
            {
                TaskQueue.this.coalescingTasksPerContext.remove(this.context);
                TaskQueue.this.coalescingTasksPool.offer(this);
            }
        }

        // NOTE: offer is called by a thread that synchronizes on TaskQueue.this.lock
        @Override
        public void offer(ICoalescingRunnable latest)
        {
            this.stats.itemSubmitted();
            synchronized (this)
            {
                this.latestTask = latest;
            }
        }

        @Override
        public String toString()
        {
            return "CoalescingTasks [" + this.context + "]";
        }
    }

    final Queue<Runnable> queue = CollectionUtils.newDeque();
    final Map<Object, SequentialTasks> sequentialTasksPerContext = new HashMap<>();
    final Map<Object, CoalescingTasks> coalescingTasksPerContext = new HashMap<>();
    final Map<Object, TaskStatistics> sequentialTaskStatsPerContext = new ConcurrentHashMap<>();
    final Map<Object, TaskStatistics> coalescingTaskStatsPerContext = new ConcurrentHashMap<>();
    final TaskStatistics allCoalescingStats;
    final TaskStatistics allSequentialStats;
    final Object lock = new Object();

    final SingleThreadReusableObjectPool<SequentialTasks> sequentialTasksPool;
    final SingleThreadReusableObjectPool<CoalescingTasks> coalescingTasksPool;
    long sequentialTaskCreateCount;
    long coalescingTaskCreateCount;
    final String name;

    TaskQueue(String name)
    {
        this.name = name;
        this.sequentialTasksPool =
            new SingleThreadReusableObjectPool<>("sequential-" + name, () -> new SequentialTasks(), (instance) -> {
                instance.context = null;
                instance.stats = null;
                instance.active = false;
            }, SEQUENTIAL_TASKS_MAX_POOL_SIZE);
        this.coalescingTasksPool =
            new SingleThreadReusableObjectPool<>("coalescing-" + name, () -> new CoalescingTasks(), (instance) -> {
                instance.context = null;
                instance.stats = null;
                instance.active = false;
            }, COALESCING_TASKS_MAX_POOL_SIZE);
        this.allCoalescingStats = new TaskStatistics("Coalescing" + IContextExecutor.QUEUE_LEVEL_STATS);
        this.coalescingTaskStatsPerContext.put(IContextExecutor.QUEUE_LEVEL_STATS, this.allCoalescingStats);
        this.allSequentialStats = new TaskStatistics("Sequential" + IContextExecutor.QUEUE_LEVEL_STATS);
        this.sequentialTaskStatsPerContext.put(IContextExecutor.QUEUE_LEVEL_STATS, this.allSequentialStats);
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
                sequentialTasks = this.sequentialTasksPool.get();
                sequentialTasks.context = context;
                sequentialTasks.stats = stats;
                this.sequentialTasksPerContext.put(context, sequentialTasks);
            }

            sequentialTasks.offer((ISequentialRunnable) runnable);
            if (sequentialTasks.activate())
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
                    coalescingTasks = this.coalescingTasksPool.get();
                    coalescingTasks.context = context;
                    coalescingTasks.stats = stats;
                    this.coalescingTasksPerContext.put(context, coalescingTasks);
                }

                coalescingTasks.offer((ICoalescingRunnable) runnable);
                if (coalescingTasks.activate())
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
            new HashMap<>(this.sequentialTaskStatsPerContext.size());
        Map.Entry<Object, TaskStatistics> entry = null;
        Object key = null;
        TaskStatistics value = null;
        for (Iterator<Map.Entry<Object, TaskStatistics>> it =
            this.sequentialTaskStatsPerContext.entrySet().iterator(); it.hasNext();)
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
            new HashMap<>(this.coalescingTaskStatsPerContext.size());
        Map.Entry<Object, TaskStatistics> entry = null;
        Object key = null;
        TaskStatistics value = null;
        for (Iterator<Map.Entry<Object, TaskStatistics>> it =
            this.coalescingTaskStatsPerContext.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            key = entry.getKey();
            value = entry.getValue();
            stats.put(key, value.intervalFinished());
        }
        return stats;
    }
}
