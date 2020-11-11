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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.util.CollectionUtils;
import com.fimtra.util.LowGcLinkedList;
import com.fimtra.util.SingleThreadReusableObjectPool;
import com.fimtra.util.SystemUtils;

/**
 * An unbounded queue that internally manages the order of {@link Runnable} tasks that are submitted via the
 * {@link #offer_callWhilstHoldingLock(Runnable)} method. The {@link #poll_callWhilstHoldingLock()} method
 * will return the most appropriate task.
 * <p>
 * Generally, runnables are returned from the queue in a FIFO manner. Runnables extending {@link
 * ISequentialRunnable} or {@link ICoalescingRunnable} will be returned from the queue in a manner that fits
 * the interface description, e.g. for {@link ISequentialRunnable}, the runnable is only returned if no other
 * runnable (for the same context) is executing, thus maintaining the contract of sequentially executing
 * tasks.
 * <p>
 * <b>This is thread safe.</b>
 *
 * @author Ramon Servadei
 */
final class TaskQueue {
    /**
     * By default, the queue tracks task statistics at a queue level, not at a per-context level
     */
    private final static boolean GENERATE_STATISTICS_PER_CONTEXT =
            SystemUtils.getProperty("thimble.generateStatisticsPerContext", false);

    private final static int SEQUENTIAL_TASKS_MAX_POOL_SIZE =
            SystemUtils.getPropertyAsInt("thimble.sequentialTasksMaxPoolSize", 1000);
    private final static int COALESCING_TASKS_MAX_POOL_SIZE =
            SystemUtils.getPropertyAsInt("thimble.coalescingTasksMaxPoolSize", 1000);

    interface InternalTaskQueue<T> extends Runnable {
        void offer(T t);

        void onTaskFinished();
    }

    /**
     * Holds all {@link ISequentialRunnable} objects for the same execution context. Each call to {@link
     * #run()} will dequeue the next runnable from an internal list and execute it.
     *
     * @author Ramon Servadei
     */
    final class SequentialTasks implements InternalTaskQueue<ISequentialRunnable> {
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
     * Holds all {@link ICoalescingRunnable} objects for the same execution context. Each call to {@link
     * #run()} will execute the latest {@link ICoalescingRunnable} for the context.
     *
     * @author Ramon Servadei
     */
    final class CoalescingTasks implements InternalTaskQueue<ICoalescingRunnable> {
        final AtomicReference<ICoalescingRunnable> latestTask = new AtomicReference<>();
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
            this.latestTask.getAndSet(null).run();
        }

        @Override
        public void onTaskFinished()
        {
            this.stats.itemExecuted();
            if (this.latestTask.get() != null)
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
            if(this.latestTask.getAndSet(latest) == null)
            {
                // only track submitted if there was no coalescing (else the qOverflow becomes skewed)
                this.stats.itemSubmitted();
            }
        }

        @Override
        public String toString()
        {
            return "CoalescingTasks [" + this.context + "]";
        }
    }

    enum QChangeTypeEnum {
        NONE, CONTEXT, RUNNABLE
    }

    final Queue<Runnable> queue = CollectionUtils.newDeque();
    final Map<Object, SequentialTasks> sequentialTasksPerContext = new HashMap<>();
    final Map<Object, CoalescingTasks> coalescingTasksPerContext = new HashMap<>();
    final Map<Object, TaskStatistics> sequentialTaskStatsPerContext = new ConcurrentHashMap<>();
    final Map<Object, TaskStatistics> coalescingTaskStatsPerContext = new ConcurrentHashMap<>();
    final Object lock = new Object();

    final SingleThreadReusableObjectPool<SequentialTasks> sequentialTasksPool;
    final SingleThreadReusableObjectPool<CoalescingTasks> coalescingTasksPool;
    final String name;

    /**
     * Tracks the last not-draining threshold size breach
     */
    int lastTriggerSize;

    TaskQueue(String name)
    {
        this.name = name;
        this.sequentialTasksPool =
                new SingleThreadReusableObjectPool<>("sequential-" + name, SequentialTasks::new,
                        (instance) -> {
                            instance.context = null;
                            instance.stats = null;
                            instance.active = false;
                        }, SEQUENTIAL_TASKS_MAX_POOL_SIZE);
        this.coalescingTasksPool =
                new SingleThreadReusableObjectPool<>("coalescing-" + name, CoalescingTasks::new,
                        (instance) -> {
                            instance.context = null;
                            instance.stats = null;
                            instance.active = false;
                        }, COALESCING_TASKS_MAX_POOL_SIZE);
    }

    /**
     * Add the runnable to the queue, ordering it as appropriate for any annotation present on the runnable.
     */
    QChangeTypeEnum offer_callWhilstHoldingLock(Runnable runnable)
    {
        QChangeTypeEnum qChange = QChangeTypeEnum.NONE;
        if (runnable instanceof ISequentialRunnable)
        {
            final ISequentialRunnable sequentialRunnable = (ISequentialRunnable) runnable;
            final Object context = sequentialRunnable.context();
            SequentialTasks sequentialTasks = this.sequentialTasksPerContext.get(context);
            if (sequentialTasks == null)
            {
                sequentialTasks = this.sequentialTasksPool.get();
                sequentialTasks.context = context;
                sequentialTasks.stats = this.sequentialTaskStatsPerContext.computeIfAbsent(
                        GENERATE_STATISTICS_PER_CONTEXT ? context : IContextExecutor.QUEUE_LEVEL_STATS,
                        TaskStatistics::new);
                this.sequentialTasksPerContext.put(context, sequentialTasks);
            }

            sequentialTasks.offer((ISequentialRunnable) runnable);
            if (sequentialTasks.activate())
            {
                this.queue.offer(sequentialTasks);
                qChange = QChangeTypeEnum.CONTEXT;
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
                    coalescingTasks = this.coalescingTasksPool.get();
                    coalescingTasks.context = context;
                    coalescingTasks.stats = this.coalescingTaskStatsPerContext.computeIfAbsent(
                            GENERATE_STATISTICS_PER_CONTEXT ? context : IContextExecutor.QUEUE_LEVEL_STATS,
                            TaskStatistics::new);
                    this.coalescingTasksPerContext.put(context, coalescingTasks);
                }

                coalescingTasks.offer((ICoalescingRunnable) runnable);
                if (coalescingTasks.activate())
                {
                    this.queue.offer(coalescingTasks);
                    qChange = QChangeTypeEnum.CONTEXT;
                }
            }
            else
            {
                this.queue.offer(runnable);
                qChange = QChangeTypeEnum.RUNNABLE;
            }
        }
        return qChange;
    }

    /**
     * @see Queue#poll()
     */
    Runnable poll_callWhilstHoldingLock()
    {
        return this.queue.poll();
    }

    /**
     * @return true if the queue is > 0 and has increased or remained constant since the last call
     */
    boolean isNotDraining_callWhilstHoldingLock()
    {
        final int currentSize = this.queue.size() + this.coalescingTasksPerContext.size()
                + this.sequentialTasksPerContext.size();
        try
        {
            return currentSize != 0 && currentSize >= this.lastTriggerSize;
        }
        finally
        {
            this.lastTriggerSize = currentSize;
        }
    }

    /**
     * @return a copy of the internal statistics per sequential task context
     */
    Map<Object, TaskStatistics> getSequentialTaskStatistics()
    {
        HashMap<Object, TaskStatistics> stats = new HashMap<>(this.sequentialTaskStatsPerContext.size());
        for (Map.Entry<Object, TaskStatistics> entry : this.sequentialTaskStatsPerContext.entrySet())
        {
            stats.put(entry.getKey(), entry.getValue().intervalFinished());
        }
        return stats;
    }

    /**
     * @return a copy of the internal statistics per coalescing task context
     */
    Map<Object, TaskStatistics> getCoalescingTaskStatistics()
    {
        HashMap<Object, TaskStatistics> stats = new HashMap<>(this.coalescingTaskStatsPerContext.size());
        for (Map.Entry<Object, TaskStatistics> entry : this.coalescingTaskStatsPerContext.entrySet())
        {
            stats.put(entry.getKey(), entry.getValue().intervalFinished());
        }
        return stats;
    }
}
