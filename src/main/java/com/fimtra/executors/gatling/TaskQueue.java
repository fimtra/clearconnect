/*
 * Copyright (c) 2020 Ramon Servadei
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
package com.fimtra.executors.gatling;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.fimtra.executors.ICoalescingRunnable;
import com.fimtra.executors.IContextExecutor;
import com.fimtra.executors.ISequentialRunnable;
import com.fimtra.executors.ITaskStatistics;
import com.fimtra.executors.TaskStatistics;
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
    private final static int SEQUENTIAL_TASKS_MAX_POOL_SIZE =
            SystemUtils.getPropertyAsInt("gatling.sequentialTasksMaxPoolSize", 1000);
    private final static int COALESCING_TASKS_MAX_POOL_SIZE =
            SystemUtils.getPropertyAsInt("gatling.coalescingTasksMaxPoolSize", 1000);

    abstract static class InternalTaskQueue<T> implements Runnable {
        Object context;
        TaskStatistics stats;
        boolean active;

        /**
         * @return <code>true</code> if it was activated, <code>false</code> if it is already active
         */
        abstract boolean offer(T t);

        /**
         * @return the next task to run
         */
        abstract Runnable onTaskFinished();
    }

    /**
     * Holds all {@link ISequentialRunnable} objects for the same execution context. Each call to {@link
     * #run()} will dequeue the next runnable from an internal list and execute it.
     *
     * @author Ramon Servadei
     */
    final class SequentialTasks extends InternalTaskQueue<ISequentialRunnable> {
        final Deque<ISequentialRunnable> sequentialTasks = new LowGcLinkedList<>(2);
        int size;

        SequentialTasks()
        {
            super();
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
        Runnable onTaskFinished()
        {
            this.stats.itemExecuted();
            final Runnable poll =
                    TaskQueue.this.queue.size() == 0 && this.size > 0 ? this : TaskQueue.this.queue.poll();
            if (this.size > 0)
            {
                if (poll != this)
                {
                    TaskQueue.this.queue.offer(this);
                    TaskQueue.this.lock.notify();
                }
            }
            else
            {
                TaskQueue.this.sequentialTasksPerContext.remove(this.context);
                TaskQueue.this.sequentialTasksPool.offer(this);
            }
            return poll;
        }

        // NOTE: offer is called by a thread that synchronizes on TaskQueue.this.lock
        @Override
        boolean offer(ISequentialRunnable latest)
        {
            synchronized (this.sequentialTasks)
            {
                this.sequentialTasks.offer(latest);
                this.size++;
            }
            this.stats.itemSubmitted();

            if (!this.active)
            {
                this.active = true;
                TaskQueue.this.queue.offer(this);
                return true;
            }
            return false;
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
    final class CoalescingTasks extends InternalTaskQueue<ICoalescingRunnable> {
        final AtomicReference<ICoalescingRunnable> latestTask = new AtomicReference<>();

        CoalescingTasks()
        {
            super();
        }

        @Override
        public void run()
        {
            this.latestTask.getAndSet(null).run();
        }

        @Override
        Runnable onTaskFinished()
        {
            this.stats.itemExecuted();
            final ICoalescingRunnable next = this.latestTask.get();
            final Runnable poll =
                    TaskQueue.this.queue.size() == 0 && next != null ? this : TaskQueue.this.queue.poll();
            if (next != null)
            {
                if (poll != this)
                {
                    TaskQueue.this.queue.offer(this);
                    TaskQueue.this.lock.notify();
                }
            }
            else
            {
                TaskQueue.this.coalescingTasksPerContext.remove(this.context);
                TaskQueue.this.coalescingTasksPool.offer(this);
            }
            return poll;
        }

        // NOTE: offer is called by a thread that synchronizes on TaskQueue.this.lock
        @Override
        boolean offer(ICoalescingRunnable latest)
        {
            this.latestTask.getAndSet(latest);
            this.stats.itemSubmitted();

            if (!this.active)
            {
                this.active = true;
                TaskQueue.this.queue.offer(this);
                return true;
            }
            return false;
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
    final Function<Object, SequentialTasks> sequentialTasksFunction;
    final Function<Object, CoalescingTasks> coalescingTasksFunction;
    final TaskStatistics sequentialTaskStats = new TaskStatistics(IContextExecutor.QUEUE_LEVEL_STATS);
    final TaskStatistics coalescingTaskStats = new TaskStatistics(IContextExecutor.QUEUE_LEVEL_STATS);
    final Object lock = new Object();
    final SingleThreadReusableObjectPool<SequentialTasks> sequentialTasksPool;
    final SingleThreadReusableObjectPool<CoalescingTasks> coalescingTasksPool;
    final String name;

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
        this.sequentialTasksFunction = (c) -> {
            final SequentialTasks tasks = this.sequentialTasksPool.get();
            tasks.context = c;
            tasks.stats = this.sequentialTaskStats;
            return tasks;
        };
        this.coalescingTasksFunction = (c) -> {
            final CoalescingTasks tasks = this.coalescingTasksPool.get();
            tasks.context = c;
            tasks.stats = this.coalescingTaskStats;
            return tasks;
        };

    }

    /**
     * Add the runnable to the queue, ordering it as appropriate for any annotation present on the runnable.
     */
    void offer_callWhilstHoldingLock(Runnable runnable)
    {
        if (runnable instanceof ISequentialRunnable)
        {
            if (this.sequentialTasksPerContext.computeIfAbsent(((ISequentialRunnable) runnable).context(),
                    this.sequentialTasksFunction).offer((ISequentialRunnable) runnable))
            {
                this.lock.notify();
            }
        }
        else
        {
            if (runnable instanceof ICoalescingRunnable)
            {
                if (this.coalescingTasksPerContext.computeIfAbsent(((ICoalescingRunnable) runnable).context(),
                        this.coalescingTasksFunction).offer((ICoalescingRunnable) runnable))
                {
                    this.lock.notify();
                }
            }
            else
            {
                this.queue.offer(runnable);
                this.lock.notify();
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
    Map<Object, ? extends ITaskStatistics> getSequentialTaskStatistics()
    {
        Map<Object, TaskStatistics> stats = new HashMap<>(2);
        stats.put(IContextExecutor.QUEUE_LEVEL_STATS, this.sequentialTaskStats.intervalFinished());
        return stats;
    }

    /**
     * @return a copy of the internal statistics per coalescing task context
     */
    Map<Object, ? extends ITaskStatistics> getCoalescingTaskStatistics()
    {
        Map<Object, TaskStatistics> stats = new HashMap<>(2);
        stats.put(IContextExecutor.QUEUE_LEVEL_STATS, this.coalescingTaskStats.intervalFinished());
        return stats;
    }
}
