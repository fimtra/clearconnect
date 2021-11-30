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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import com.fimtra.util.CollectionUtils;
import com.fimtra.util.Log;
import com.fimtra.util.LowGcLinkedList;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.ThreadUtils;

/**
 * A ThimbleExecutor is a multi-thread {@link Executor} implementation that supports sequential and
 * coalescing tasks.
 * <p>
 * The executor has a thread pool that expands as tasks are needed, never exceeds the maximum limit.
 * Threads that are idle in the pool stay alive for 10 seconds, this idle time adapts to the use of
 * the pool to ensure that there is at least 2 threads ready to receive tasks at the expected
 * interval based on the longest gap between tasks submitted. The idle gap will expand if tasks are
 * submitted after the idle period.
 * <p>
 * Sequential tasks are guaranteed to run in order, sequentially (but may run in different threads).
 * Coalescing tasks are tasks where only the latest submitted task needs to be executed (effectively
 * overwriting previously submitted tasks).
 * <p>
 * <b>Sequential and coalesing tasks are mutually exclusive.</b>
 * <p>
 * As an example, if 50,000 sequential tasks for the same context are submitted, then we can expect
 * that the ThimbleExecutor will run all 50,000 tasks in submission order and sequentially. If
 * 50,000 coalescing tasks for the same context are submitted then, depending on performance, we can
 * expect that the ThimbleExecutor will run perhaps only 20,000.
 * <p>
 * Statistics for the number of submitted and executed sequential and coalescing tasks can be
 * obtained via the {@link #getSequentialTaskStatistics()} and
 * {@link #getCoalescingTaskStatistics()} methods. The statistics are recomputed on each successive
 * call to these methods. It is up to user code to call these methods periodically. Statistics for
 * the number of submitted and executed tasks (regardless of type) can be obtained via the
 * {@link #getExecutorStatistics()} method.
 * 
 * @see ISequentialRunnable
 * @see ICoalescingRunnable
 * @author Ramon Servadei
 */
public final class ThimbleExecutor implements IContextExecutor
{
    public static Set<ThimbleExecutor> getExecutors()
    {
        return Collections.unmodifiableSet(EXECUTORS);
    }

    static final Set<ThimbleExecutor> EXECUTORS = Collections.synchronizedSet(new LinkedHashSet<>());
    static final long IDLE_PERIOD_NANOS =
        Long.parseLong(System.getProperty("thimble.idlePeriodMillis", "10000")) * 1_000_000;

    /**
     * A task runner has a single thread that handles dequeuing of tasks from the {@link TaskQueue}
     * and executing them.
     * 
     * @author Ramon Servadei
     */
    final class TaskRunner implements Runnable
    {
        final Thread workerThread;
        final Object lock;
        final Integer number;

        Runnable task;
        boolean active = true;
        boolean idle = true;

        TaskRunner(final String name, Integer number)
        {
            super();
            this.number = number;
            this.lock = new Object();
            this.workerThread = ThreadUtils.newDaemonThread(this, name);
            this.workerThread.start();
            addThreadId(this.workerThread.getId());
        }

        @Override
        public void run()
        {
            while (this.active)
            {
                if (this.task != null)
                {
                    try
                    {
                        this.task.run();
                    }
                    catch (Throwable e)
                    {
                        Log.log(this, "Could not execute " + ObjectUtils.safeToString(this.task), e);
                    }
                    finally
                    {
                        synchronized (ThimbleExecutor.this.taskQueue.lock)
                        {
                            try
                            {
                                if (this.task instanceof TaskQueue.InternalTaskQueue<?>)
                                {
                                    ((TaskQueue.InternalTaskQueue<?>) this.task).onTaskFinished();
                                }
                            }
                            finally
                            {
                                this.task = null;

                                ThimbleExecutor.this.stats.itemExecuted();

                                this.task = ThimbleExecutor.this.taskQueue.poll_callWhilstHoldingLock();
                                if (this.task == null)
                                {
                                    // no more tasks so place back into the runners list
                                    ThimbleExecutor.this.taskRunners.offerFirst(TaskRunner.this);
                                }
                            }
                        }
                    }
                }
                else
                {
                    boolean destroy = false;
                    synchronized (this.lock)
                    {
                        if (this.task == null)
                        {
                            try
                            {
                                final long pauseMillis = (long) (ThimbleExecutor.this.idlePeriodNanos * 0.000001d);
                                final long now = System.nanoTime();
                                this.lock.wait(pauseMillis);

                                destroy = this.task == null
                                    // check for "spurious" waking up - check within a factor of 10,
                                    // so 0.1ms (hence multiply by 100,000, not 1,000,000)
                                    && System.nanoTime() - now >= pauseMillis * 100_000;
                            }
                            catch (InterruptedException e)
                            {
                                // don't care
                            }
                        }
                    }

                    if (destroy)
                    {
                        synchronized (ThimbleExecutor.this.taskQueue.lock)
                        {
                            // leave 2 threads available - one may be running right now
                            if (this.idle && ThimbleExecutor.this.taskRunnersRef.size() > 2)
                            {
                                destroy();
                            }
                            else
                            {
                                this.idle = true;
                            }
                        }
                    }
                }
            }
        }

        void execute(Runnable task)
        {
            synchronized (this.lock)
            {
                this.task = task;
                this.lock.notify();
            }
        }

        void destroy()
        {
            this.active = false;
            ThimbleExecutor.this.taskRunnersRef.remove(this);
            ThimbleExecutor.this.taskRunners.remove(this);
            ThimbleExecutor.this.freeNumbers.push(this.number);
            // trigger to wake up and stop
            execute(null);
            removeThreadId(this.workerThread.getId());
        }
    }

    final TaskQueue taskQueue;
    final Deque<TaskRunner> taskRunners;
    final List<TaskRunner> taskRunnersRef;
    private final String name;
    private final int size;
    final LowGcLinkedList<Integer> freeNumbers;
    private final AtomicInteger threadCounter;
    TaskStatistics stats;
    long idlePeriodNanos = IDLE_PERIOD_NANOS;
    long lastExecuteTimeNanos = System.nanoTime();

    volatile long[] tids = new long[0];

    synchronized void addThreadId(long id)
    {
        final long[] tidsNew = Arrays.copyOf(this.tids, this.tids.length + 1);
        tidsNew[this.tids.length] = id;
        Arrays.sort(tidsNew);
        this.tids = tidsNew;
    }

    synchronized void removeThreadId(long id)
    {
        long[] tidsNew = Arrays.copyOf(this.tids, this.tids.length);
        int k = 0;
        for (int i = 0; i < this.tids.length; i++)
        {
            if (this.tids[i] != id)
            {
                tidsNew[k++] = this.tids[i];
            }
        }
        tidsNew = Arrays.copyOf(tidsNew, k);
        Arrays.sort(tidsNew);
        this.tids = tidsNew;
    }

    @Override
    public boolean isExecutorThread(long id)
    {
        return Arrays.binarySearch(this.tids, id) > -1;
    }

    /**
     * Construct the {@link ThimbleExecutor} with a specific thread pool size.
     * 
     * @param size
     *            the internal thread pool size. The thread pool does not shrink or grow.
     */
    public ThimbleExecutor(int size)
    {
        this(ThimbleExecutor.class.getSimpleName(), size);
    }

    /**
     * Construct the {@link ThimbleExecutor} with a specific thread pool size and name for the
     * threads.
     * 
     * @param name
     *            the name to use for each thread in the thread pool
     * @param size
     *            the internal thread pool size. The thread pool does not shrink or grow.
     */
    public ThimbleExecutor(String name, int size)
    {
        this.name = name;
        this.size = size;
        this.threadCounter = new AtomicInteger(0);
        this.freeNumbers = new LowGcLinkedList<>();
        this.stats = new TaskStatistics(this.name);
        this.taskRunners = CollectionUtils.newDeque();
        this.taskQueue = new TaskQueue(this.name);
        this.taskRunnersRef = new LinkedList<>();
        EXECUTORS.add(this);
    }

    @Override
    public String toString()
    {
        return "ThimbleExecutor[" + this.name + ":" + this.size + "]";
    }

    @Override
    public void execute(Runnable command)
    {

        final Runnable task;
        TaskRunner runner = null;

        final long now = System.nanoTime();

        synchronized (this.taskQueue.lock)
        {
            final long timeSinceLastExecute = now - this.lastExecuteTimeNanos;
            if (timeSinceLastExecute > this.idlePeriodNanos)
            {
                this.idlePeriodNanos = (long) (timeSinceLastExecute * 1.5);
                Log.log(this, this.name + " idle period is now " + (long) (this.idlePeriodNanos * 0.000001d) + "ms");
            }
            this.lastExecuteTimeNanos = now;

            this.taskQueue.offer_callWhilstHoldingLock(command);
            this.stats.itemSubmitted();

            if (this.taskRunners.size() == 0)
            {
                if (this.taskRunnersRef.size() < this.size)
                {
                    Integer number;
                    if ((number = this.freeNumbers.poll()) == null)
                    {
                        number = Integer.valueOf(this.threadCounter.getAndIncrement());
                    }
                    runner = new TaskRunner(this.name + "-" + number, number);
                    this.taskRunnersRef.add(runner);
                    this.taskRunners.offerFirst(runner);
                }
                else
                {
                    // all runners being used - they will auto-drain the taskQueue
                    return;
                }
            }

            task = this.taskQueue.poll_callWhilstHoldingLock();
            // depending on the type of runnable, the queue may return null (e.g. if its a
            // sequential task that is already processing)
            if (task != null)
            {
                runner = this.taskRunners.poll();
                runner.idle = false;
            }
        }

        // do the runner execute outside of the task queue lock
        if (runner != null && task != null)
        {
            runner.execute(task);
        }
    }

    /**
     * Get the statistics for the sequential tasks submitted to this {@link ThimbleExecutor}. The
     * statistics are updated on each successive call to this method (this defines the time interval
     * for the statistics).
     * 
     * @return a Map holding all sequential task context statistics. The statistics objects will be
     *         updated on each successive call to this method.
     */
    @Override
    public Map<Object, TaskStatistics> getSequentialTaskStatistics()
    {
        return this.taskQueue.getSequentialTaskStatistics();
    }

    /**
     * Get the statistics for the coalescing tasks submitted to this {@link ThimbleExecutor}. The
     * statistics are updated on each successive call to this method (this defines the time interval
     * for the statistics).
     * 
     * @return a Map holding all coalescing task context statistics. The statistics objects will be
     *         updated on each successive call to this method.
     */
    @Override
    public Map<Object, TaskStatistics> getCoalescingTaskStatistics()
    {
        return this.taskQueue.getCoalescingTaskStatistics();
    }

    /**
     * @return the statistics for all tasks submitted to this ThimbleExecutor
     */
    @Override
    public TaskStatistics getExecutorStatistics()
    {
        return this.stats.intervalFinished();
    }

    @Override
    public void destroy()
    {
        synchronized (this.taskQueue.lock)
        {
            for (TaskRunner taskRunner : new ArrayList<>(this.taskRunnersRef))
            {
                taskRunner.destroy();
            }
            while (this.taskQueue.poll_callWhilstHoldingLock() != null)
            {
                // noop - drain the queue
            }
        }
        EXECUTORS.remove(this);
    }

    /**
     * @return the name of the {@link ThimbleExecutor} (this is also what each internal thread name
     *         begins with)
     */
    @Override
    public String getName()
    {
        return this.name;
    }
}
