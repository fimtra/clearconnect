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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fimtra.util.CollectionUtils;
import com.fimtra.util.Log;
import com.fimtra.util.LowGcLinkedList;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.ThreadUtils;

/**
 * A ThimbleExecutor is a multi-thread {@link Executor} implementation that supports sequential and coalescing
 * tasks.
 * <p>
 * The executor has a thread pool that expands as tasks are needed. A core number is kept alive. Extra threads
 * that are idle in the pool stay alive for 10 seconds, this idle time adapts to the use of the pool. The idle
 * gap will expand if tasks are submitted after the idle period.
 * <p>
 * Every 500ms (configurable, see {@link #STALL_PERIOD_CHECK}) all threads are checked for "stalled" status
 * (i.e. not RUNNING and the queue is increasing). In this state, a new thread is added.
 * <p>
 * Sequential tasks are guaranteed to run in order and sequentially (but may run in different threads).
 * Coalescing tasks are tasks where only the latest submitted task needs to be executed (effectively
 * overwriting previously submitted tasks).
 * <p>
 * <b>Sequential and coalescing tasks are mutually exclusive.</b>
 * <p>
 * As an example, if 50,000 sequential tasks for the same context are submitted, then we can expect that the
 * ThimbleExecutor will run all 50,000 tasks in submission order and sequentially. If 50,000 coalescing tasks
 * for the same context are submitted then, depending on performance, we can expect that the ThimbleExecutor
 * will run perhaps only 500 (depends on how fast the processing of 1 occurrence of the task is, the faster
 * its processed, the more occurrences will be processed, but its hard to really beat coalescing like this!).
 * </p>
 * <p>
 * Statistics for the number of submitted and executed sequential and coalescing tasks can be obtained via the
 * {@link #getSequentialTaskStatistics()} and {@link #getCoalescingTaskStatistics()} methods. The statistics
 * are recomputed on each successive call to these methods. It is up to user code to call these methods
 * periodically. Statistics for the number of submitted and executed tasks (regardless of type) can be
 * obtained via the {@link #getExecutorStatistics()} method.
 *
 * @author Ramon Servadei
 * @see ISequentialRunnable
 * @see ICoalescingRunnable
 */
public final class ThimbleExecutor implements IContextExecutor
{
    public static Set<ThimbleExecutor> getExecutors()
    {
        return Collections.unmodifiableSet(EXECUTORS);
    }

    private static final Set<ThimbleExecutor> EXECUTORS = Collections.synchronizedSet(new LinkedHashSet<>());

    private static final Runnable NOOP = () -> {
    };
    private static final long IDLE_PERIOD_MILLIS =
            Long.parseLong(System.getProperty("thimble.idlePeriodMillis", "10000"));
    private static final int MAX_THREADS = Integer.parseInt(
            System.getProperty("thimble.maxThreads", "" + Runtime.getRuntime().availableProcessors() * 10));

    private static final int STALL_PERIOD_CHECK =
            Integer.parseInt(System.getProperty("thimble.stallCheckMillis", "500"));
    private static final ScheduledExecutorService ANTI_STALL =
            ThreadUtils.newPermanentScheduledExecutorService("anti-stall", 1);

    private static void checkInstances()
    {
        try
        {
            synchronized (EXECUTORS)
            {
                EXECUTORS.forEach(ThimbleExecutor::check);
            }
        }
        catch (Exception e)
        {
            Log.log(ThimbleExecutor.class, "Could not trigger stall check");
        }
    }

    static
    {
        ANTI_STALL.scheduleWithFixedDelay(ThimbleExecutor::checkInstances, STALL_PERIOD_CHECK,
                STALL_PERIOD_CHECK, TimeUnit.MILLISECONDS);
    }

    /**
     * A task runner has a single thread that handles dequeuing of tasks from the {@link TaskQueue} and
     * executing them.
     *
     * @author Ramon Servadei
     */
    final class TaskRunner implements Runnable
    {
        final Thread workerThread;
        final Object lock;
        final Integer number;
        final boolean isCore;

        volatile Runnable task;
        boolean active = true;

        TaskRunner(final String name, Integer number, boolean isCore)
        {
            super();
            this.isCore = isCore;
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
                    long waitTimeNanos = 0;
                    synchronized (this.lock)
                    {
                        if (this.task == null)
                        {
                            try
                            {
                                waitTimeNanos = System.nanoTime();
                                this.lock.wait(this.isCore ? 0 : ThimbleExecutor.this.idlePeriodMillis);
                                waitTimeNanos = System.nanoTime() - waitTimeNanos;
                            }
                            catch (InterruptedException e)
                            {
                                // don't care
                            }
                        }
                    }

                    if (toRun == null &&
                            // check for "spurious" waking up - check within a factor of 10,
                            // so 0.1ms (hence multiply by 100,000, not 1,000,000)
                            waitTimeNanos >= ThimbleExecutor.this.idlePeriodMillis * 100_000)
                    {
                        synchronized (ThimbleExecutor.this.taskQueue.lock)
                        {
                            // we could have had a task assigned so check before final destroy
                            if (this.task == null
                                    && ThimbleExecutor.this.taskRunners.size() > ThimbleExecutor.this.size)
                            {
                                destroy_callWhilstHoldingLock();
                            }
                        }
                    }
                }
            }
        }

        void execute()
        {
            synchronized (this.lock)
            {
                this.lock.notify();
            }
        }

        void destroy_callWhilstHoldingLock()
        {
            this.active = false;
            ThimbleExecutor.this.taskRunnersRef.remove(this);
            ThimbleExecutor.this.taskRunners.remove(this);
            ThimbleExecutor.this.freeNumbers.push(this.number);
            Collections.sort(ThimbleExecutor.this.freeNumbers);
            // trigger to wake up and stop
            execute();
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
    long idlePeriodNanos = IDLE_PERIOD_MILLIS * 1_000_000;
    volatile long idlePeriodMillis = IDLE_PERIOD_MILLIS;
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
        for (long tid : this.tids)
        {
            if (tid != id)
            {
                tidsNew[k++] = tid;
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
     * @see #ThimbleExecutor(String, int)
     */
    public ThimbleExecutor(int size)
    {
        this(ThimbleExecutor.class.getSimpleName(), size);
    }

    /**
     * Construct the {@link ThimbleExecutor} with a thread pool minimum size and name for the threads.
     *
     * @param name the name to use for each thread in the thread pool
     * @param size the internal core thread pool minimum thread size.
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
        TaskRunner runner;
        final long now = System.nanoTime();

        synchronized (this.taskQueue.lock)
        {
            final long timeSinceLastExecute = now - this.lastExecuteTimeNanos;
            if (timeSinceLastExecute > this.idlePeriodNanos)
            {
                final long previousIdlePeriodMillis = this.idlePeriodMillis;
                this.idlePeriodNanos = (long) (timeSinceLastExecute * 1.5);
                this.idlePeriodMillis = (long) (this.idlePeriodNanos * 0.000001d);
                Log.log(this, this.name + " idle period extended from " + previousIdlePeriodMillis + "ms to "
                        + this.idlePeriodMillis + "ms");
            }
            this.lastExecuteTimeNanos = now;

            final TaskQueue.QChangeTypeEnum qChange = this.taskQueue.offer_callWhilstHoldingLock(command);
            this.stats.itemSubmitted();

            if (qChange != TaskQueue.QChangeTypeEnum.NONE)
            {
                if (this.taskRunners.size() == 0)
                {
                    check();
                    // all runners being used - they will auto-drain the taskQueue
                    return;
                }

                runner = this.taskRunners.poll();
                runner.task = this.taskQueue.poll_callWhilstHoldingLock();
            }
            else
            {
                // no taskQueue change (it was a change for a running coalescing/sequential context)
                // let the runners auto-drain the taskQueue
                return;
            }
        }

        // do the runner execute outside of the task queue lock
        runner.execute();
    }

    private void check()
    {
        try
        {
            synchronized (this.taskQueue.lock)
            {
                if (!this.taskQueue.isNotDraining_callWhilstHoldingLock())
                {
                    // if draining, check runners are not all blocked
                    for (TaskRunner taskRunner : this.taskRunnersRef)
                    {
                        if (taskRunner.task == null
                                || taskRunner.workerThread.getState() == Thread.State.RUNNABLE)
                        {
                            return;
                        }
                    }
                }

                addTaskRunner_callWhilstHoldingLock();
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Could not check " + this.toString(), e);
        }
    }

    private void addTaskRunner_callWhilstHoldingLock()
    {
        // todo check we don't create 1000's of threads
        final int size = this.taskRunnersRef.size();
        if (size > MAX_THREADS)
        {
            Log.log(this, this.toString() + " maximum thread count exceeded: " + size + "/" + MAX_THREADS);
            //            System.err.println(this.toString() + " maximum thread count exceeded: " + size + "/" + MAX_THREADS);
        }

        TaskRunner runner;
        Integer number;
        if ((number = this.freeNumbers.poll()) == null)
        {
            number = Integer.valueOf(this.threadCounter.getAndIncrement());
        }
        runner = new TaskRunner(this.name + "-" + number, number, number < this.size);
        this.taskRunnersRef.add(runner);
        this.taskRunners.offerFirst(runner);
        // dummy task to trigger queue draining
        execute(NOOP);
    }

    /**
     * Get the statistics for the sequential tasks submitted to this {@link ThimbleExecutor}. The statistics
     * are updated on each successive call to this method (this defines the time interval for the
     * statistics).
     *
     * @return a Map holding all sequential task context statistics. The statistics objects will be updated on
     * each successive call to this method.
     */
    @Override
    public Map<Object, TaskStatistics> getSequentialTaskStatistics()
    {
        return this.taskQueue.getSequentialTaskStatistics();
    }

    /**
     * Get the statistics for the coalescing tasks submitted to this {@link ThimbleExecutor}. The statistics
     * are updated on each successive call to this method (this defines the time interval for the
     * statistics).
     *
     * @return a Map holding all coalescing task context statistics. The statistics objects will be updated on
     * each successive call to this method.
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
                taskRunner.destroy_callWhilstHoldingLock();
            }
            while (this.taskQueue.poll_callWhilstHoldingLock() != null)
            {
                // noop - drain the queue
            }
        }
        EXECUTORS.remove(this);
    }

    /**
     * @return the name of the {@link ThimbleExecutor} (this is also what each internal thread name begins
     * with)
     */
    @Override
    public String getName()
    {
        return this.name;
    }
}
