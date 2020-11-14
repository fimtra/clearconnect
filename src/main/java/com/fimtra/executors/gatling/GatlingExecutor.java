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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import com.fimtra.executors.ICoalescingRunnable;
import com.fimtra.executors.IContextExecutor;
import com.fimtra.executors.ISequentialRunnable;
import com.fimtra.executors.ITaskStatistics;
import com.fimtra.executors.TaskStatistics;
import com.fimtra.util.CollectionUtils;
import com.fimtra.util.Log;
import com.fimtra.util.LowGcLinkedList;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SystemUtils;
import com.fimtra.util.ThreadUtils;

/**
 * A GatlingExecutor is a multi-thread {@link Executor} implementation that supports sequential and coalescing
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
 * GatlingExecutor will run all 50,000 tasks in submission order and sequentially. If 50,000 coalescing tasks
 * for the same context are submitted then, depending on performance, we can expect that the GatlingExecutor
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
public class GatlingExecutor implements IContextExecutor, ScheduledExecutorService {
    public static Set<? extends IContextExecutor> getExecutors()
    {
        return Collections.unmodifiableSet(EXECUTORS);
    }

    private static final Set<GatlingExecutor> EXECUTORS = Collections.synchronizedSet(new LinkedHashSet<>());

    private static final Runnable NOOP = () -> {
    };
    private static final long IDLE_PERIOD_MILLIS =
            SystemUtils.getPropertyAsLong("gatling.idlePeriodMillis", 10_000);
    private static final int MAX_THREADS = SystemUtils.getPropertyAsInt("gatling.maxThreads",
            Runtime.getRuntime().availableProcessors() * 10);
    private static final long PENDING_STALL_CHECK_MILLIS =
            SystemUtils.getPropertyAsLong("gatling.pendingStallCheckMillis", 50);
    private static final long STALL_PERIOD_CHECK =
            SystemUtils.getPropertyAsLong("gatling.stallCheckMillis", 500);
    private static final ScheduledExecutorService ANTI_STALL =
            ThreadUtils.newPermanentScheduledExecutorService("anti-stall", 1);
    private static final ScheduledExecutorService SCHEDULER =
            ThreadUtils.newPermanentScheduledExecutorService("scheduler", 1);

    private static void checkInstances()
    {
        try
        {
            synchronized (EXECUTORS)
            {
                EXECUTORS.forEach(GatlingExecutor::check);
            }
        }
        catch (Exception e)
        {
            Log.log(GatlingExecutor.class, "Could not trigger stall check");
        }
    }

    static
    {
        ANTI_STALL.scheduleWithFixedDelay(GatlingExecutor::checkInstances, STALL_PERIOD_CHECK,
                STALL_PERIOD_CHECK, TimeUnit.MILLISECONDS);
    }

    /**
     * A task runner has a single thread that handles dequeuing of tasks from the {@link TaskQueue} and
     * executing them.
     *
     * @author Ramon Servadei
     */
    final class TaskRunner implements Runnable {
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
            Runnable toRun = null;
            while (this.active && GatlingExecutor.this.active)
            {
                // note: toRun could be set to a value after a previous run
                if (toRun != null || (toRun = this.task) != null)
                {
                    try
                    {
                        toRun.run();
                    }
                    catch (Throwable e)
                    {
                        Log.log(this, "Could not execute " + ObjectUtils.safeToString(toRun), e);
                    }
                    finally
                    {
                        synchronized (GatlingExecutor.this.taskQueue.lock)
                        {
                            try
                            {
                                if (toRun instanceof TaskQueue.InternalTaskQueue<?>)
                                {
                                    ((TaskQueue.InternalTaskQueue<?>) toRun).onTaskFinished();
                                }
                            }
                            finally
                            {
                                toRun = GatlingExecutor.this.taskQueue.poll_callWhilstHoldingLock();
                                this.task = toRun;

                                GatlingExecutor.this.stats.itemExecuted();

                                if (toRun == null)
                                {
                                    // no more tasks so place back into the runners list
                                    GatlingExecutor.this.taskRunners.offerFirst(TaskRunner.this);
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
                                this.lock.wait(this.isCore ? 0 : GatlingExecutor.this.idlePeriodMillis);
                                waitTimeNanos = System.nanoTime() - waitTimeNanos;
                            }
                            catch (InterruptedException e)
                            {
                                // don't care
                            }
                        }
                        toRun = this.task;
                    }

                    if (toRun == null &&
                            // check for "spurious" waking up - check within a factor of 10,
                            // so 0.1ms (hence multiply by 100,000, not 1,000,000)
                            waitTimeNanos >= GatlingExecutor.this.idlePeriodMillis * 100_000)
                    {
                        synchronized (GatlingExecutor.this.taskQueue.lock)
                        {
                            // we could have had a task assigned so check before final destroy
                            if (this.task == null
                                    && GatlingExecutor.this.taskRunners.size() > GatlingExecutor.this.size)
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
            GatlingExecutor.this.taskRunnersRef.remove(this);
            GatlingExecutor.this.taskRunners.remove(this);
            GatlingExecutor.this.freeNumbers.push(this.number);
            Collections.sort(GatlingExecutor.this.freeNumbers);
            // trigger to wake up and stop
            execute();
            removeThreadId(this.workerThread.getId());
        }
    }

    final GatlingExecutor pool;
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
    ScheduledFuture<?> stallCheckPending;
    boolean active = true;

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
     * Construct the {@link GatlingExecutor} with a thread pool minimum size and name for the threads.
     *
     * @param name the name to use for each thread in the thread pool
     * @param size the internal core thread pool minimum thread size.
     */
    public GatlingExecutor(String name, int size)
    {
        this(name, size, null);
    }

    /**
     * Construct the {@link GatlingExecutor} with a thread pool minimum size and name for the threads.
     *
     * @param name the name to use for each thread in the thread pool
     * @param size the internal core thread pool minimum thread size.
     * @param pool the instance that will execute all tasks (used for thread pooling)
     */
    public GatlingExecutor(String name, int size, GatlingExecutor pool)
    {
        this.name = name;
        this.size = size;
        this.pool = pool;

        this.threadCounter = new AtomicInteger(0);
        this.freeNumbers = new LowGcLinkedList<>();
        this.stats = new TaskStatistics(this.name);
        this.taskRunners = CollectionUtils.newDeque();
        this.taskQueue = new TaskQueue(this.name);
        this.taskRunnersRef = new LinkedList<>();

        if (this.pool == null)
        {
            EXECUTORS.add(this);
            this.stallCheckPending =
                    ANTI_STALL.schedule(this::check, PENDING_STALL_CHECK_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public String toString()
    {
        return "GatlingExecutor[" + this.name + ":" + this.taskRunnersRef.size() + "]";
    }

    @Override
    public void execute(Runnable command)
    {
        if (!this.active)
        {
            return;
        }

        if (this.pool != null)
        {
            this.pool.execute(command);
            return;
        }

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
                    if (this.taskRunnersRef.size() < this.size)
                    {
                        addTaskRunner_callWhilstHoldingLock();
                    }
                    else if (this.stallCheckPending.isDone())
                    {
                        // trigger a check
                        this.stallCheckPending = ANTI_STALL.schedule(this::check, PENDING_STALL_CHECK_MILLIS,
                                TimeUnit.MILLISECONDS);
                    }

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
                    // if draining (or 0!), check runners are not all blocked
                    for (TaskRunner taskRunner : this.taskRunnersRef)
                    {
                        if (taskRunner.task == null
                                || taskRunner.workerThread.getState() == Thread.State.RUNNABLE)
                        {
                            // at least one runner idle or running
                            return;
                        }
                    }
                }

                // add a runner per outstanding task if under MAX_THREADS, else 1
                final int burst = Math.max(1,
                        Math.min(MAX_THREADS - this.taskRunnersRef.size(), this.taskQueue.lastTriggerSize));
                for (int i = 0; i < burst; i++)
                {
                    addTaskRunner_callWhilstHoldingLock();
                }
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Could not check " + this.toString(), e);
        }
    }

    private void addTaskRunner_callWhilstHoldingLock()
    {
        final int size = this.taskRunnersRef.size();
        if (size > MAX_THREADS)
        {
            Log.log(this, this.toString() + " maximum thread count exceeded: " + size + "/" + MAX_THREADS);
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
     * Get the statistics for the sequential tasks submitted to this {@link GatlingExecutor}. The statistics
     * are updated on each successive call to this method (this defines the time interval for the
     * statistics).
     *
     * @return a Map holding all sequential task context statistics. The statistics objects will be updated on
     * each successive call to this method.
     */
    @Override
    public Map<Object, ITaskStatistics> getSequentialTaskStatistics()
    {
        return (Map<Object, ITaskStatistics>) this.taskQueue.getSequentialTaskStatistics();
    }

    /**
     * Get the statistics for the coalescing tasks submitted to this {@link GatlingExecutor}. The statistics
     * are updated on each successive call to this method (this defines the time interval for the
     * statistics).
     *
     * @return a Map holding all coalescing task context statistics. The statistics objects will be updated on
     * each successive call to this method.
     */
    @Override
    public Map<Object, ITaskStatistics> getCoalescingTaskStatistics()
    {
        return (Map<Object, ITaskStatistics>) this.taskQueue.getCoalescingTaskStatistics();
    }

    /**
     * @return the statistics for all tasks submitted to this GatlingExecutor
     */
    @Override
    public TaskStatistics getExecutorStatistics()
    {
        return this.stats.intervalFinished();
    }

    @Override
    public void destroy()
    {
        shutdownNow();
    }

    /**
     * @return the name of the {@link GatlingExecutor} (this is also what each internal thread name begins
     * with)
     */
    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
    {
        return SCHEDULER.schedule(
                GatlingUtils.createContextRunnable(command, getGeneralContext(command), this), delay, unit);
    }

    private Object getGeneralContext(Object command)
    {
        return this;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
    {
        return SCHEDULER.schedule(
                GatlingUtils.createContextCallable(callable, getGeneralContext(callable), this), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
            TimeUnit unit)
    {
        return SCHEDULER.scheduleAtFixedRate(
                GatlingUtils.createContextRunnable(command, getGeneralContext(command), this), initialDelay,
                period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
            TimeUnit unit)
    {
        return SCHEDULER.scheduleWithFixedDelay(
                GatlingUtils.createContextRunnable(command, getGeneralContext(command), this), initialDelay,
                delay, unit);
    }

    @Override
    public void shutdown()
    {
        this.active = false;
        synchronized (this.taskQueue.lock)
        {
            for (TaskRunner taskRunner : new ArrayList<>(this.taskRunnersRef))
            {
                taskRunner.destroy_callWhilstHoldingLock();
            }
        }
        EXECUTORS.remove(this);
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        this.active = false;
        final List<Runnable> notCompleted = new LinkedList<>();
        synchronized (this.taskQueue.lock)
        {
            Runnable task;
            while ((task = this.taskQueue.poll_callWhilstHoldingLock()) != null)
            {
                // note: this will include InternalTaskQueue types
                notCompleted.add(task);
            }
        }
        shutdown();
        return notCompleted;
    }

    @Override
    public boolean isShutdown()
    {
        return !this.active;
    }

    @Override
    public boolean isTerminated()
    {
        return !this.active && this.taskQueue.queue.size() == 0;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        if (this.active)
        {
            return false;
        }

        final long now = System.nanoTime();
        final long end = now + unit.convert(timeout, TimeUnit.NANOSECONDS);
        while (this.taskQueue.queue.size() != 0)
        {
            if (System.nanoTime() > end)
            {
                return false;
            }
            LockSupport.parkNanos(1000);
        }
        return true;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task)
    {
        return SCHEDULER.submit(GatlingUtils.createContextCallable(task, getGeneralContext(task), this));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result)
    {
        return SCHEDULER.submit(
                GatlingUtils.createContextCallable(task, result, getGeneralContext(task), this));
    }

    @Override
    public Future<?> submit(Runnable task)
    {
        return SCHEDULER.submit(
                GatlingUtils.createContextCallable(task, null, getGeneralContext(task), this));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
    {
        return SCHEDULER.invokeAll(getCallables(tasks));
    }

    private <T> Collection<Callable<T>> getCallables(Collection<? extends Callable<T>> tasks)
    {
        final Collection<Callable<T>> callables = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks)
        {
            callables.add(GatlingUtils.createContextCallable(task, getGeneralContext(task), this));
        }
        return callables;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException
    {
        return SCHEDULER.invokeAll(getCallables(tasks), timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException
    {
        return SCHEDULER.invokeAny(getCallables(tasks));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        return SCHEDULER.invokeAny(getCallables(tasks), timeout, unit);
    }
}
