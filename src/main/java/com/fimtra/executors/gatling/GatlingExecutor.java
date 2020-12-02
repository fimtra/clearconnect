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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
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
import com.fimtra.util.CollectionUtils;
import com.fimtra.util.Log;
import com.fimtra.util.LowGcLinkedList;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SystemUtils;
import com.fimtra.util.ThreadUtils;

/**
 * A GatlingExecutor is an elastic multi-threaded {@link IContextExecutor} implementation.
 *
 * <h1>Execution</h1>
 * <p>
 * The executor has a thread pool that expands as tasks are needed. A core number is kept alive. Extra threads
 * (auxiliary threads) stay alive for 10 seconds (configurable).
 * <p>
 * Every 250ms (configurable, see {@link #CHECK_PERIOD}) all threads are checked for "blocked" status (i.e.
 * not RUNNING and there are tasks still pending in the queue). In this state, new threads are added. Also
 * during the check, if there are free threads and threads that have a backlog of tasks, the backlog is
 * distributed to the free threads.
 *
 * <h1>Scheduling</h1>
 * <p>
 * Scheduling is implemented using a standard, single-threaded, {@link ScheduledExecutorService} which will
 * wrap the Runnable/Callable as a sequential-runnable and execute internally. This will introduce some extra
 * latency for when the task actually executes, but this is seen as negligible.
 *
 * @author Ramon Servadei
 * @see ISequentialRunnable
 * @see ICoalescingRunnable
 */
public class GatlingExecutor implements IContextExecutor {

    public static final Deque<Runnable> NOOP_INITIAL_TASKS = CollectionUtils.newDeque();

    public static Set<? extends IContextExecutor> getExecutors()
    {
        return Collections.unmodifiableSet(EXECUTORS);
    }

    private static final Set<GatlingExecutor> EXECUTORS = Collections.synchronizedSet(new LinkedHashSet<>());

    private static final long NON_CORE_LIVE_PERIOD_MILLIS =
            SystemUtils.getPropertyAsLong("gatling.nonCoreLivePeriodMillis", 10_000);
    /** The number of times (repeating) the core count is exceeded before bumping up the core count by 1. */
    private static final int CORE_COUNT_BUMP_THRESHOLD =
            SystemUtils.getPropertyAsInt("gatling.coreCountBumpThreshold", 10);
    private static final long NON_CORE_LIVE_PERIOD_NANOS = NON_CORE_LIVE_PERIOD_MILLIS * 1_000_000;
    private static final int PENDING_TASK_COUNT_TRANSFER_THRESHOLD =
            SystemUtils.getPropertyAsInt("gatling.pendingTaskCountTransferThreshold", 1000);
    /** The number of pending contexts to process in a runner that will trigger transfer to a free runner */
    private static final int PENDING_CONTEXT_TRANSFER_THRESHOLD =
            SystemUtils.getPropertyAsInt("gatling.pendingContextTransferThreshold", 1);
    private static final long CHECK_PERIOD = SystemUtils.getPropertyAsLong("gatling.checkPeriodMillis", 250);
    private static final ScheduledExecutorService CHECKER =
            ThreadUtils.newPermanentScheduledExecutorService("gatling-checker", 1);
    private static final ScheduledExecutorService SCHEDULER =
            ThreadUtils.newPermanentScheduledExecutorService("gatling-scheduler", 1);

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
        CHECKER.scheduleWithFixedDelay(GatlingExecutor::checkInstances, CHECK_PERIOD, CHECK_PERIOD,
                TimeUnit.MILLISECONDS);
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

        boolean active = true;
        private boolean runningTask;

        final Deque<Runnable> tasks = CollectionUtils.newSynchronizedDeque();

        TaskRunner(final String name, Integer number, boolean isCore, Deque<Runnable> initialTasks)
        {
            super();
            this.isCore = isCore;
            this.number = number;
            this.lock = new Object();
            this.workerThread = ThreadUtils.newDaemonThread(this, name);
            this.runningTask = false;
            this.tasks.addAll(initialTasks);
            this.workerThread.start();
            addThreadId(this.workerThread.getId());
        }

        @Override
        public void run()
        {
            final TaskQueue taskQueue = GatlingExecutor.this.taskQueue;
            Runnable toRun = this.tasks.poll();
            long startTimeNanos = System.nanoTime();
            while (this.active)
            {
                if (toRun != null)
                {
                    signalRunningTask(true);
                    try
                    {
                        toRun.run();
                    }
                    catch (Throwable e)
                    {
                        Log.log(this, "Could not execute " + ObjectUtils.safeToString(toRun), e);
                    }
                    signalRunningTask(false);
                }
                else
                {
                    signalRunningTask(false);

                    // check for destroy
                    if (!GatlingExecutor.this.active || (!this.isCore
                            && System.nanoTime() - startTimeNanos >= NON_CORE_LIVE_PERIOD_NANOS))
                    {
                        synchronized (GatlingExecutor.this.taskRunners)
                        {
                            // must access inside taskRunners lock - see check()
                            if (this.tasks.size() == 0)
                            {
                                this.active = false;
                                GatlingExecutor.this.taskRunners.remove(this);
                                GatlingExecutor.this.freeNumbers.push(this.number);
                                Collections.sort(GatlingExecutor.this.freeNumbers);
                                removeThreadId(this.workerThread.getId());
                                return;
                            }
                        }
                    }
                }

                if (toRun instanceof TaskQueue.InternalTaskQueue<?>)
                {
                    toRun = ((TaskQueue.InternalTaskQueue<?>) toRun).onTaskFinished(this.tasks);
                }
                else
                {
                    toRun = null;
                }

                // do quick check to grab pending work from the task queue
                if (GatlingExecutor.this.taskRunnersWaiting_accessWithQLock.size() == 0
                        && taskQueue.queue.size() > 0)
                {
                    final Runnable t;
                    synchronized (taskQueue.lock)
                    {
                        t = taskQueue.poll_callWhilstHoldingLock();
                    }
                    if (t != null)
                    {
                        if (toRun == null)
                        {
                            toRun = t;
                        }
                        else
                        {
                            // swap round
                            synchronized (this.tasks)
                            {
                                if (this.tasks.size() == 0)
                                {
                                    // if its zero size and toRun != null it means toRun is an InternalTaskQueue
                                    // and has been run at least once already, swap round to be fair to the new task
                                    this.tasks.addLast(toRun);
                                    toRun = t;
                                }
                                else
                                {
                                    // add it in front of other tasks so it is executed next
                                    this.tasks.addFirst(t);
                                }
                            }
                        }
                    }
                }

                // check our local queue
                if (toRun == null)
                {
                    toRun = this.tasks.poll();
                }

                if (toRun == null)
                {
                    synchronized (taskQueue.lock)
                    {
                        toRun = taskQueue.poll_callWhilstHoldingLock();
                        if (toRun == null)
                        {
                            if (GatlingExecutor.this.active)
                            {
                                try
                                {
                                    GatlingExecutor.this.taskRunnersWaiting_accessWithQLock.add(this);
                                    taskQueue.lock.wait(this.isCore ? 0 : NON_CORE_LIVE_PERIOD_MILLIS);
                                }
                                catch (InterruptedException e)
                                {
                                    // don't care ???
                                }
                                finally
                                {
                                    GatlingExecutor.this.taskRunnersWaiting_accessWithQLock.remove(this);
                                    toRun = taskQueue.queue.poll();
                                }
                            }
                        }
                    }
                }
            }
        }

        private void signalRunningTask(boolean runningTask)
        {
            this.runningTask = runningTask;
        }

    }

    volatile boolean active = true;
    volatile long[] tids = new long[0];
    final TaskQueue taskQueue;
    final List<TaskRunner> taskRunners;
    final Set<TaskRunner> taskRunnersWaiting_accessWithQLock;
    private int size;
    private final String name;
    private final AtomicInteger threadCounter;
    private final AtomicInteger coreCountExceeded;
    private final LowGcLinkedList<Integer> freeNumbers;

    /**
     * @param name the initial core thread pool minimum thread size
     * @param size the internal core thread pool minimum thread size.
     */
    public GatlingExecutor(String name, int size)
    {
        this.name = name;
        this.size = size;
        this.threadCounter = new AtomicInteger(0);
        this.coreCountExceeded = new AtomicInteger(0);
        this.freeNumbers = new LowGcLinkedList<>();
        this.taskQueue = new TaskQueue(this.name);
        this.taskRunners = new CopyOnWriteArrayList<>();
        this.taskRunnersWaiting_accessWithQLock = new HashSet<>();

        // create core threads
        for (int i = 0; i < size; i++)
        {
            addTaskRunner_callWhilstHoldingLock(NOOP_INITIAL_TASKS);
        }
        EXECUTORS.add(this);
    }

    @Override
    public boolean isExecutorThread(long id)
    {
        return Arrays.binarySearch(this.tids, id) > -1;
    }

    @Override
    public String toString()
    {
        return "GatlingExecutor[" + this.name + ":" + this.taskRunners.size() + "]";
    }

    @Override
    public void execute(Runnable command)
    {
        if (!this.active)
        {
            return;
        }

        synchronized (this.taskQueue.lock)
        {
            this.taskQueue.add_callWhilstHoldingLock(command);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Object, ITaskStatistics> getSequentialTaskStatistics()
    {
        return (Map<Object, ITaskStatistics>) this.taskQueue.getSequentialTaskStatistics();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Object, ITaskStatistics> getCoalescingTaskStatistics()
    {
        return (Map<Object, ITaskStatistics>) this.taskQueue.getCoalescingTaskStatistics();
    }

    @Override
    public void destroy()
    {
        shutdownNow();
    }

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
            this.taskQueue.lock.notifyAll();
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
    public boolean awaitTermination(long timeout, TimeUnit unit)
    {
        if (this.active)
        {
            return false;
        }

        final long now = System.nanoTime();
        final long end = now + unit.toNanos(timeout);
        while (this.taskQueue.queue.size() != 0 || this.taskRunners.size() != 0)
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

    private void check()
    {
        try
        {
            synchronized (this.taskRunners)
            {
                final Deque<TaskRunner> runnersWithWork = CollectionUtils.newDeque();
                final Deque<Deque<Runnable>> blockedTaskLists = CollectionUtils.newDeque();
                for (TaskRunner taskRunner : this.taskRunners)
                {
                    synchronized (taskRunner.tasks)
                    {
                        if (taskRunner.runningTask
                                && taskRunner.workerThread.getState() != Thread.State.RUNNABLE)
                        {
                            final Deque<Runnable> taskList = CollectionUtils.newDeque();
                            // remove the tasks and place onto the blocked queue
                            while (taskRunner.tasks.size() > 0)
                            {
                                taskList.add(taskRunner.tasks.poll());
                            }
                            blockedTaskLists.add(taskList);
                        }
                        else
                        {
                            if (taskRunner.tasks.size() > PENDING_CONTEXT_TRANSFER_THRESHOLD)
                            {
                                // compute full size
                                int taskCount = 0;
                                for (Runnable task : taskRunner.tasks)
                                {
                                    if (task instanceof TaskQueue.SequentialTasks)
                                    {
                                        taskCount += ((TaskQueue.SequentialTasks) task).size;
                                    }
                                    else
                                    {
                                        taskCount++;
                                    }
                                }
                                if (taskCount > PENDING_TASK_COUNT_TRANSFER_THRESHOLD)
                                {
                                    runnersWithWork.add(taskRunner);
                                }
                            }
                        }
                    }
                }

                if (runnersWithWork.size() > 0)
                {
                    // go through runners with work and transfer a SINGLE task to free or new runner
                    synchronized (this.taskQueue.lock)
                    {
                        if (this.taskRunnersWaiting_accessWithQLock.size() > 0)
                        {
                            for (TaskRunner taskRunner : this.taskRunnersWaiting_accessWithQLock)
                            {
                                final TaskRunner runner = runnersWithWork.poll();
                                if (runner != null)
                                {
                                    taskRunner.tasks.add(runner.tasks.poll());
                                }
                            }
                            this.taskQueue.lock.notifyAll();
                        }
                        else
                        {
                            final Deque<Runnable> initialTasks = CollectionUtils.newDeque();
                            final Runnable task = Objects.requireNonNull(runnersWithWork.poll()).tasks.poll();
                            Log.log(this, "Creating new thread to handle transferred task: " + task);
                            initialTasks.add(task);
                            addTaskRunner_callWhilstHoldingLock(initialTasks);
                        }
                    }
                }

                if (blockedTaskLists.size() > 0)
                {
                    // pass blocked lists to available runners
                    synchronized (this.taskQueue.lock)
                    {
                        if (this.taskRunnersWaiting_accessWithQLock.size() > 0)
                        {
                            for (TaskRunner taskRunner : this.taskRunnersWaiting_accessWithQLock)
                            {
                                final Deque<Runnable> blocked = blockedTaskLists.poll();
                                if (blocked != null)
                                {
                                    taskRunner.tasks.addAll(blocked);
                                }
                            }
                            this.taskQueue.lock.notifyAll();
                        }
                    }
                    if (blockedTaskLists.size() > 0)
                    {
                        // create task runners for the remaining
                        Log.log(this,
                                "Creating " + blockedTaskLists.size() + " threads to handle blocked tasks");
                        for (Deque<Runnable> blockedTaskList : blockedTaskLists)
                        {
                            addTaskRunner_callWhilstHoldingLock(blockedTaskList);
                        }
                    }
                }
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Could not check " + this.toString(), e);
        }
    }

    private void addTaskRunner_callWhilstHoldingLock(Deque<Runnable> initialTasks)
    {
        Integer number;
        if ((number = this.freeNumbers.poll()) == null)
        {
            number = Integer.valueOf(this.threadCounter.getAndIncrement());
        }
        final boolean isCore = number < this.size;
        if (number == this.size)
        {
            this.coreCountExceeded.incrementAndGet();
            // if we go over the core size every X times then bump up the core size
            if (this.coreCountExceeded.get() % CORE_COUNT_BUMP_THRESHOLD == 0)
            {
                this.size++;
                Log.log(this, "Increasing core size to " + this.size);
            }
        }
        this.taskRunners.add(new TaskRunner(this.name + (isCore ? "" : "-aux") + "-" + number, number, isCore,
                initialTasks));
    }

    private Object getGeneralContext(Object command)
    {
        return this;
    }
}
