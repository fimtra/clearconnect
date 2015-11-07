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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fimtra.util.CollectionUtils;

/**
 * A ThimbleExecutor is a multi-thread {@link Executor} implementation that supports sequential and
 * coalescing tasks.
 * <p>
 * The executor has a fixed thread pool that is created on construction.
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
public final class ThimbleExecutor implements Executor
{
    public static final String QUEUE_LEVEL_STATS = "QueueLevelStats";

    /**
     * A task runner exists for each internal single-threaded Executor used by the
     * {@link ThimbleExecutor}. The task runner handles dequeuing of tasks from the internal
     * {@link TaskQueue} and executing them using its single-threaded executor.
     * 
     * @author Ramon Servadei
     */
    final class TaskRunner implements Runnable
    {
        private Runnable task;
        private final ExecutorService executor;

        TaskRunner(ExecutorService executor)
        {
            super();
            this.executor = executor;
        }

        @Override
        public void run()
        {
            // todo should the run add some context to the thread name?
            // System.err.println(Thread.currentThread() + " running " + task);
            try
            {
                this.task.run();
            }
            catch (Exception e)
            {
                StringWriter stringWriter = new StringWriter(1000);
                PrintWriter pw = new PrintWriter(stringWriter);
                pw.print(Thread.currentThread() + " could not execute " + this.task + " due to: ");
                e.printStackTrace(pw);
                System.err.print(stringWriter.toString());
            }
            finally
            {
                this.task = null;
                
                ThimbleExecutor.this.taskQueue.lock.lock();
                try
                {
                    executeNextTask_callWhilstHoldingLock(this);
                }
                finally
                {
                    ThimbleExecutor.this.taskQueue.lock.unlock();
                }
            }
        }

        void execute(Runnable task)
        {
            this.task = task;
            try
            {
                this.executor.execute(this);
            }
            catch (RejectedExecutionException e)
            {
                // REALLY don't care
            }
        }

        void destroy()
        {
            this.executor.shutdown();
        }
    }

    final TaskQueue taskQueue;
    private final Queue<TaskRunner> taskRunners;
    private final List<TaskRunner> taskRunnersRef;
    private final String name;
    private final int size;
    private TaskStatistics stats;

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
        this.stats = new TaskStatistics(this.name);
        this.taskRunners = CollectionUtils.newDeque();
        this.taskQueue = new TaskQueue();
        for (int i = 0; i < size; i++)
        {
            final String threadName = this.name + i;
            ThreadPoolExecutor executor =
                new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.MILLISECONDS,
                    new LinkedBlockingDeque<Runnable>(), new ThreadFactory()
                    {
                        private final AtomicInteger threadNumber = new AtomicInteger();

                        @Override
                        public Thread newThread(Runnable r)
                        {
                            Thread t = new Thread(r, threadName + "-" + this.threadNumber.getAndIncrement());
                            t.setDaemon(true);
                            return t;
                        }
                    });
            this.taskRunners.offer(new TaskRunner(executor));
        }
        this.taskRunnersRef = new ArrayList<TaskRunner>(this.taskRunners);
    }

    @Override
    public String toString()
    {
        return "ThimbleExecutor[" + this.name + ":" + this.size + "]";
    }

    @Override
    public void execute(Runnable command)
    {
        this.stats.itemSubmitted();
        this.taskQueue.lock.lock();
        try
        {
            this.taskQueue.offer_callWhilstHoldingLock(command);
            executeNextTask_callWhilstHoldingLock(null);
        }
        finally
        {
            this.taskQueue.lock.unlock();
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
    public Map<Object, TaskStatistics> getCoalescingTaskStatistics()
    {
        return this.taskQueue.getCoalescingTaskStatistics();
    }

    /**
     * @return the statistics for all tasks submitted to this ThimbleExecutor
     */
    public TaskStatistics getExecutorStatistics()
    {
        return this.stats.intervalFinished();
    }

    void executeNextTask_callWhilstHoldingLock(final TaskRunner taskRunner)
    {
        TaskRunner runner = taskRunner;
        if (runner == null)
        {
            if (this.taskRunners.size() == 0)
            {
                return;
            }
            runner = this.taskRunners.poll();
        }
        Runnable task = this.taskQueue.poll_callWhilstHoldingLock();
        if (task != null)
        {
            runner.execute(task);
            this.stats.itemExecuted();
        }
        else
        {
            this.taskRunners.offer(runner);
        }
    }

    public void destroy()
    {
        this.taskQueue.lock.lock();
        try
        {
            for (TaskRunner taskRunner : this.taskRunnersRef)
            {
                taskRunner.destroy();
            }
            this.taskRunners.clear();
            while (this.taskQueue.poll_callWhilstHoldingLock() != null)
            {
                // noop - drain the queue
            }
        }
        finally
        {
            this.taskQueue.lock.unlock();
        }
    }

    /**
     * @return the name of the {@link ThimbleExecutor} (this is also what each internal thread name
     *         begins with)
     */
    public String getName()
    {
        return this.name;
    }
}
