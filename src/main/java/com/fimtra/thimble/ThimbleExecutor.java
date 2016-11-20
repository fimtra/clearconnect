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
     * A task runner has a single thread that handles dequeuing of tasks from the {@link TaskQueue}
     * and executing them.
     * 
     * @author Ramon Servadei
     */
    final class TaskRunner implements Runnable
    {
        final Thread workerThread;
        final Object lock;

        Runnable task;
        boolean active = true;

        TaskRunner(final String name)
        {
            super();
            this.lock = new Object();
            this.workerThread = new Thread(this, name);
            this.workerThread.setDaemon(true);
            this.workerThread.start();
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

                        synchronized (ThimbleExecutor.this.taskQueue.lock)
                        {
                            ThimbleExecutor.this.stats.itemExecuted();
                            
                            this.task = ThimbleExecutor.this.taskQueue.poll_callWhilstHoldingLock();
                            if (this.task == null)
                            {
                                // no more tasks so place back into the runners list
                                ThimbleExecutor.this.taskRunners.offer(TaskRunner.this);
                            }
                        }
                    }
                }
                else
                {
                    synchronized (this.lock)
                    {
                        if (this.task == null)
                        {
                            try
                            {
                                this.lock.wait();
                            }
                            catch (InterruptedException e)
                            {
                                // don't care
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
            // trigger to stop
            execute(new Runnable()
            {
                @Override
                public void run()
                {
                    // noop
                }
            });
        }
    }

    final TaskQueue taskQueue;
    final Queue<TaskRunner> taskRunners;
    private final List<TaskRunner> taskRunnersRef;
    private final String name;
    private final int size;
    TaskStatistics stats;

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
            this.taskRunners.offer(new TaskRunner(threadName));
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
        final Runnable task;
        TaskRunner runner = null; 
        
        synchronized (this.taskQueue.lock)
        {
            this.taskQueue.offer_callWhilstHoldingLock(command);
            this.stats.itemSubmitted();
            
            if (this.taskRunners.size() == 0)
            {
                // all runners being used - they will auto-drain the taskQueue
                return;
            }

            task = this.taskQueue.poll_callWhilstHoldingLock();
            // depending on the type of runnable, the queue may return null (e.g. if its a
            // sequential task that is already processing)
            if (task != null)
            {
                runner = this.taskRunners.poll();
            }
        }
        
        // do the runner execute outside of the task queue lock
        if(runner != null)
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

    public void destroy()
    {
        synchronized (this.taskQueue.lock)
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
