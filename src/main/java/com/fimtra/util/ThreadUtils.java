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
package com.fimtra.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides utilities for working with threads
 *
 * @author Ramon Servadei
 */
public abstract class ThreadUtils {
    private static final String PREFIX = System.getProperty("threadUtils.prefix", "clearconnect-");

    static final Thread.UncaughtExceptionHandler UNCAUGHT_EXCEPTION_HANDLER =
            new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e)
                {
                    try
                    {
                        Log.log(this, "Thread: " + t.getName(), e);
                    }
                    catch (Throwable problem)
                    {
                        final StringWriter stringWriter = new StringWriter(1024);
                        try (final PrintWriter pw = new PrintWriter(stringWriter))
                        {
                            pw.println("Could not log uncaught exception in thread: " + t.getName());
                            e.printStackTrace(pw);
                            pw.print("Exception that occurred when trying to log this: ");
                            problem.printStackTrace(pw);
                        }
                        System.err.println(stringWriter.toString());
                    }
                }
            };

    static final Map<Thread, List<Runnable>> THREAD_LOCAL_CLEANUP =
            Collections.synchronizedMap(CollectionUtils.newMap());

    public static void registerThreadLocalCleanup(Runnable threadLocalCleanup)
    {
        THREAD_LOCAL_CLEANUP.computeIfAbsent(Thread.currentThread(), r -> new ArrayList<>()).add(
                threadLocalCleanup);
    }

    private static final ScheduledExecutorService SCHEDULER =
            ThreadUtils.newPermanentScheduledExecutorService("threadutils-scheduler", 1);

    public static ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
            TimeUnit unit)
    {
        return SCHEDULER.scheduleWithFixedDelay(command, initialDelay, period, unit);
    }

    public static ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
            TimeUnit unit)
    {
        return SCHEDULER.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    /**
     * Logs the exception generated in the run method of a delegate runnable.
     *
     * @author Ramon Servadei
     */
    public static final class ExceptionLoggingRunnable implements Runnable {
        private final Runnable command;

        public ExceptionLoggingRunnable(Runnable command)
        {
            this.command = command;
        }

        @Override
        public void run()
        {
            try
            {
                this.command.run();
            }
            catch (Throwable e)
            {
                Log.log(this, "Could not complete execution of " + ObjectUtils.safeToString(this.command), e);
            }
        }
    }

    private static final String MAIN_METHOD_CLASSNAME;

    static
    {
        String name = null;
        final String command = System.getProperty("sun.java.command");
        if (command != null)
        {
            final String[] javaFqcn = command.split(" ")[0].split("\\.");
            if (javaFqcn.length > 0)
            {
                name = javaFqcn[javaFqcn.length - 1];
            }
        }
        if (name == null)
        {
            name = "Unknown";
            final Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
            Thread key;
            StackTraceElement[] value;
            for (Map.Entry<Thread, StackTraceElement[]> entry : allStackTraces.entrySet())
            {
                key = entry.getKey();
                value = entry.getValue();
                if ("main".equals(key.getName()))
                {
                    final StackTraceElement stackTraceElement = value[value.length - 1];
                    final String className = stackTraceElement.getClassName();
                    name = className.substring(className.lastIndexOf(".") + 1);
                    break;
                }
            }
        }
        MAIN_METHOD_CLASSNAME = name;

        if (Thread.getDefaultUncaughtExceptionHandler() == null)
        {
            Thread.setDefaultUncaughtExceptionHandler(
                    (t, e) -> Log.log(ThreadUtils.class, "Uncaught throwable: ", e));
        }
    }

    private ThreadUtils()
    {

    }

    /**
     * Get the simple class name of the main method used to start the entire VM
     */
    public static String getMainMethodClassSimpleName()
    {
        return MAIN_METHOD_CLASSNAME;
    }

    /**
     * Get the class name of the direct class calling this method.
     */
    public static String getDirectCallingClass()
    {
        return new Exception().getStackTrace()[1].getClassName();
    }

    /**
     * Get the simple name of the direct class calling this method.
     */
    public static String getDirectCallingClassSimpleName()
    {
        final String className = new Exception().getStackTrace()[1].getClassName();
        return className.substring(className.lastIndexOf(".") + 1);
    }

    /**
     * Get the class name of the indirect class calling this method - the class calling the class calling this
     * method
     */
    public static String getIndirectCallingClass()
    {
        return new Exception().getStackTrace()[2].getClassName();
    }

    /**
     * Get the simple name of the indirect class calling this method - the class calling the class calling
     * this method
     */
    public static String getIndirectCallingClassSimpleName()
    {
        final String className = new Exception().getStackTrace()[2].getClassName();
        return className.substring(className.lastIndexOf(".") + 1);
    }

    /**
     * Creates a {@link ThreadFactory} instance that will create daemon threads that use the provided name as
     * the thread name. Each created thread has an incrementing number appended to the name.
     * <p>
     * <b>The created threads are NOT started by the factory</b>
     *
     * @param threadName the thread name for each thread created by the returned factory
     * @return a {@link ThreadFactory} instance that creates named daemon threads
     */
    public static ThreadFactory newDaemonThreadFactory(final String threadName)
    {
        return new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r)
            {
                Thread t = ThreadUtils.newThread(r, threadName + "-" + this.threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        };
    }

    /**
     * Gets a single thread executor service that uses a {@link Thread} with name threadName.
     */
    public static ExecutorService newSingleThreadExecutorService(String threadName)
    {
        return Executors.newSingleThreadExecutor(newDaemonThreadFactory(threadName));
    }

    /**
     * Returns an executor service for scheduling tasks to be run in the future.
     * <p>
     * <b>NOTE:</b> all submitted {@link Runnable} tasks are wrapped in a
     * {@link ExceptionLoggingRunnable} to log any exception
     *
     * @see Executors#newScheduledThreadPool(int, ThreadFactory)
     */
    public static ScheduledExecutorService newScheduledExecutorService(final String threadName,
            final int threadCount)
    {
        return getScheduledExecutorService(threadName, threadCount, true);
    }

    /**
     * Gets a cached thread pool executor that uses a {@link Thread} with name threadName.
     */
    public static Executor newCachedThreadPoolExecutor(String threadName)
    {
        return Executors.newCachedThreadPool(newDaemonThreadFactory(threadName));
    }

    public static Thread newThread(final Runnable target, String threadName)
    {
        Thread thread = new Thread(() -> {
            Log.log(ThreadUtils.class, "Starting");
            try
            {
                target.run();
            }
            finally
            {
                Log.log(ThreadUtils.class, "Terminating");
                final List<Runnable> cleanupTasks = THREAD_LOCAL_CLEANUP.remove(Thread.currentThread());
                if (cleanupTasks != null)
                {
                    Log.log(ThreadUtils.class,
                            "Clearing threadlocals: " + ObjectUtils.safeToString(cleanupTasks));
                    for (Runnable cleanupTask : cleanupTasks)
                    {
                        try
                        {
                            cleanupTask.run();
                        }
                        catch (Exception e)
                        {
                            Log.log(cleanupTask, "Error in " + ObjectUtils.safeToString(cleanupTask), e);
                        }
                    }
                }
            }
        });
        thread.setName(PREFIX + threadName);
        thread.setUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
        return thread;
    }

    public static Thread newDaemonThread(final Runnable target, String threadName)
    {
        Thread thread = newThread(target, threadName);
        thread.setDaemon(true);
        return thread;
    }

    /**
     * Pause the current thread for the specified millis by calling {@link Thread#sleep(long)}. This is a
     * convenience method that does not throw any exception.
     *
     * @param millis the milliseconds to pause
     */
    public static void sleep(int millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            Log.log(ThreadUtils.class, "interrupted during sleep", e);
        }
    }

    /**
     * Like {@link #newScheduledExecutorService(String, int)} but the {@link ScheduledExecutorService} cannot
     * be shutdown
     */
    public static ScheduledExecutorService newPermanentScheduledExecutorService(final String threadName,
            final int threadCount)
    {
        return getScheduledExecutorService(threadName, threadCount, false);
    }

    private static ScheduledExecutorService getScheduledExecutorService(String threadName, int threadCount,
            boolean canShutdown)
    {
        return new ScheduledExecutorService() {
            final ScheduledExecutorService newScheduledThreadPool =
                    Executors.newScheduledThreadPool(threadCount, newDaemonThreadFactory(threadName));

            @Override
            public ScheduledFuture<?> schedule(final Runnable command, long delay, TimeUnit unit)
            {
                return this.newScheduledThreadPool.schedule(new ExceptionLoggingRunnable(command), delay,
                        unit);
            }

            @Override
            public void execute(Runnable command)
            {
                this.newScheduledThreadPool.execute(new ExceptionLoggingRunnable(command));
            }

            @Override
            public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
            {
                return this.newScheduledThreadPool.schedule(callable, delay, unit);
            }

            @Override
            public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
                    TimeUnit unit)
            {
                return this.newScheduledThreadPool.scheduleAtFixedRate(new ExceptionLoggingRunnable(command),
                        initialDelay, period, unit);
            }

            @Override
            public void shutdown()
            {
                if (canShutdown)
                {
                    this.newScheduledThreadPool.shutdown();
                }
                else
                {
                    Log.log(this, ObjectUtils.safeToString(this),
                            " is a 'permanent' service and cannot be shutdown");
                }
            }

            @Override
            public List<Runnable> shutdownNow()
            {
                if (canShutdown)
                {
                    return this.newScheduledThreadPool.shutdownNow();
                }
                else
                {
                    Log.log(this, ObjectUtils.safeToString(this),
                            " is a 'permanent' service and cannot be shutdown");
                    return Collections.emptyList();
                }
            }

            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                    TimeUnit unit)
            {
                return this.newScheduledThreadPool.scheduleWithFixedDelay(
                        new ExceptionLoggingRunnable(command), initialDelay, delay, unit);
            }

            @Override
            public boolean isShutdown()
            {
                return this.newScheduledThreadPool.isShutdown();
            }

            @Override
            public boolean isTerminated()
            {
                return this.newScheduledThreadPool.isTerminated();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
            {
                if (canShutdown)
                {
                    return this.newScheduledThreadPool.awaitTermination(timeout, unit);
                }
                else
                {
                    Log.log(this, ObjectUtils.safeToString(this),
                            " is a 'permanent' service and will not terminate");
                    return false;
                }
            }

            @Override
            public <T> Future<T> submit(Callable<T> task)
            {
                return this.newScheduledThreadPool.submit(task);
            }

            @Override
            public <T> Future<T> submit(Runnable task, T result)
            {
                return this.newScheduledThreadPool.submit(new ExceptionLoggingRunnable(task), result);
            }

            @Override
            public Future<?> submit(Runnable task)
            {
                return this.newScheduledThreadPool.submit(new ExceptionLoggingRunnable(task));
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
                    throws InterruptedException
            {
                return this.newScheduledThreadPool.invokeAll(tasks);
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
                    TimeUnit unit) throws InterruptedException
            {
                return this.newScheduledThreadPool.invokeAll(tasks, timeout, unit);
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                    throws InterruptedException, ExecutionException
            {
                return this.newScheduledThreadPool.invokeAny(tasks);
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException
            {
                return this.newScheduledThreadPool.invokeAny(tasks, timeout, unit);
            }
        };
    }
}