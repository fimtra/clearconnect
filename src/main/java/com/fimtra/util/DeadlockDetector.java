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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.Thread.State;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Uses a {@link ThreadMXBean} to detect deadlocks.
 * <p>
 * Use {@link #newDeadlockDetectorThread(String, long, DeadlockObserver, boolean)} to create a
 * thread to check for deadlocks.
 * <p>
 * Also dumps all threads to a file.
 * 
 * @author Ramon Servadei
 */
public final class DeadlockDetector
{
    /**
     * An observer that receives events when threads are deadlocked. Registered via
     * {@link DeadlockDetector#newDeadlockDetectorThread(String, long, DeadlockObserver, boolean)}
     * 
     * @author Ramon Servadei
     */
    public static interface DeadlockObserver
    {
        void onDeadlockFound(ThreadInfoWrapper[] deadlocks);
    }

    /**
     * Create and start a <b>daemon</b> thread that checks for deadlocks at the specified period.
     * Deadlocks are written to System.err first then passed to the deadlockObserver for handling.
     * <p>
     * This thread will also dump the current active threads to a file. The file is either static or
     * rolling.
     * 
     * @param rollingThreaddumpFile
     *            <code>true</code> to dump threads to a rolling log file, <code>false</code> for a
     *            static file
     * 
     * @return an AtomicBoolean that can be set to false to stop the detection and terminate the
     *         thread
     */
    public static final AtomicBoolean newDeadlockDetectorThread(String threadName, final long checkPeriodMillis,
        final DeadlockObserver deadlockObserver, final boolean rollingThreaddumpFile)
    {
        final AtomicBoolean active = new AtomicBoolean(true);

        final Runnable task = new Runnable()
        {
            final DeadlockDetector deadlockDetector = new DeadlockDetector();
            final RollingFileAppender appender;
            {
                if (rollingThreaddumpFile)
                {
                    this.appender =
                        RollingFileAppender.createStandardRollingFileAppender("threadDump",
                            UtilProperties.Values.LOG_DIR);
                }
                else
                {
                    this.appender = null;
                }
            }

            @Override
            public void run()
            {
                // prepare a static file for logging threaddumps if the rolling option is not used
                final File staticFile;
                if (this.appender == null)
                {
                    final String filePrefix = ThreadUtils.getMainMethodClassSimpleName() + "-threadDump";
                    // delete old files
                    FileUtils.deleteFiles(new File(UtilProperties.Values.LOG_DIR),
                        TimeUnit.MINUTES.convert(1, TimeUnit.DAYS), filePrefix);
                    staticFile = FileUtils.createLogFile_yyyyMMddHHmmss(UtilProperties.Values.LOG_DIR, filePrefix);
                    try
                    {
                        staticFile.createNewFile();
                    }
                    catch (IOException e)
                    {
                        Log.log(DeadlockDetector.class, "Could not create " + ObjectUtils.safeToString(staticFile), e);
                    }
                }
                else
                {
                    staticFile = null;
                }

                while (active.get())
                {
                    try
                    {
                        if (this.appender != null || staticFile != null)
                        {
                            final ThreadInfoWrapper[] threads = this.deadlockDetector.getThreadInfoWrappers();
                            if (threads != null)
                            {
                                StringBuilder sb = new StringBuilder(1024);
                                for (int i = 0; i < threads.length; i++)
                                {
                                    sb.append(threads[i].toString());
                                }
                                if (this.appender != null)
                                {
                                    this.appender.append("========  ").append(new Date().toString()).append("  ======").append(
                                        SystemUtils.lineSeparator());
                                    this.appender.append(sb);
                                    this.appender.flush();
                                }
                                else
                                {
                                    if (staticFile != null)
                                    {
                                        PrintWriter staticThreadDump = new PrintWriter(staticFile);
                                        try
                                        {
                                            staticThreadDump.append("========  ").append(new Date().toString()).append(
                                                "  ======").append(SystemUtils.lineSeparator());
                                            staticThreadDump.append(sb);
                                            staticThreadDump.flush();
                                        }
                                        finally
                                        {
                                            staticThreadDump.close();
                                        }
                                    }
                                }
                            }
                        }

                        final ThreadInfoWrapper[] deadlocks = this.deadlockDetector.findDeadlocks();
                        if (deadlocks != null)
                        {
                            StringBuilder sb = new StringBuilder(1024);
                            sb.append("DEADLOCKED THREADS FOUND!").append(SystemUtils.lineSeparator());
                            for (int i = 0; i < deadlocks.length; i++)
                            {
                                sb.append(deadlocks[i].toString());
                            }
                            Log.log(ThreadUtils.class, sb.toString());
                            deadlockObserver.onDeadlockFound(deadlocks);
                        }
                        try
                        {
                            Thread.sleep(checkPeriodMillis);
                        }
                        catch (InterruptedException e)
                        {
                            // don't care
                        }
                    }
                    catch (Exception e)
                    {
                        Log.log(this, "Exception during processing, will continue processing", e);
                    }
                }
            }
        };
        Thread detectorThread = ThreadUtils.newThread(task, threadName);
        detectorThread.setDaemon(true);
        detectorThread.start();
        return active;
    }

    final ThreadMXBean threadMxBean;

    /**
     * A wrapper for the {@link ThreadInfo} class and provides a {@link #toString()} that produces
     * the full stack trace for the thread.
     * 
     * @author Ramon Servadei
     */
    public static final class ThreadInfoWrapper
    {
        private final ThreadInfo delegate;

        ThreadInfoWrapper(ThreadInfo delegate)
        {
            super();
            this.delegate = delegate;
        }

        @Override
        public int hashCode()
        {
            return this.delegate.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            return this.delegate.equals(obj);
        }

        public long getThreadId()
        {
            return this.delegate.getThreadId();
        }

        public String getThreadName()
        {
            return this.delegate.getThreadName();
        }

        public State getThreadState()
        {
            return this.delegate.getThreadState();
        }

        public long getBlockedTime()
        {
            return this.delegate.getBlockedTime();
        }

        public long getBlockedCount()
        {
            return this.delegate.getBlockedCount();
        }

        public long getWaitedTime()
        {
            return this.delegate.getWaitedTime();
        }

        public long getWaitedCount()
        {
            return this.delegate.getWaitedCount();
        }

        public LockInfo getLockInfo()
        {
            return this.delegate.getLockInfo();
        }

        public String getLockName()
        {
            return this.delegate.getLockName();
        }

        public long getLockOwnerId()
        {
            return this.delegate.getLockOwnerId();
        }

        public String getLockOwnerName()
        {
            return this.delegate.getLockOwnerName();
        }

        public StackTraceElement[] getStackTrace()
        {
            return this.delegate.getStackTrace();
        }

        public boolean isSuspended()
        {
            return this.delegate.isSuspended();
        }

        public boolean isInNative()
        {
            return this.delegate.isInNative();
        }

        @Override
        public String toString()
        {
            // take over the toString to remove the 8 frame limit in ThreadInfo
            StringBuilder sb =
                new StringBuilder("\"" + this.delegate.getThreadName() + "\"" + " Id=" + this.delegate.getThreadId()
                    + " " + this.delegate.getThreadState());
            if (this.delegate.getLockName() != null)
            {
                sb.append(" on " + this.delegate.getLockName());
            }
            if (this.delegate.getLockOwnerName() != null)
            {
                sb.append(" owned by \"" + this.delegate.getLockOwnerName() + "\" Id=" + this.delegate.getLockOwnerId());
            }
            if (this.delegate.isSuspended())
            {
                sb.append(" (suspended)");
            }
            if (this.delegate.isInNative())
            {
                sb.append(" (in native)");
            }
            sb.append(SystemUtils.lineSeparator());
            int i = 0;
            for (; i < this.delegate.getStackTrace().length; i++)
            {
                StackTraceElement ste = this.delegate.getStackTrace()[i];
                sb.append("\tat " + ste.toString());
                sb.append(SystemUtils.lineSeparator());
                if (i == 0 && this.delegate.getLockInfo() != null)
                {
                    Thread.State ts = this.delegate.getThreadState();
                    switch(ts)
                    {
                        case BLOCKED:
                            sb.append("\t-  blocked on " + this.delegate.getLockInfo());
                            sb.append(SystemUtils.lineSeparator());
                            break;
                        case WAITING:
                            sb.append("\t-  waiting on " + this.delegate.getLockInfo());
                            sb.append(SystemUtils.lineSeparator());
                            break;
                        case TIMED_WAITING:
                            sb.append("\t-  waiting on " + this.delegate.getLockInfo());
                            sb.append(SystemUtils.lineSeparator());
                            break;
                        default :
                    }
                }

                for (MonitorInfo mi : this.delegate.getLockedMonitors())
                {
                    if (mi.getLockedStackDepth() == i)
                    {
                        sb.append("\t-  locked " + mi);
                        sb.append(SystemUtils.lineSeparator());
                    }
                }
            }
            if (i < this.delegate.getStackTrace().length)
            {
                sb.append("\t...");
                sb.append(SystemUtils.lineSeparator());
            }

            LockInfo[] locks = this.delegate.getLockedSynchronizers();
            if (locks.length > 0)
            {
                sb.append(SystemUtils.lineSeparator()).append("\tNumber of locked synchronizers = " + locks.length);
                sb.append(SystemUtils.lineSeparator());
                for (LockInfo li : locks)
                {
                    sb.append("\t- " + li);
                    sb.append(SystemUtils.lineSeparator());
                }
            }
            sb.append(SystemUtils.lineSeparator());
            return sb.toString();
        }

        public MonitorInfo[] getLockedMonitors()
        {
            return this.delegate.getLockedMonitors();
        }

        public LockInfo[] getLockedSynchronizers()
        {
            return this.delegate.getLockedSynchronizers();
        }

    }

    public DeadlockDetector()
    {
        this.threadMxBean = ManagementFactory.getThreadMXBean();
    }

    /**
     * @see ThreadMXBean#findDeadlockedThreads()
     * @return <code>null</code> if no threads are deadlocked, otherwise an array of
     *         {@link ThreadInfoWrapper} objects describing the threads that are deadlocked
     */
    public ThreadInfoWrapper[] findDeadlocks()
    {
        long[] deadlockedThreadIds = this.threadMxBean.findDeadlockedThreads();
        if (deadlockedThreadIds == null)
        {
            return null;
        }
        return getThreadInfoWrappersFor(deadlockedThreadIds);
    }

    public ThreadInfoWrapper[] getThreadInfoWrappers()
    {
        long[] allThreadIds = this.threadMxBean.getAllThreadIds();
        if (allThreadIds == null)
        {
            return null;
        }
        return getThreadInfoWrappersFor(allThreadIds);
    }

    private ThreadInfoWrapper[] getThreadInfoWrappersFor(long[] deadlockedThreadIds)
    {
        ThreadInfo[] threadInfos = this.threadMxBean.getThreadInfo(deadlockedThreadIds, true, true);
        ThreadInfoWrapper[] wrappers = new ThreadInfoWrapper[threadInfos.length];
        for (int i = 0; i < wrappers.length; i++)
        {
            wrappers[i] = new ThreadInfoWrapper(threadInfos[i]);
        }
        return wrappers;
    }
}
