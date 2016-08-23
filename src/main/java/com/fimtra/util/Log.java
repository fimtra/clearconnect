/*
 * Copyright (c) 2013 Ramon Servadei, Paul Mackinlay
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.util;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple logger that writes to a file and to <code>System.err</code>. Uses a
 * {@link RollingFileAppender} to log to a file in the logs directory called
 * message_yyyyMMddHHmmss.log.
 * 
 * @see UtilProperties
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public abstract class Log
{
    private static final int LOG_PREFIX_EST_SIZE = 256;
    /** Controls logging to std.err, default is <code>false</code> for performance reasons */
    private static final boolean LOG_TO_STDERR = UtilProperties.Values.LOG_TO_STDERR;
    private static final String TAB = "|";
    private static final ScheduledExecutorService FILE_APPENDER_EXECUTOR = ThreadUtils.newScheduledExecutorService("LogAsyncFileAppender", 1);
    private static final Lock lock = new ReentrantLock();
    private static PrintStream consoleStream = System.err;

    static final Queue<LogMessage> LOG_MESSAGE_QUEUE = new ConcurrentLinkedQueue<LogMessage>();
    static final CharSequence LINE_SEPARATOR = SystemUtils.lineSeparator();
    static final RollingFileAppender FILE_APPENDER;
    private static boolean exceptionEncountered;
    private static boolean flushTaskPending;
    private static final Runnable flushTask = new Runnable()
    {
        @Override
        public void run()
        {
            flushMessages();
        }
    };

    /**
     * Encapsulates a log message with logic to print to a stream
     * 
     * @author Ramon Servadei
     */
    private static final class LogMessage
    {
        private static final FastDateFormat fastDateFormat = new FastDateFormat();
        
        final Thread t;
        final Object source;
        final CharSequence[] messages;
        final long timeMillis;
        String formattedMessage;

        LogMessage(Thread t, Object source, CharSequence... messages)
        {
            this.t = t;
            this.source = source;
            this.messages = messages;
            this.timeMillis = System.currentTimeMillis();
        }

        String getTime()
        {
            return fastDateFormat.yyyyMMddHHmmssSSS(this.timeMillis);
        }

        void print(PrintStream consoleStream)
        {
            consoleStream.print(generateMessage());
        }

        void print(RollingFileAppender fileAppender) throws IOException
        {
            fileAppender.append(generateMessage());
        }

        private String generateMessage()
        {
            if (this.formattedMessage != null)
            {
                return this.formattedMessage;
            }
            
            int len = LOG_PREFIX_EST_SIZE;
            for (int i = 0; i < this.messages.length; i++)
            {
                if(this.messages[i] != null)
                {
                    len += this.messages[i].length();
                }
            }
            final StringBuilder sb = new StringBuilder(len);
            sb.append(getTime()).append(TAB).append(this.t.getName()).append(TAB).append(this.source instanceof Class
                ? ((Class<?>) this.source).getName() : this.source.getClass().getName()).append(":").append(
                    Integer.toString(System.identityHashCode(this.source))).append(TAB);
            for (int i = 0; i < this.messages.length; i++)
            {
                sb.append(this.messages[i]);
            }
            sb.append(LINE_SEPARATOR);
            
            this.formattedMessage = sb.toString();
            
            return this.formattedMessage;
        }
    }

    static
    {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
        {
            @SuppressWarnings("synthetic-access")
            @Override
            public void run()
            {
                System.err.println("Shutting down logging...");
                FILE_APPENDER_EXECUTOR.shutdownNow();
                flushMessages();
                try
                {
                    FILE_APPENDER.flush();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
                try
                {
                    FILE_APPENDER.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }));

        // use a thread to perform archiving/purging to prevent startup delays
        ThreadUtils.newThread(new Runnable()
        {
            @Override
            public void run()
            {
                if (UtilProperties.Values.ARCHIVE_LOGS_OLDER_THAN_MINUTES > 0)
                {
                    FileUtils.archiveLogs(UtilProperties.Values.ARCHIVE_LOGS_OLDER_THAN_MINUTES);
                }
                if (UtilProperties.Values.PURGE_ARCHIVE_LOGS_OLDER_THAN_MINUTES > 0)
                {
                    FileUtils.purgeArchiveLogs(UtilProperties.Values.PURGE_ARCHIVE_LOGS_OLDER_THAN_MINUTES);
                }
            }
        }, "log-archiver").start();

        FILE_APPENDER =
            RollingFileAppender.createStandardRollingFileAppender("messages", UtilProperties.Values.LOG_DIR);
        System.out.println("Log file " + FILE_APPENDER);
    }

    private Log()
    {
        super();
    }

    /**
     * Set the PrintStream to use for console messages
     * 
     * @param stream
     *            the console stream to use
     */
    public static final void setConsoleStream(PrintStream stream)
    {
        lock.lock();
        try
        {
            consoleStream = stream;
        }
        finally
        {
            lock.unlock();
        }
    }

    public static final void log(Object source, String... messages)
    {
        log(new LogMessage(Thread.currentThread(), source, messages));
    }

    public static final void log(Object source, String message, Throwable t)
    {
        final StringWriter stringWriter = new StringWriter(1024);
        final PrintWriter pw = new PrintWriter(stringWriter);
        t.printStackTrace(pw);
        final LogMessage logMessage = new LogMessage(Thread.currentThread(), source, message, stringWriter.toString());
        log(logMessage);
    }

    private static void log(final LogMessage message)
    {
        LOG_MESSAGE_QUEUE.add(message);
        if (!flushTaskPending)
        {
            flushTaskPending = true;
            try
            {
                FILE_APPENDER_EXECUTOR.schedule(flushTask, 250, TimeUnit.MILLISECONDS);
            }
            catch (RejectedExecutionException e)
            {
                if (!FILE_APPENDER_EXECUTOR.isShutdown())
                {
                    throw e;
                }
            }
        }
        if (LOG_TO_STDERR)
        {
            lock.lock();
            try
            {
                message.print(consoleStream);
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    /**
     * Create a banner message
     */
    public static final void banner(Object source, String message)
    {
        // note: use "\n" to cover unix "\n" and windows "\r\n"
        final String[] elements = message.split("\n");
        int len = elements[0].length();
        int i = 0;
        for (i = 0; i < elements.length; i++)
        {
            if (len < elements[i].length())
            {
                len = elements[i].length();
            }
        }
        final StringBuilder sb = new StringBuilder(len);
        for (i = 0; i < len; i++)
        {
            sb.append("=");
        }
        final String surround = sb.toString();
        Log.log(source, SystemUtils.lineSeparator(), surround, SystemUtils.lineSeparator(), message,
            SystemUtils.lineSeparator(), surround);
    }

    static void flushMessages()
    {
        flushTaskPending = false;

        lock.lock();
        try
        {
            final int size = LOG_MESSAGE_QUEUE.size();
            if (size > 0)
            {
                int i = 0;
                if (exceptionEncountered)
                {
                    for (; i < size; i++)
                    {
                        LOG_MESSAGE_QUEUE.poll().print(System.err);
                    }
                }
                else
                {
                    for (; i < size; i++)
                    {
                        try
                        {
                            LOG_MESSAGE_QUEUE.poll().print(FILE_APPENDER);
                        }
                        catch (IOException e)
                        {
                            panic(e);
                            // print the remainder
                            for (i++; i < size; i++)
                            {
                                LOG_MESSAGE_QUEUE.poll().print(System.err);
                            }
                            return;
                        }
                    }
                    try
                    {
                        FILE_APPENDER.flush();
                    }
                    catch (IOException e)
                    {
                        panic(e);
                    }
                }
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    private static void panic(IOException e)
    {
        if (!exceptionEncountered)
        {
            synchronized (System.err)
            {
                System.err.println("ALERT! LOG MESSAGE(S) LOST! Log output switching to stderr. See exception below.");
                e.printStackTrace();
            }
            exceptionEncountered = true;
        }
    }
}
