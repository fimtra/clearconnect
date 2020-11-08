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
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A simple logger that writes to a file and to <code>System.err</code>. Uses a
 * {@link RollingFileAppender} to log to a file in the logs directory called
 * message_yyyyMMddHHmmss.log.
 *
 * @author Ramon Servadei
 * @author Paul Mackinlay
 * @see UtilProperties
 */
public abstract class Log
{
    private static final int LOG_PREFIX_EST_SIZE = 256;
    /**
     * Controls logging to std.err, default is <code>false</code> for performance reasons
     */
    private static final boolean LOG_TO_STDERR = UtilProperties.Values.LOG_TO_STDERR;
    private static final String DELIM = "|";
    private static final String MESSAGE_DELIM = DELIM + "    ";
    private static final ScheduledExecutorService FILE_APPENDER_EXECUTOR =
            ThreadUtils.newScheduledExecutorService("log-file-appender", 1);
    private static final Object lock = new Object();
    private static final Object qLock = new Object();
    private static final LazyObject<ExecutorService> CONSOLE_WRITER_EXECUTOR =
            new LazyObject<>(() -> ThreadUtils.newSingleThreadExecutorService("console-writer"),
                    ExecutorService::shutdown);
    private static PrintStream consoleStream = System.err;

    static final Queue<LogMessage> LOG_MESSAGE_QUEUE = new ArrayDeque<>();
    static final CharSequence LINE_SEPARATOR = SystemUtils.lineSeparator();
    static final RollingFileAppender FILE_APPENDER;
    private static boolean exceptionEncountered;

    static final long timeRefMillis;
    static final long timeRefNanos;

    static
    {
        synchronized (Log.class)
        {
            // need these to execute atomically
            timeRefNanos = System.nanoTime();
            timeRefMillis = System.currentTimeMillis();
        }

        final int periodMillis = UtilProperties.Values.LOG_FLUSH_PERIOD_MILLIS;
        FILE_APPENDER_EXECUTOR.scheduleWithFixedDelay(Log::flushMessages, periodMillis, periodMillis,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Encapsulates a log message with logic to print to a stream
     *
     * @author Ramon Servadei
     */
    private static final class LogMessage
    {
        private static final FastDateFormat fastDateFormat = new FastDateFormat();

        final String threadName;
        final Object source;
        final CharSequence[] messages;
        final long timeNanos;
        String formattedMessage;

        LogMessage(Thread t, Object source, CharSequence... messages)
        {
            this.threadName = t.getName();
            this.source = source;
            this.messages = messages;
            this.timeNanos = System.nanoTime();
        }

        String getTime()
        {
            return fastDateFormat.yyyyMMddHHmmssSSS(
                    timeRefMillis + (long) ((this.timeNanos - timeRefNanos) * 0.000001d));
        }

        void print(PrintStream consoleStream)
        {
            consoleStream.print(generateMessage());
        }

        void print() throws IOException
        {
            FILE_APPENDER.append(generateMessage());
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
                if (this.messages[i] != null)
                {
                    len += this.messages[i].length();
                }
            }
            final StringBuilder sb = new StringBuilder(len);
            final Class<?> c =
                    (this.source instanceof Class<?> ? (Class<?>) this.source : this.source.getClass());
            String classSimpleName = cachedSimpleNames.get(c);
            if (classSimpleName == null)
            {
                if (c.isAnonymousClass())
                {
                    classSimpleName = c.getName();
                    classSimpleName = classSimpleName.substring(classSimpleName.lastIndexOf(".") + 1);
                }
                else
                {
                    classSimpleName = c.getSimpleName();
                }
                cachedSimpleNames.put(c, classSimpleName);
            }
            sb.append(getTime()).append(DELIM).append(this.threadName).append(DELIM).append(
                    classSimpleName).append(":").append(System.identityHashCode(this.source)).append(
                    MESSAGE_DELIM);
            for (int i = 0; i < this.messages.length; i++)
            {
                sb.append(this.messages[i]);
            }
            sb.append(LINE_SEPARATOR);

            this.formattedMessage = sb.toString();

            return this.formattedMessage;
        }
    }

    final static ConcurrentHashMap<Class<?>, String> cachedSimpleNames = new ConcurrentHashMap<>();

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
        ThreadUtils.newThread(() -> {
            if (UtilProperties.Values.ARCHIVE_LOGS_OLDER_THAN_MINUTES > 0)
            {
                FileUtils.archiveLogs(UtilProperties.Values.ARCHIVE_LOGS_OLDER_THAN_MINUTES);
            }
            if (UtilProperties.Values.PURGE_ARCHIVE_LOGS_OLDER_THAN_MINUTES > 0)
            {
                FileUtils.purgeArchiveLogs(UtilProperties.Values.PURGE_ARCHIVE_LOGS_OLDER_THAN_MINUTES);
            }
        }, "log-archiver").start();

        FILE_APPENDER = RollingFileAppender.createStandardRollingFileAppender("messages",
                UtilProperties.Values.LOG_DIR);
        System.out.println("Log file " + FILE_APPENDER);
    }

    private Log()
    {
        super();
    }

    /**
     * Set the PrintStream to use for console messages
     *
     * @param stream the console stream to use
     */
    public static void setConsoleStream(PrintStream stream)
    {
        CONSOLE_WRITER_EXECUTOR.get().execute(() -> {
            synchronized (lock)
            {
                consoleStream = stream;
            }
        });
    }

    public static void log(Object source, String... messages)
    {
        log(new LogMessage(Thread.currentThread(), source, messages));
    }

    public static void log(Object source, String message, Throwable t)
    {
        final StringWriter stringWriter = new StringWriter(1024);
        try (final PrintWriter pw = new PrintWriter(stringWriter);)
        {
            pw.println();
            t.printStackTrace(pw);
            final LogMessage logMessage = new LogMessage(Thread.currentThread(), source, message, stringWriter.toString());
            log(logMessage);
        }
    }

    private static void log(final LogMessage message)
    {
        synchronized (qLock)
        {
            LOG_MESSAGE_QUEUE.add(message);
        }

        if (LOG_TO_STDERR)
        {
            CONSOLE_WRITER_EXECUTOR.get().execute(() -> {
                synchronized (lock)
                {
                    message.print(consoleStream);
                }
            });
        }
    }

    /**
     * Create a banner message
     */
    public static void banner(Object source, String message)
    {
        // note: use "\n" to cover unix "\n" and windows "\r\n"
        final String[] elements = message.split("\n");
        int len = elements[0].length();
        int i;
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
        synchronized (lock)
        {
            LogMessage message;
            if (exceptionEncountered)
            {
                while ((message = pollMessages()) != null)
                {
                    message.print(System.err);
                }
            }
            else
            {
                while ((message = pollMessages()) != null)
                {
                    try
                    {
                        message.print();
                    }
                    catch (IOException e)
                    {
                        panic(e);
                        // print the remainder
                        while ((message = pollMessages()) != null)
                        {
                            message.print(System.err);
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

    private static LogMessage pollMessages()
    {
        synchronized (qLock)
        {
            return LOG_MESSAGE_QUEUE.poll();
        }
    }

    private static void panic(IOException e)
    {
        if (!exceptionEncountered)
        {
            synchronized (System.err)
            {
                System.err.println(
                        "ALERT! LOG MESSAGE(S) LOST! Log output switching to stderr. See exception below.");
                e.printStackTrace();
            }
            exceptionEncountered = true;
        }
    }
}
