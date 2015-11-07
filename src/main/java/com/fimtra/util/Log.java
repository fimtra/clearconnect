/*
 * Copyright (c) 2013 Ramon Servadei, Paul Mackinlay 
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

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fimtra.thimble.ThimbleExecutor;

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
    /** Controls logging to std.err, default is <code>false</code> for performance reasons */
    private static final boolean LOG_TO_STDERR = UtilProperties.Values.LOG_TO_STDERR;
    private static final String TAB = "|";
    private static final FastDateFormat fastDateFormat = new FastDateFormat();
    private static final ThimbleExecutor FILE_APPENDER_EXECUTOR = new ThimbleExecutor("LogAsyncFileAppender", 1);
    private static final Lock lock = new ReentrantLock();
    private static PrintStream consoleStream = System.err;

    static final Queue<String> LOG_MESSAGE_QUEUE = new ConcurrentLinkedQueue<String>();
    static final CharSequence LINE_SEPARATOR = SystemUtils.lineSeparator();
    static final RollingFileAppender FILE_APPENDER;
    private static boolean exceptionEncountered;

    static
    {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
        {
            @SuppressWarnings("synthetic-access")
            @Override
            public void run()
            {
                System.err.println("Shutting down logging...");
                FILE_APPENDER_EXECUTOR.destroy();
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
        lock.lock();
        try
        {
            FILE_APPENDER =
                RollingFileAppender.createStandardRollingFileAppender("messages", UtilProperties.Values.LOG_DIR);
            System.out.println("Log file " + FILE_APPENDER);
        }
        finally
        {
            lock.unlock();
        }
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
        lock.lock();
        try
        {
            println(getLogMessage(source, messages));
        }
        finally
        {
            lock.unlock();
        }
    }

    public static final void log(Object source, String message1, String message2, String message3, String message4)
    {
        lock.lock();
        try
        {
            println(getLogMessage(source, message1, message2, message3, message4));
        }
        finally
        {
            lock.unlock();
        }
    }

    public static final void log(Object source, String message1, String message2, String message3)
    {
        lock.lock();
        try
        {
            println(getLogMessage(source, message1, message2, message3));
        }
        finally
        {
            lock.unlock();
        }
    }

    public static final void log(Object source, String message1, String message2)
    {
        lock.lock();
        try
        {
            println(getLogMessage(source, message1, message2));
        }
        finally
        {
            lock.unlock();
        }
    }

    public static final void log(Object source, String message)
    {
        lock.lock();
        try
        {
            println(getLogMessage(source, message));
        }
        finally
        {
            lock.unlock();
        }
    }

    public static final void log(Object source, String message, Throwable t)
    {
        lock.lock();
        try
        {
            StringWriter stringWriter = new StringWriter(1024);
            PrintWriter pw = new PrintWriter(stringWriter);
            pw.println(getLogMessage(source, message));
            t.printStackTrace(pw);
            print(stringWriter.toString());
        }
        finally
        {
            lock.unlock();
        }
    }

    private static String getLogMessage(Object source, CharSequence... messages)
    {
        int len = 256;
        for (int i = 0; i < messages.length; i++)
        {
            len += messages[i] == null ? 4 : messages[i].length();
        }
        StringBuilder sb = new StringBuilder(len);
        sb.append(fastDateFormat.yyyyMMddHHmmssSSS(System.currentTimeMillis())).append(TAB).append(
            Thread.currentThread().getName()).append(TAB).append(
            source instanceof Class ? ((Class<?>) source).getName() : source.getClass().getName()).append(":").append(
            System.identityHashCode(source)).append(TAB);
        for (int i = 0; i < messages.length; i++)
        {
            sb.append(messages[i]);
        }
        return sb.toString();
    }

    private static String getLogMessage(Object source, String message1, String message2, String message3,
        String message4)
    {
        StringBuilder sb = new StringBuilder(256);
        sb.append(fastDateFormat.yyyyMMddHHmmssSSS(System.currentTimeMillis())).append(TAB).append(
            Thread.currentThread().getName()).append(TAB).append(
            source instanceof Class ? ((Class<?>) source).getName() : source.getClass().getName()).append(":").append(
            System.identityHashCode(source)).append(TAB).append(message1).append(message2).append(message3).append(
            message4);
        return sb.toString();
    }

    private static String getLogMessage(Object source, String message1, String message2, String message3)
    {
        StringBuilder sb = new StringBuilder(256);
        sb.append(fastDateFormat.yyyyMMddHHmmssSSS(System.currentTimeMillis())).append(TAB).append(
            Thread.currentThread().getName()).append(TAB).append(
            source instanceof Class ? ((Class<?>) source).getName() : source.getClass().getName()).append(":").append(
            System.identityHashCode(source)).append(TAB).append(message1).append(message2).append(message3);
        return sb.toString();
    }

    private static String getLogMessage(Object source, String message1, String message2)
    {
        StringBuilder sb = new StringBuilder(256);
        sb.append(fastDateFormat.yyyyMMddHHmmssSSS(System.currentTimeMillis())).append(TAB).append(
            Thread.currentThread().getName()).append(TAB).append(
            source instanceof Class ? ((Class<?>) source).getName() : source.getClass().getName()).append(":").append(
            System.identityHashCode(source)).append(TAB).append(message1).append(message2);
        return sb.toString();
    }

    private static String getLogMessage(Object source, String message)
    {
        StringBuilder sb = new StringBuilder(256);
        sb.append(fastDateFormat.yyyyMMddHHmmssSSS(System.currentTimeMillis())).append(TAB).append(
            Thread.currentThread().getName()).append(TAB).append(
            source instanceof Class ? ((Class<?>) source).getName() : source.getClass().getName()).append(":").append(
            System.identityHashCode(source)).append(TAB).append(message);
        return sb.toString();
    }

    private static void println(final String logMessage)
    {
        final StringBuilder sb = new StringBuilder(logMessage.length() + LINE_SEPARATOR.length());
        sb.append(logMessage).append(LINE_SEPARATOR);
        print(sb.toString());
    }

    private static void print(final String logMessageWithLineSeparator)
    {
        LOG_MESSAGE_QUEUE.add(logMessageWithLineSeparator);
        FILE_APPENDER_EXECUTOR.execute(new Runnable()
        {
            @Override
            public void run()
            {
                flushMessages();
            }
        });
        if (LOG_TO_STDERR)
        {
            consoleStream.print(logMessageWithLineSeparator);
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
        final int size = LOG_MESSAGE_QUEUE.size();
        if (size > 0)
        {
            int i = 0;
            if (exceptionEncountered)
            {
                for (; i < size; i++)
                {
                    System.err.append(LOG_MESSAGE_QUEUE.poll());
                }
            }
            else
            {
                for (; i < size; i++)
                {
                    try
                    {
                        FILE_APPENDER.append(LOG_MESSAGE_QUEUE.poll());
                    }
                    catch (IOException e)
                    {
                        panic(e);
                        // print the remainder
                        for (i++; i < size; i++)
                        {
                            System.err.append(LOG_MESSAGE_QUEUE.poll());
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
