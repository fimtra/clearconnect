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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.Flushable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * An {@link Appendable} implementation that writes to a {@link File} and will roll the file when
 * the allotted number of characters has been written. The rolling convention is to name the current
 * file:
 *
 * <pre>
 * {filename}.{counter}.logged
 * </pre>
 * <p>
 * and then create a new file called {filename}. This all occurs in the same directory as the
 * original file.
 * <p>
 * <b>This is not thread safe</b>
 *
 * @author Ramon Servadei
 */
public final class RollingFileAppender implements Appendable, Closeable, Flushable
{
    /**
     * Create a standard {@link RollingFileAppender} allowing 1M per file, deleting older than 1
     * day.
     *
     * @throws RuntimeException if the file cannot be created due to some {@link IOException}
     */
    public static RollingFileAppender createStandardRollingFileAppender(String fileIdentity,
            String directory)
    {
        final String filePrefix = ThreadUtils.getMainMethodClassSimpleName() + "-" + fileIdentity;
        final File file = FileUtils.createLogFile_yyyyMMddHHmmss(directory, filePrefix);
        try
        {
            return new RollingFileAppender(file, UtilProperties.Values.LOG_FILE_ROLL_SIZE_KB * 1024);
        }
        catch (IOException e)
        {
            final RuntimeException runtimeException =
                    new RuntimeException("Could not create file: " + file, e);
            runtimeException.printStackTrace();
            throw runtimeException;
        }
    }

    static void checkFileWriteable(File file) throws IOException
    {
        if (!file.exists() && !file.createNewFile())
        {
            throw new IOException("Could not create file: " + file);
        }
        if (!file.canWrite())
        {
            throw new IOException("Cannot write to file: " + file);
        }
    }

    /**
     * Combines the interfaces {@link Appendable}, {@link Flushable} and {@link Closeable}
     *
     * @author Ramon Servadei
     */
    interface AppendableFlushableCloseable extends Appendable, Flushable, Closeable
    {
    }

    /**
     * An implementation of {@link AppendableFlushableCloseable} that writes to a file that will
     * roll after it exceeds a certain size.
     *
     * @author Ramon Servadei
     */
    private final class FileWriterAppendableFlushableCloseableImplementation
            implements AppendableFlushableCloseable
    {
        private final int maxChars;
        private final String parent;
        private final String name;
        private File currentFile;
        private int currentCharCount;
        private int rollCount;
        private Writer writer;

        public FileWriterAppendableFlushableCloseableImplementation(File file, int maximumCharacters)
                throws IOException
        {
            this.currentFile = file;
            this.name = this.currentFile.getName();
            this.parent = this.currentFile.getAbsoluteFile().getParentFile().getAbsolutePath();
            checkFileWriteable(this.currentFile);
            this.maxChars = maximumCharacters;
            this.writer = new BufferedWriter(new FileWriter(file));
        }

        @Override
        public Appendable append(CharSequence csq) throws IOException
        {
            try
            {
                checkSize(csq.length());
                this.writer.append(csq);
            }
            catch (IOException e)
            {
                emergencyAction(e, csq);
            }
            return this;
        }

        @Override
        public Appendable append(CharSequence csq, int start, int end) throws IOException
        {
            try
            {
                checkSize(end - start);
                this.writer.append(csq, start, end);
            }
            catch (IOException e)
            {
                emergencyAction(e, csq, start, end);
            }
            return this;
        }

        @Override
        public Appendable append(char c) throws IOException
        {
            try
            {
                checkSize(1);
                this.writer.append(c);
            }
            catch (IOException e)
            {
                emergencyAction(e, c);
            }
            return this;
        }

        @Override
        public void flush() throws IOException
        {
            try
            {
                this.writer.flush();
            }
            catch (IOException e)
            {
                switchToStdErr(e);
            }
        }

        @Override
        public void close() throws IOException
        {
            try
            {
                this.writer.close();
            }
            catch (IOException e)
            {
                switchToStdErr(e);
            }
        }

        @Override
        public String toString()
        {
            return this.currentFile.getAbsolutePath();
        }

        private void checkSize(int charCount) throws IOException
        {
            this.currentCharCount += charCount;

            if (this.currentCharCount >= this.maxChars)
            {
                final String rolledFileName = this.name + "." + this.rollCount++ + ".logged";
                this.writer.append("file closed as ").append(rolledFileName);
                this.writer.close();
                final File rolledFile = new File(this.parent, rolledFileName);
                if (this.currentFile.renameTo(rolledFile))
                {
                    this.currentFile = new File(this.parent, this.name);
                    checkFileWriteable(this.currentFile);
                    this.currentCharCount = charCount;
                    this.writer = new BufferedWriter(new FileWriter(this.currentFile));

                    if (UtilProperties.Values.COMPRESS_ROLLED_LOGS)
                    {
                        if (FileUtils.gzip(rolledFile, new File(this.parent)))
                        {
                            if (!rolledFile.delete())
                            {
                                System.out.println("Could not delete: " + rolledFile);
                            }
                        }
                        else
                        {
                            System.out.println("Could not gzip: " + rolledFile);
                        }
                    }
                }
                else
                {
                    throw new IOException("Could not rename " + this.currentFile + " to " + rolledFile);
                }
            }
        }
    }

    /**
     * An {@link AppendableFlushableCloseable} that simply writes to std.err
     *
     * @author Ramon Servadei
     */
    private static final class StdErrAppendableFlushableCloseable implements AppendableFlushableCloseable
    {
        private final PrintWriter stdErr = new PrintWriter(System.err);

        public StdErrAppendableFlushableCloseable()
        {
        }

        @Override
        public Appendable append(CharSequence csq, int start, int end)
        {
            this.stdErr.append(csq, start, end);
            return this;
        }

        @Override
        public Appendable append(char c)
        {
            this.stdErr.append(c);
            return this;
        }

        @Override
        public Appendable append(CharSequence csq)
        {
            this.stdErr.append(csq);
            return this;
        }

        @Override
        public void flush()
        {
            // noop
        }

        @Override
        public void close()
        {
            // noop
        }
    }

    private static final StdErrAppendableFlushableCloseable STD_ERR_APPENDER =
            new StdErrAppendableFlushableCloseable();

    AppendableFlushableCloseable delegate;

    /**
     * Construct an instance writing to the given file.
     *
     * @param file              the file to write to
     * @param maximumCharacters the maximum number of characters to write to the file before rolling to a new file
     */
    public RollingFileAppender(final File file, int maximumCharacters) throws IOException
    {
        if (maximumCharacters <= 0)
        {
            throw new IOException("Cannot have negative or 0 maximum characters");
        }

        this.delegate = new FileWriterAppendableFlushableCloseableImplementation(file, maximumCharacters);
    }

    /**
     * @deprecated
     */
    @Deprecated
    public RollingFileAppender(final File file, int maximumCharacters, final long olderThanMinutes,
            final String prefixToMatchWhenDeleting) throws IOException
    {
        this(file, maximumCharacters);
    }

    @Override
    public String toString()
    {
        return this.delegate.toString();
    }

    @Override
    public Appendable append(CharSequence csq) throws IOException
    {
        this.delegate.append(csq);
        return this;
    }

    @Override
    public Appendable append(CharSequence csq, int start, int end) throws IOException
    {
        this.delegate.append(csq, start, end);
        return this;
    }

    @Override
    public Appendable append(char c) throws IOException
    {
        this.delegate.append(c);
        return this;
    }

    @Override
    public void flush() throws IOException
    {
        this.delegate.flush();
    }

    @Override
    public void close() throws IOException
    {
        this.delegate.close();
    }

    void emergencyAction(IOException e, char c) throws IOException
    {
        STD_ERR_APPENDER.append(c);
        switchToStdErr(e);
    }

    void emergencyAction(IOException e, CharSequence csq, int start, int end) throws IOException
    {
        STD_ERR_APPENDER.append(csq, start, end);
        switchToStdErr(e);
    }

    void emergencyAction(IOException e, CharSequence csq) throws IOException
    {
        STD_ERR_APPENDER.append(csq);
        switchToStdErr(e);
    }

    void switchToStdErr(IOException e) throws IOException
    {
        try
        {
            this.delegate.close();
        }
        catch (Exception closeException)
        {
            synchronized (System.err)
            {
                System.err.println(
                        "ALERT! Could not close stream for " + RollingFileAppender.class.getSimpleName() + " "
                                + this
                                + " but switching to stderr anyway due to emergency situation (see message following this).");
                e.printStackTrace();
            }
        }
        synchronized (System.err)
        {
            System.err.println("ALERT! " + RollingFileAppender.class.getSimpleName() + " " + this
                    + " output switching to stderr. See exception below.");
            e.printStackTrace();
        }
        this.delegate = STD_ERR_APPENDER;
        throw e;
    }
}
