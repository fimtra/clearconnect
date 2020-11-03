/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei
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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods used to interact with the filesystem.
 *
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
public abstract class FileUtils
{

    private FileUtils()
    {
        // Not for instantiation
    }

    /**
     * Filter that accepts files with allowed extensions.
     */
    public static final class ExtensionFileFilter implements FileFilter
    {

        private final String[] allowedFileExtensions;

        /**
         * Filters files that have an allowed file extension. Lowercase and uppercase extensions are
         * ignored, so .ext, .EXT, .eXt are all matched.
         *
         * @param allowedFileExtensions the extenstions to allow, specified <b>without a "."</b> so "ext" NOT ".ext"
         */
        public ExtensionFileFilter(String... allowedFileExtensions)
        {
            this.allowedFileExtensions = allowedFileExtensions;
        }

        @Override
        public boolean accept(File pathname)
        {
            for (String allowedExt : this.allowedFileExtensions)
            {
                StringBuilder extBuilder = (new StringBuilder()).append(".").append(allowedExt);
                if (pathname.getName().toLowerCase().endsWith(extBuilder.toString().toLowerCase()))
                {
                    return true;
                }
            }
            return false;
        }
    }

    private static File logDirCanonical;
    private static File archiveDirCanonical;

    static
    {
        try
        {
            logDirCanonical = new File(UtilProperties.Values.LOG_DIR).getCanonicalFile();
        }
        catch (IOException e)
        {
            logDirCanonical = null;
        }
        try
        {
            archiveDirCanonical = new File(UtilProperties.Values.ARCHIVE_DIR).getCanonicalFile();
        }
        catch (IOException e)
        {
            archiveDirCanonical = null;
        }
    }

    /**
     * @return a {@link List} of {@link File}s in a directory with {@link File}s that are filtered
     * using the fileFilter.
     * @throws IllegalArgumentException if the directory parameter is not a filesystem directory.
     */
    public static File[] readFiles(File directory, FileFilter fileFilter)
    {
        if (!directory.isDirectory())
        {
            throw new IllegalArgumentException(directory.getName() + " is not a directory");
        }
        return directory.listFiles(fileFilter);
    }

    /**
     * Copies, recursively, the contents of the srcDir to the targetDir. This creates the targetDir
     * if it does not exist.
     * <p>
     * This is non-atomic.
     */
    public static void copyRecursive(File srcDir, File targetDir) throws IOException
    {
        if (!targetDir.exists() && !targetDir.mkdir())
        {
            throw new IOException("Could not create target dir: " + targetDir);
        }
        final File[] listFiles = srcDir.listFiles();
        if (listFiles == null)
        {
            return;
        }
        for (File file : listFiles)
        {
            if (file.isDirectory())
            {
                copyRecursive(file, new File(targetDir, file.getName()));
            }
            else
            {
                copyFile(file, new File(targetDir, file.getName()));
            }
        }
    }

    /**
     * Deletes all files in the directory, recursively deleting sub-directories of this directory.
     * Depth first scanning.
     * <p>
     * This is non-atomic.
     */
    public static void clearDirectory(File src) throws IOException
    {
        if (src.exists() && src.isDirectory())
        {
            for (File file : src.listFiles())
            {
                if (file.isDirectory())
                {
                    deleteRecursive(file);
                }
                else
                {
                    if (!file.delete())
                    {
                        throw new IOException("Could not delete: " + file);
                    }
                }
            }
        }
    }

    /**
     * Deletes the file, recursively scanning sub-directories if its a directory. Depth first
     * scanning.
     * <p>
     * This is non-atomic.
     */
    public static void deleteRecursive(File src) throws IOException
    {
        clearDirectory(src);
        if (src.exists() && !src.delete())
        {
            throw new IOException("Could not delete: " + src);
        }
    }

    /**
     * Move the src to the dest (in an atomic manner if possible)
     * <p>
     * <ul>
     * <li>
     * For directories, this performs a recursive delete of the dest then a rename of src to dest.
     * <li>For files, this copies the src to the dest and then deletes the src.
     * </ul>
     */
    public static void move(File src, File dest) throws IOException
    {
        if (src.isDirectory())
        {
            deleteRecursive(dest);
            int i = 0;
            final int sleepTime = 100;
            final int maxAttempts = 1000 / sleepTime;
            // the destination has been logically deleted...spin until we can rename
            while (!src.renameTo(dest) && i++ < maxAttempts)
            {
                Log.log(FileUtils.class, "Could not rename ", ObjectUtils.safeToString(src), " to ",
                        ObjectUtils.safeToString(dest), ", retrying in ", Integer.toString(sleepTime),
                        "ms...");
                ThreadUtils.sleep(sleepTime);
            }
            if (!dest.exists())
            {
                throw new IOException("Could not rename " + src + " to " + dest);
            }
        }
        else
        {
            moveNonAtomic(src, dest);
        }
    }

    /**
     * Create the directory (if it already exists, this does nothing).
     *
     * @throws IOException if the directory could not be created
     */
    public static File createDir(File dir) throws IOException
    {
        if (!dir.exists() && !dir.mkdir())
        {
            throw new IOException("Could not create directory " + dir);
        }
        return dir;
    }

    /**
     * Archives all files that are in the log directory that are olderThanMinutes. Each archived file is gzipped, suffixed with
     * .gz and put into the archive directory.
     */
    public static void archiveLogs(long olderThanMinutes)
    {
        if (logDirCanonical != null && logDirCanonical.exists() && logDirCanonical.isDirectory())
        {
            for (File file : FileUtils.findFiles(logDirCanonical, olderThanMinutes))
            {
                if (!file.getName().endsWith(".gz"))
                {
                    boolean isGzipped = FileUtils.gzip(file, archiveDirCanonical);
                    if (isGzipped)
                    {
                        file.delete();
                    }
                    else
                    {
                        Log.log(FileUtils.class, "Could not gzip ", file.getAbsolutePath());
                    }
                }
                else
                {
                    // move it to the archive dir
                    try
                    {
                        FileUtils.move(file, new File(archiveDirCanonical, file.getName()));
                    }
                    catch (IOException e)
                    {
                        Log.log(FileUtils.class, "Could not move to archive dir: " + file.getAbsolutePath(),
                                e);
                    }
                }
            }
        }
    }

    /**
     * Deletes all archived log files that are olderThanMinutes.
     */
    public static void purgeArchiveLogs(long olderThanMinutes)
    {
        if (archiveDirCanonical != null && archiveDirCanonical.exists() && archiveDirCanonical.isDirectory())
        {
            for (File file : FileUtils.findFiles(archiveDirCanonical, olderThanMinutes))
            {
                file.delete();
            }
        }
    }

    public static void copyFile(File src, File dest) throws IOException
    {
        if (!dest.exists())
        {
            dest.createNewFile();
        }
        fastCopyFile(src, dest);
    }

    private static void moveNonAtomic(File src, File dest) throws IOException
    {
        fastCopyFile(src, dest);
        src.delete();
    }

    private static void fastCopyFile(final File sourceFile, final File targetFile) throws IOException
    {
        try (FileInputStream fileInputStream = new FileInputStream(sourceFile);
             FileChannel sourceChannel = fileInputStream.getChannel();
             FileOutputStream fileOutputStream = new FileOutputStream(targetFile);
             FileChannel destinationChannel = fileOutputStream.getChannel())
        {
            sourceChannel.transferTo(0, sourceChannel.size(), destinationChannel);
        }
    }

    /**
     * Call {@link Closeable#close()} on the target, catching any exception
     *
     * @param c the target to close
     */
    public static void safeClose(Closeable c)
    {
        if (c != null)
        {
            try
            {
                c.close();
            }
            catch (Exception e)
            {
                Log.log(FileUtils.class, "Could not close " + ObjectUtils.safeToString(c), e);
            }
        }
    }

    /**
     * Convenience method to construct a log file in the directory with a prefix and a suffix of
     * <code>_yyyyMMddHHmmss.log</code>
     *
     * @param directory  the directory for the file
     * @param filePrefix the prefix for the file
     * @return the file
     */
    public static File createLogFile_yyyyMMddHHmmss(String directory, final String filePrefix)
    {
        final File fileDirectory = new File(directory);
        fileDirectory.mkdir();
        String yyyyMMddHHmmssSSS = new FastDateFormat().yyyyMMddHHmmssSSS(System.currentTimeMillis());
        yyyyMMddHHmmssSSS = yyyyMMddHHmmssSSS.replace(":", "");
        yyyyMMddHHmmssSSS = yyyyMMddHHmmssSSS.replace("-", "_");
        yyyyMMddHHmmssSSS = yyyyMMddHHmmssSSS.substring(0, 15);
        return new File(fileDirectory, filePrefix + "_" + yyyyMMddHHmmssSSS + ".log");
    }

    /**
     * Will gzip the sourceFile add a .gz file extension and save it in the gzipFileDir. The gzipFileDir will
     * be created if it does not exist.
     *
     * @return true if successful. If unsuccessful logging will give the reason.
     */
    public static boolean gzip(File sourceFile, File gzipFileDir)
    {
        if (gzipFileDir.exists() && !gzipFileDir.isDirectory())
        {
            Log.log(FileUtils.class, "gzipFileDir [", gzipFileDir.getAbsolutePath(),
                    "] exists and it is not a directory");
            return false;
        }
        if (!gzipFileDir.exists())
        {
            boolean created = gzipFileDir.mkdirs();
            if (!created)
            {
                Log.log(FileUtils.class, "It is not possible to create gzipFileDir [",
                        gzipFileDir.getAbsolutePath(), "]");
                return false;
            }
        }
        try
        {
            File gzipFile = new File(gzipFileDir, sourceFile.getName() + ".gz");
            InputStream inputStream = new FileInputStream(sourceFile);
            OutputStream outputStream = new FileOutputStream(gzipFile);
            GZipUtils.compressInputToOutput(inputStream, outputStream);
            outputStream.close();
            inputStream.close();
            return true;
        }
        catch (IOException e)
        {
            Log.log(FileUtils.class, "An error occured while gzipping file [", sourceFile.getName(), "] in [",
                    gzipFileDir.getAbsolutePath(), "]");
            return false;
        }
    }

    /**
     * Find files in the directory that are older than the specified number of minutes.
     *
     * @param dir              the directory to scan
     * @param olderThanMinutes the age in minutes for files to delete
     */
    public static File[] findFiles(File dir, long olderThanMinutes)
    {
        return readFiles(dir, new OlderThanFileFilter(olderThanMinutes, TimeUnit.MINUTES));
    }

    /**
     * Delete files in the directory that have the prefix and are older than the specified number of
     * minutes.
     *
     * @param directory                 the directory to scan
     * @param olderThanMinutes          the age in minutes for files to delete
     * @param prefixToMatchWhenDeleting the file prefix to match for eligible files
     */
    public static void deleteFiles(File directory, final long olderThanMinutes,
            final String prefixToMatchWhenDeleting)
    {
        File[] toDelete = readFiles(directory,
                new OlderThanPrefixFileFilter(olderThanMinutes, TimeUnit.MINUTES, prefixToMatchWhenDeleting));
        for (File file : toDelete)
        {
            Log.log(FileUtils.class, "DELETING ", ObjectUtils.safeToString(file));
            try
            {
                file.delete();
            }
            catch (Exception e)
            {
                Log.log(FileUtils.class, "ERROR DELETING " + file, e);
            }
        }
    }

    /**
     * Write the input stream to a file.
     * <p>
     * The input stream is not closed after this.
     *
     * @param in   the input stream
     * @param file the file to write to
     */
    public static void writeInputStreamToFile(InputStream in, File file) throws IOException
    {
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file)))
        {
            final byte[] chunk = new byte[1024];
            int len;
            while ((len = in.read(chunk, 0, 1024)) > -1)
            {
                out.write(chunk, 0, len);
            }
            out.flush();
        }
    }

    private static class OlderThanFileFilter implements FileFilter
    {

        private final long olderThan;
        private final TimeUnit timeUnit;

        /**
         * Filters files which have a last modified time that is olderThan the timeUnit.
         */
        public OlderThanFileFilter(long olderThan, TimeUnit timeUnit)
        {
            this.olderThan = olderThan;
            this.timeUnit = timeUnit;
        }

        @Override
        public boolean accept(File file)
        {
            if (file.isFile() && file.lastModified() < System.currentTimeMillis() - this.timeUnit.toMillis(
                    this.olderThan))
            {
                return true;
            }
            return false;
        }
    }

    private static class OlderThanPrefixFileFilter extends OlderThanFileFilter
    {

        private final String filenamePrefix;

        public OlderThanPrefixFileFilter(long olderThan, TimeUnit timeUnit, String filenamePrefix)
        {
            super(olderThan, timeUnit);
            this.filenamePrefix = filenamePrefix;
        }

        @Override
        public boolean accept(File file)
        {
            if (file.getName().startsWith(this.filenamePrefix, 0))
            {
                return super.accept(file);
            }
            return false;
        }
    }

}
