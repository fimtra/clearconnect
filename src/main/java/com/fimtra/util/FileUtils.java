/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei 
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

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods used to interact with the filesystem.
 *
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
public abstract class FileUtils {

    public static final String recordFileExtension = "record";
    public static final String propertyFileExtension = "properties";

    private FileUtils() {
        // Not for instantiation
    }

    public static final String getRecordNameFromFile(File recordFile) {
        StringBuilder extBuilder = (new StringBuilder()).append(".").append(recordFileExtension);
        String fileName = recordFile.getName();
        if (fileName.endsWith(extBuilder.toString())) {
            return fileName.substring(0, fileName.lastIndexOf("."));
        }
        throw new IllegalArgumentException("The record file [" + recordFile.getName() + "] should have the extension ["
                + recordFileExtension + "]");
    }

    /**
     * @return a {@link List} of {@link File}s in a directory with {@link File}s that are filtered
     *         using the fileFilter.
     * @throws IllegalArgumentException
     *             if the directory parameter is not a filesystem directory.
     */
    public static final File[] readFiles(File directory, FileFilter fileFilter) {
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException(directory.getName() + " is not a directory");
        }
        return directory.listFiles(fileFilter);
    }

    public static final class ExtensionFileFilter implements FileFilter {

        private final String[] allowedFileExtensions;

        /**
         * Filters files that have an allowed file extension. Lowercase and uppercase extensions are
         * ignored, so .ext, .EXT, .eXt are all matched.
         */
        public ExtensionFileFilter(String... allowedFileExtensions) {
            this.allowedFileExtensions = allowedFileExtensions;
        }

        @Override
        public boolean accept(File pathname) {
            for (String allowedExt : this.allowedFileExtensions) {
                StringBuilder extBuilder = (new StringBuilder()).append(".").append(allowedExt);
                if (pathname.getName().toLowerCase().endsWith(extBuilder.toString().toLowerCase())) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Copies, recursively, the contents of the srcDir to the targetDir. This creates the targetDir
     * if it does not exist.
     * <p>
     * This is non-atomic.
     */
    public static final void copyRecursive(File srcDir, File targetDir) throws IOException {
        if (!targetDir.exists() && !targetDir.mkdir()) {
            throw new IOException("Could not create target dir: " + targetDir);
        }
        final File[] listFiles = srcDir.listFiles();
        if (listFiles == null) {
            return;
        }
        for (File file : listFiles) {
            if (file.isDirectory()) {
                copyRecursive(file, new File(targetDir, file.getName()));
            } else {
                copyFile(file, new File(targetDir, file.getName()));
            }
        }
    }

    /**
     * Deletes all files in the directory, recursively deleting sub-directories of this directory.
     * Depth first scanning.
     * <p>
     * This is non-atomic.
     *
     * @throws IOException
     */
    public static final void clearDirectory(File src) throws IOException {
        if (src.exists() && src.isDirectory()) {
            for (File file : src.listFiles()) {
                if (file.isDirectory()) {
                    deleteRecursive(file);
                } else {
                    if (!file.delete()) {
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
     *
     * @throws IOException
     */
    public static final void deleteRecursive(File src) throws IOException {
        clearDirectory(src);
        if (src.exists() && !src.delete()) {
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
     *
     * @throws IOException
     */
    public static final void move(File src, File dest) throws IOException {
        if (src.isDirectory()) {
            deleteRecursive(dest);
            int i = 0;
            final int sleepTime = 100;
            final int maxAttempts = 1000 / sleepTime;
            // the destination has been logically deleted...spin until we can rename
            while (!src.renameTo(dest) && i++ < maxAttempts) {
                Log.log(FileUtils.class, "Could not rename ", ObjectUtils.safeToString(src), " to ", ObjectUtils.safeToString(dest), ", retrying in ", Integer.toString(sleepTime), "ms...");
                ThreadUtils.sleep(sleepTime);
            }
            if (!dest.exists()) {
                throw new IOException("Could not rename " + src + " to " + dest);
            }
        } else {
            moveNonAtomic(src, dest);
        }
    }

    /**
     * Create the directory (if it already exists, this does nothing).
     *
     * @throws IOException
     *             if the directory could not be created
     */
    public static final File createDir(File dir) throws IOException {
        if (!dir.exists() && !dir.mkdir()) {
            throw new IOException("Could not create directory " + dir);
        }
        return dir;
    }

    private static void copyFile(File src, File dest) throws IOException {
        if (!dest.exists()) {
            dest.createNewFile();
        }
        fastCopyFile(src, dest);
    }

    private static void moveNonAtomic(File src, File dest) throws IOException {
        fastCopyFile(src, dest);
        src.delete();
    }
    
    private static void fastCopyFile(final File sourceFile, final File targetFile) throws IOException
    {
        FileChannel sourceChannel = null;
        FileChannel destinationChannel = null;
        try
        {
            sourceChannel = new FileInputStream(sourceFile).getChannel();
            destinationChannel = new FileOutputStream(targetFile).getChannel();
            sourceChannel.transferTo(0, sourceChannel.size(), destinationChannel);
        }
        finally
        {
            FileUtils.safeClose(sourceChannel);
            FileUtils.safeClose(destinationChannel);
        }
    }

    /**
     * Call {@link Closeable#close()} on the target, catching any exception
     * 
     * @param c
     *            the target to close
     */
    public static final void safeClose(Closeable c)
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
     * @param directory
     *            the directory for the file
     * @param filePrefix
     *            the prefix for the file
     * @return the file
     */
    public static final File createLogFile_yyyyMMddHHmmss(String directory, final String filePrefix)
    {
        final File fileDirectory = new File(directory);
        fileDirectory.mkdir();
        String yyyyMMddHHmmssSSS = new FastDateFormat().yyyyMMddHHmmssSSS(System.currentTimeMillis());
        yyyyMMddHHmmssSSS = yyyyMMddHHmmssSSS.replace(":", "");
        yyyyMMddHHmmssSSS = yyyyMMddHHmmssSSS.replace("-", "_");
        yyyyMMddHHmmssSSS = yyyyMMddHHmmssSSS.substring(0, 15);
        final File file = new File(fileDirectory, filePrefix + "_" + yyyyMMddHHmmssSSS + ".log");
        return file;
    }

    /**
     * Delete files in the directory that have the prefix and are older than the specified number of
     * minutes.
     * 
     * @param directory
     *            the directory to scan
     * @param olderThanMinutes
     *            the age in minutes for files to delete
     * @param prefixToMatchWhenDeleting
     *            the file prefix to match for eligible files
     */
    public static final void deleteFiles(File directory, final long olderThanMinutes,
        final String prefixToMatchWhenDeleting)
    {
        File[] toDelete = readFiles(directory, new FileFilter()
        {
            @Override
            public boolean accept(File file)
            {
                if (file.isFile()
                    && file.lastModified() < System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(olderThanMinutes)
                    && file.getName().startsWith(prefixToMatchWhenDeleting, 0))
                {
                    return true;
                }
                return false;
            }
        });
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
}
