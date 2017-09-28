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
package com.fimtra.datafission.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.DataFissionProperties.Values;
import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IPermissionFilter;
import com.fimtra.datafission.IPublisherContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.ProxyContext.IRemoteSystemRecordNames;
import com.fimtra.datafission.field.BlobValue;
import com.fimtra.thimble.TaskStatistics;
import com.fimtra.thimble.ThimbleExecutor;
import com.fimtra.util.ArrayUtils;
import com.fimtra.util.CharBufferUtils;
import com.fimtra.util.FastDateFormat;
import com.fimtra.util.FileUtils;
import com.fimtra.util.FileUtils.ExtensionFileFilter;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.RollingFileAppender;
import com.fimtra.util.SubscriptionManager;
import com.fimtra.util.SystemUtils;
import com.fimtra.util.ThreadUtils;
import com.fimtra.util.UtilProperties;
import com.fimtra.util.is;

/**
 * Utility objects and methods for datafission core
 * 
 * @author Ramon Servadei, Paul Mackinlay
 */
public final class ContextUtils
{

    /**
     * This listener is attached to the {@link ISystemRecordNames#CONTEXT_RECORDS} and will register
     * an inner listener to any new records in the context.
     * <p>
     * To prevent a memory leak, the {@link #destroy()} <b>MUST</b> be called when application code
     * no longer requires the manager.
     * 
     * @author Ramon Servadei
     */
    public static final class AllRecordsRegistrationManager implements IRecordListener
    {
        final IRecordListener allRecordsListener;
        final IObserverContext context;
        final Set<String> subscribed = new HashSet<String>();

        AllRecordsRegistrationManager(IRecordListener allRecordsListener, IObserverContext context)
        {
            this.allRecordsListener = allRecordsListener;
            this.context = context;
            context.addObserver(this, ISystemRecordNames.CONTEXT_RECORDS);
        }

        public void destroy()
        {
            this.context.removeObserver(this, ISystemRecordNames.CONTEXT_RECORDS);
            List<String> temp = new LinkedList<String>();
            for (String recordName : this.subscribed)
            {
                temp.add(recordName);
            }
            if (temp.size() > 0)
            {
                this.context.removeObserver(this.allRecordsListener, temp.toArray(new String[temp.size()]));
            }
            this.subscribed.clear();
        }

        @Override
        public void onChange(IRecord imageCopy, IRecordChange atomicChange)
        {
            List<String> temp = new LinkedList<String>();
            for (String recordName : atomicChange.getPutEntries().keySet())
            {
                if (!ContextUtils.isSystemRecordName(recordName) && this.subscribed.add(recordName))
                {
                    temp.add(recordName);
                }
            }
            if (temp.size() > 0)
            {
                this.context.addObserver(this.allRecordsListener, temp.toArray(new String[temp.size()]));
            }

            temp = new LinkedList<String>();
            for (String recordName : atomicChange.getRemovedEntries().keySet())
            {
                if (this.subscribed.remove(recordName))
                {
                    temp.add(recordName);
                }
            }
            if (temp.size() > 0)
            {
                this.context.removeObserver(this.allRecordsListener, temp.toArray(new String[temp.size()]));
            }
        }
    }

    static final String FISSION = "fission";
    static final String FISSION_RPC = FISSION + "-rpc";
    static final String FISSION_CORE = FISSION + "-core";
    static final String FISSION_SYSTEM = FISSION + "-system";

    private static final String RECORD_FILE_EXTENSION_NAME = "record";
    static final String RECORD_FILE_EXTENSION = "." + RECORD_FILE_EXTENSION_NAME;
    public static final ExtensionFileFilter RECORD_FILE_FILTER =
        new FileUtils.ExtensionFileFilter(RECORD_FILE_EXTENSION_NAME);

    private static final double INVERSE_1000000 = 1d / 1000000;

    public static final Set<String> SYSTEM_RECORDS;

    static
    {
        Set<String> set = new HashSet<String>();
        set.add(ISystemRecordNames.CONTEXT_RPCS);
        set.add(ISystemRecordNames.CONTEXT_STATUS);
        set.add(ISystemRecordNames.CONTEXT_RECORDS);
        set.add(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        set.add(ISystemRecordNames.CONTEXT_CONNECTIONS);
        SYSTEM_RECORDS = Collections.unmodifiableSet(set);
    }

    /**
     * This is the default shared {@link ThimbleExecutor} used for <b>system record</b> event
     * handling.
     * 
     * @see #SYSTEM_RECORDS
     */
    final static ThimbleExecutor SYSTEM_RECORD_EXECUTOR = new ThimbleExecutor(FISSION_SYSTEM, SYSTEM_RECORDS.size());

    /**
     * This is the default shared {@link ThimbleExecutor} that can be used by all contexts.
     */
    final static ThimbleExecutor CORE_EXECUTOR =
        new ThimbleExecutor(FISSION_CORE, DataFissionProperties.Values.CORE_THREAD_COUNT);

    /**
     * This is dedicated to handle RPC results. If RPC results are handled by the
     * {@link #CORE_EXECUTOR}, a timeout could occur if the result is placed onto the same queue of
     * the thread that is waiting for the result!
     */
    final static ThimbleExecutor RPC_EXECUTOR =
        new ThimbleExecutor(FISSION_RPC, DataFissionProperties.Values.RPC_THREAD_COUNT);

    /**
     * This is the default shared SINGLE-THREAD 'utility scheduler' that is used by all contexts.
     */
    final static ScheduledExecutorService UTILITY_SCHEDULER =
        ThreadUtils.newPermanentScheduledExecutorService("fission-utility", 1);

    final static Map<Object, TaskStatistics> coreSequentialStats = CORE_EXECUTOR.getSequentialTaskStatistics();
    final static RollingFileAppender statisticsLog =
        RollingFileAppender.createStandardRollingFileAppender("Qstats", UtilProperties.Values.LOG_DIR);

    static
    {
        try
        {
            statisticsLog.append("Time, Q, QOverflowInterval, QSubmittedInterval, QSubmittedTotal").append(
                SystemUtils.lineSeparator());
        }
        catch (IOException e)
        {
            Log.log(ContextUtils.class, "Could not log to QStats file", e);
        }
        UTILITY_SCHEDULER.scheduleWithFixedDelay(new Runnable()
        {
            final FastDateFormat fdf = new FastDateFormat();

            @Override
            public void run()
            {

                final Set<ThimbleExecutor> executors = ThimbleExecutor.getExecutors();
                final StringBuilder sb = new StringBuilder(1024);
                final String yyyyMMddHHmmssSSS = this.fdf.yyyyMMddHHmmssSSS(System.currentTimeMillis());
                for (ThimbleExecutor thimbleExecutor : executors)
                {
                    sb.append(yyyyMMddHHmmssSSS).append(", ").append(thimbleExecutor.getName()).append(
                        " coalescing queue, ").append(getStats(thimbleExecutor.getCoalescingTaskStatistics())).append(
                            SystemUtils.lineSeparator());
                    sb.append(yyyyMMddHHmmssSSS).append(", ").append(thimbleExecutor.getName()).append(
                        " sequential queue, ").append(getStats(thimbleExecutor.getSequentialTaskStatistics())).append(
                            SystemUtils.lineSeparator());
                }
                try
                {
                    statisticsLog.append(sb.toString());
                    statisticsLog.flush();
                }
                catch (IOException e)
                {
                    Log.log(ContextUtils.class, "Could not log to QStats file", e);
                }
            }

            final String getStats(Map<Object, TaskStatistics> taskStatisticsMap)
            {
                final TaskStatistics stats = taskStatisticsMap.get(ThimbleExecutor.QUEUE_LEVEL_STATS);
                final StringBuilder sb = new StringBuilder(50);

                long overflow = stats.getIntervalSubmitted() - stats.getIntervalExecuted();
                sb.append(overflow < 0 ? 0 : overflow).append(", ");

                // submitted stats
                sb.append(stats.getIntervalSubmitted()).append(", ");
                sb.append(stats.getTotalSubmitted());

                return sb.toString();
            }
        }, Values.STATS_LOGGING_PERIOD_SECS, Values.STATS_LOGGING_PERIOD_SECS, TimeUnit.SECONDS);
    }

    private static final char LINE_SEPARATOR = '\n';

    public final static Map<String, IValue> EMPTY_MAP = Collections.emptyMap();

    static final Set<String> EMPTY_STRING_SET = Collections.emptySet();
    static final String PROTOCOL_PREFIX = "_";

    /**
     * @return a long[] for the sequential tasks statistics, format {queue-overflow,
     *         queue-total-submitted, queue-total-executed}
     */
    public static long[] getCoreStats()
    {
        final TaskStatistics stats = coreSequentialStats.get(ThimbleExecutor.QUEUE_LEVEL_STATS);
        final long totalSubmitted = stats.getTotalSubmitted();
        final long totalExecuted = stats.getTotalExecuted();
        return new long[] { (totalSubmitted - totalExecuted), totalSubmitted, totalExecuted };
    }

    /**
     * Serialise the state of a context to the directory. Each record in the context is serialized
     * into a distinct file in the directory called {record-name}.record. This does not serialize
     * system records.
     * <p>
     * This will create a backup of the current directory at the same level called
     * {directory-name}-backup, then write the context records to a temp directory at the same level
     * called {directory-name}-temp, then rename the temp directory to the directory argument.
     * <p>
     * <b>This is a non-atomic operation.</b>
     * 
     * @see #serializeRecordToFile(IRecord, File)
     * @param context
     *            the context to serialise
     * @param directory
     *            the directory for the data files for each record in the context
     * @throws IOException
     */
    public static void serializeContextToDirectory(IPublisherContext context, File directory) throws IOException
    {
        final File backupDir = new File(directory.getParent(), directory.getName() + "-backup");
        FileUtils.clearDirectory(backupDir);
        FileUtils.copyRecursive(directory, backupDir);

        final File tmpDir = FileUtils.createDir(new File(directory.getParent(), directory.getName() + "-temp"));
        FileUtils.clearDirectory(tmpDir);

        for (String recordName : context.getRecordNames())
        {
            if (!isSystemRecordName(recordName))
            {
                serializeRecordToFile(context.getOrCreateRecord(recordName), tmpDir);
            }
        }

        FileUtils.move(tmpDir, directory);
    }

    /**
     * Resolve a context's internal records by loading them from data files in directory. This does
     * not resolve system records.
     * <p>
     * <b>NOTE:</b> records that currently exist in the context will have their state merged with
     * the data held in the record file that is loaded.
     * 
     * @see #resolveRecordFromFile(IRecord, File)
     * @param context
     *            the context to resolve
     * @param directory
     *            the directory for the data files for each record in the context
     * @throws IOException
     */
    public static void resolveContextFromDirectory(IPublisherContext context, File directory) throws IOException
    {
        File[] recordFiles = FileUtils.readFiles(directory, RECORD_FILE_FILTER);
        String recordName;
        IRecord record;
        for (File recordFile : recordFiles)
        {
            recordName =
                recordFile.getName().substring(0, recordFile.getName().length() - RECORD_FILE_EXTENSION.length());
            record = context.getOrCreateRecord(recordName);
            resolveRecordFromFile(record, directory);
        }
    }

    /**
     * Convenience method to serialise a record to the directory. The record contents are serialised
     * to a flat file {record-name}.record in the directory.
     * 
     * @param record
     *            the record to serialise
     * @param directory
     *            the directory for the data file
     * @throws IOException
     */
    public static void serializeRecordToFile(IRecord record, File directory) throws IOException
    {
        final File f = new File(directory, record.getName() + RECORD_FILE_EXTENSION + ".tmp");
        final Writer writer = new BufferedWriter(new FileWriter(f));
        if (f.exists() || f.createNewFile())
        {
            try
            {
                record.serializeToStream(writer);
                writer.flush();
            }
            finally
            {
                writer.close();
            }
            FileUtils.move(f, new File(directory, record.getName() + RECORD_FILE_EXTENSION));
        }
        else
        {
            Log.log(ContextUtils.class, "Could not create data file for record ", record.getName());
        }
    }

    /**
     * Convenience method to resolve a record's internal data from a data file in directory. The
     * record contents are serialised in a flat file {record-name}.record in the directory.
     * 
     * @param record
     *            the record to resolve
     * @param directory
     *            the directory for the data file
     * @throws IOException
     */
    public static void resolveRecordFromFile(IRecord record, File directory) throws IOException
    {
        File f = new File(directory, record.getName() + RECORD_FILE_EXTENSION);
        Reader reader = new BufferedReader(new FileReader(f));
        if (f.exists())
        {
            try
            {
                record.resolveFromStream(reader);
            }
            finally
            {
                reader.close();
            }
        }
        else
        {
            Log.log(ContextUtils.class, "No data file for record ", record.getName());
        }
    }

    /**
     * Convenience method to delete a record data file in directory.
     * 
     * @param recordName
     *            the name of the record to be deleted
     * @param directory
     *            the directory for the data file
     * 
     * @return true of the record file was deleted.
     */
    public static boolean deleteRecordFile(String recordName, File directory)
    {
        File f = new File(directory, recordName + RECORD_FILE_EXTENSION);
        if (f.exists())
        {
            return f.delete();
        }
        return false;
    }

    /**
     * Convenience method to get the record name from a record file. This expects the file name to
     * be in the format: {record name}.record
     * 
     * @param recordFile
     *            the record file
     * @return the record name of the file, <code>null</code> if not a record file
     */
    public static final String getRecordNameFromFile(File recordFile)
    {
        final String fileName = recordFile.getName();
        if (fileName.endsWith(RECORD_FILE_EXTENSION))
        {
            return fileName.substring(0, fileName.lastIndexOf("."));
        }
        return null;
    }

    /**
     * @return <code>true</code> if the record name is a reserved name (one of the system
     *         CONTEXT_XXX record names)
     * @see ISystemRecordNames#CONTEXT_RPCS
     * @see ISystemRecordNames#CONTEXT_STATUS
     * @see ISystemRecordNames#CONTEXT_RECORDS
     * @see ISystemRecordNames#CONTEXT_CONNECTIONS
     * @see ISystemRecordNames#CONTEXT_SUBSCRIPTIONS
     */
    public static boolean isSystemRecordName(String name)
    {
        if (name != null && name.startsWith(ISystemRecordNames.CONTEXT, 0))
        {
            return SYSTEM_RECORDS.contains(name);
        }
        return false;
    }

    /**
     * A utility to register a listener that will be registered against all (non-system) records in
     * a context. This creates an adapter listener that is registered to the context's
     * {@link ISystemRecordNames#CONTEXT_RECORDS} record; when new records are added the
     * allRecordsListener is added as an observer to the new record. <b>When finished with this, the
     * {@link AllRecordsRegistrationManager#destroy} method MUST be called otherwise there may be a
     * memory leak. </b>
     * 
     * @param context
     *            the context with the records that will be observed
     * @param allRecordsListener
     *            the observer that will be attached to every (non-system) record in the context //
     * @return the object that will automatically manage registering the allRecordsListener to any
     *         new records in the context.
     */
    public static AllRecordsRegistrationManager addAllRecordsListener(final IObserverContext context,
        final IRecordListener allRecordsListener)
    {
        return new AllRecordsRegistrationManager(allRecordsListener, context);
    }

    /**
     * Copy the field from the source into the target record field. If the source field value is
     * <code>null</code> this does not perform a copy.
     */
    public static void fieldCopy(Map<String, IValue> source, String sourceField, IRecord target, String targetField)
    {
        IValue value = source.get(sourceField);
        if (value != null)
        {
            target.put(targetField, value);
        }
    }

    public static void resolveRecordMapFromStream(Reader reader, Map<String, IValue> map) throws IOException
    {
        String key;
        IValue value;
        int index = 0;
        int c;
        boolean indexFound = false;
        CharBuffer cbuf = CharBuffer.allocate(CharBufferUtils.BLOCK_SIZE);
        while ((c = reader.read()) != -1)
        {
            switch(c)
            {
                case '\r':
                    break;
                case ContextUtils.LINE_SEPARATOR:
                    // we have a line - escaped(key)=escaped(value)
                    key = StringProtocolCodec.decodeKey(cbuf.array(), 0, index, false, new char[index]);
                    value = StringProtocolCodec.decodeValue(cbuf.array(), index + 1, cbuf.position(),
                        new char[cbuf.position() - (index + 1)]);
                    map.put(key, value);
                    cbuf.position(0);
                    indexFound = false;
                    index = 0;
                    break;
                case '=':
                    indexFound = true;
                    //$FALL-THROUGH$
                default :
                    cbuf = CharBufferUtils.put((char) c, cbuf);
                    if (!indexFound)
                    {
                        index++;
                    }
            }
        }
        // need to do the final one
        if (index > 0 && indexFound)
        {
            key = StringProtocolCodec.decodeKey(cbuf.array(), 0, index, false, new char[index]);
            value = StringProtocolCodec.decodeValue(cbuf.array(), index + 1, cbuf.position(),
                new char[cbuf.position() - (index + 1)]);
            map.put(key, value);
        }
    }

    public static void serializeRecordMapToStream(Writer writer, Map<String, IValue> map) throws IOException
    {
        final StringBuilder sb = new StringBuilder();
        Map.Entry<String, IValue> entry = null;
        String key = null;
        IValue value = null;

        final CharArrayReference chars = new CharArrayReference(new char[StringProtocolCodec.CHARRAY_SIZE]);

        for (Iterator<Map.Entry<String, IValue>> it = map.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            key = entry.getKey();
            value = entry.getValue();
            StringProtocolCodec.escape(key, sb, chars);
            sb.append('=');
            StringProtocolCodec.escape(value.toString(), sb, chars);
            sb.append(ContextUtils.LINE_SEPARATOR);
        }
        writer.write(sb.toString());
    }

    /**
     * @see #demergeMaps(Map)
     * @return a map containing all the String-IValue key-pairs in the data map and sub-maps
     */
    static Map<String, IValue> mergeMaps(final Map<String, IValue> dataMap,
        final Map<String, Map<String, IValue>> subMaps)
    {
        Map<String, IValue> flatMap = new HashMap<String, IValue>(dataMap);

        Map.Entry<String, Map<String, IValue>> entry = null;
        String subMapKey = null;
        Map<String, IValue> value = null;
        Map.Entry<String, IValue> entry2 = null;
        String key2 = null;
        IValue value2 = null;
        for (Iterator<Map.Entry<String, Map<String, IValue>>> it = subMaps.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            subMapKey = entry.getKey();
            value = entry.getValue();

            for (Iterator<Map.Entry<String, IValue>> it2 = value.entrySet().iterator(); it2.hasNext();)
            {
                entry2 = it2.next();
                key2 = entry2.getKey();
                value2 = entry2.getValue();
                flatMap.put(SubMap.encodeSubMapKey(subMapKey) + key2, value2);
            }
        }

        return flatMap;
    }

    /**
     * Opposite of {@link #mergeMaps(Map, Map)}
     * 
     * @return first index is a Map&lt;String, IValue>, second index is a Map&lt;String,
     *         Map&lt;String, IValue>> ...
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static Map<?, ?>[] demergeMaps(final Map<String, IValue> mergedMap)
    {
        Map[] result = new Map[] { new HashMap<String, IValue>(), new HashMap<String, Map<String, IValue>>(2) };

        Map.Entry<String, IValue> entry = null;
        String key = null;
        IValue value = null;
        String[] subMapKeys;
        Map<String, IValue> subMap;
        for (Iterator<Map.Entry<String, IValue>> it = mergedMap.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            key = entry.getKey();
            value = entry.getValue();
            subMapKeys = SubMap.decodeSubMapKeys(key);
            if (subMapKeys == null)
            {
                result[0].put(key, value);
            }
            else
            {
                subMap = (Map<String, IValue>) result[1].get(subMapKeys[0]);
                if (subMap == null)
                {
                    subMap = new HashMap(2);
                    result[1].put(subMapKeys[0], subMap);
                }
                subMap.put(subMapKeys[1], value);
            }
        }

        return result;
    }

    /**
     * Does the logic to handle executions of {@link IObserverContext#resubscribe(String...)}
     */
    static void resubscribeRecordsForContext(IObserverContext context,
        final SubscriptionManager<String, IRecordListener> recordSubscribers, Map<String, String> tokenPerRecord,
        String... recordNames)
    {
        Map<String, IRecordListener[]> subscribers = new HashMap<String, IRecordListener[]>(recordNames.length);
        // remove all subscribers from these records
        IRecordListener[] subscribersFor;
        int i = 0;
        for (String recordName : recordNames)
        {
            subscribersFor = recordSubscribers.getSubscribersFor(recordName);
            subscribers.put(recordName, subscribersFor);
            for (i = 0; i < subscribersFor.length; i++)
            {
                context.removeObserver(subscribersFor[i], recordName);
            }
        }
        // now re-subscribe
        String permissionToken = null;
        for (String recordName : recordNames)
        {
            permissionToken = tokenPerRecord.get(recordName);
            permissionToken = permissionToken == null ? IPermissionFilter.DEFAULT_PERMISSION_TOKEN : permissionToken;
            subscribersFor = subscribers.get(recordName);
            for (i = 0; i < subscribersFor.length; i++)
            {
                context.addObserver(permissionToken, subscribersFor[i], recordName);
            }
        }
    }

    /**
     * Get the string representation of the map, expressing {@link BlobValue} instances as their
     * byte sizes rather than the literal translation of the blob's internal byte[]
     */
    public static String mapToString(Map<String, IValue> map)
    {
        if (map.size() > Values.MAX_MAP_FIELDS_TO_PRINT)
        {
            return "{Too big to print, size=" + map.size() + "}";
        }
        StringBuilder sb = new StringBuilder(map.size() * 30);
        sb.append("{");
        boolean first = true;
        Map.Entry<String, IValue> entry = null;
        String key = null;
        IValue value = null;
        for (Iterator<Map.Entry<String, IValue>> it = map.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            key = entry.getKey();
            value = entry.getValue();
            if (first)
            {
                first = false;
            }
            else
            {
                sb.append(",");
            }
            sb.append(key).append("=");
            if (value == null)
            {
                sb.append("null");
            }
            else
            {
                switch(value.getType())
                {
                    case BLOB:
                        sb.append("BlobValue[").append(value.longValue()).append(" bytes]");
                        break;
                    case TEXT:
                        if (value.textValue().length() > 64)
                        {
                            sb.append("TextValue[").append(value.textValue().subSequence(0, 64)).append("...]");
                        }
                        else
                        {
                            sb.append(ObjectUtils.safeToString(value));
                        }
                        break;
                    case DOUBLE:
                    case LONG:
                        sb.append(ObjectUtils.safeToString(value));
                        break;
                }
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Clear all non-system records in the context and publish an update for each.
     */
    public static void clearNonSystemRecords(IPublisherContext context)
    {
        Log.log(context, "Clearing records in ", context.getName());
        final Set<String> recordNames = context.getRecordNames();
        IRecord record;
        for (String recordName : recordNames)
        {
            if (ContextUtils.isSystemRecordName(recordName)
                || ContextUtils.isSystemRecordName(ProxyContext.substituteRemoteNameWithLocalName(recordName))
                || is.eq(recordName, ProxyContext.RECORD_CONNECTION_STATUS_NAME))
            {
                continue;
            }
            record = context.getRecord(recordName);
            record.clear();
            context.publishAtomicChange(record);
        }
    }

    /**
     * @return <code>true</code> if the thread is a core thread
     */
    public static boolean isSystemThread()
    {
        return Arrays.binarySearch(SYSTEM_THREAD_IDS, Thread.currentThread().getId()) > -1;
    }

    /**
     * @return <code>true</code> if the thread is a core thread
     */
    public static boolean isCoreThread()
    {
        return Arrays.binarySearch(CORE_THREAD_IDS, Thread.currentThread().getId()) > -1;
    }

    /**
     * @return <code>true</code> if the thread is a core RPC thread
     */
    public static boolean isRpcThread()
    {
        return Arrays.binarySearch(RPC_THREAD_IDS, Thread.currentThread().getId()) > -1;
    }

    /**
     * @return <code>true</code> if the thread is an internal thread, this covers core, rpc and
     *         system threads
     * @see #isCoreThread()
     * @see #isRpcThread()
     * @see #isSystemThread()
     */
    public static boolean isFrameworkThread()
    {
        return Arrays.binarySearch(FRAMEWORK_THREAD_IDS, Thread.currentThread().getId()) > -1;
    }

    /**
     * Logs when the elapsed time > a threshold
     * 
     * @see Values#SLOW_TASK_THRESHOLD_NANOS
     */
    static void measureTask(String taskName, String action, Object context, long elapsedTimeNanos)
    {
        if (elapsedTimeNanos > Values.SLOW_TASK_THRESHOLD_NANOS)
        {
            Log.log(context, "SLOW TASK: ", action, " [", taskName, "] took ",
                Long.toString(((long) (elapsedTimeNanos * ContextUtils.INVERSE_1000000))), "ms");
        }
    }

    /**
     * A convenience method to get the RPC from the {@link ProxyContext}
     * 
     * @param discoveryTimeoutMillis
     *            the timeout to wait for the RPC to be available
     * @param rpcName
     *            the RPC name
     * @return the RPC
     * @throws TimeOutException
     *             if the RPC is not found in the given time
     */
    public static IRpcInstance getRpc(final ProxyContext proxy, long discoveryTimeoutMillis, final String rpcName)
        throws TimeOutException
    {
        final IRpcInstance rpc = proxy.getRpc(rpcName);
        if (rpc != null)
        {
            return rpc;
        }

        // NOTE: even though the ProxyContext subscribes for the ContextRpcs record on construction,
        // race conditions may mean that the record has not been fully received yet, hence if the
        // record does not have the rpc name we add our own listener (then remove it).
        final AtomicReference<IRpcInstance> rpcRef = new AtomicReference<IRpcInstance>();
        final CountDownLatch latch = new CountDownLatch(1);
        final IRecordListener observer = new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                final IRpcInstance rpc = proxy.getRpc(rpcName);
                if (rpc != null)
                {
                    try
                    {
                        rpcRef.set(rpc);
                    }
                    finally
                    {
                        latch.countDown();
                    }
                }
            }
        };
        try
        {
            proxy.addObserver(observer, IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
            try
            {
                if (!latch.await(discoveryTimeoutMillis, TimeUnit.MILLISECONDS))
                {
                    throw new TimeOutException("No RPC found with name [" + rpcName + "] during discovery period "
                        + discoveryTimeoutMillis + "ms");
                }
            }
            catch (InterruptedException e)
            {
                // we don't care!
            }
        }
        finally
        {
            proxy.removeObserver(observer, IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
        }
        return rpcRef.get();
    }

    /**
     * Utility to remove all records from a context.
     * <p>
     * Note: system records cannot be removed.
     * 
     * @param context
     *            the context to remove records
     */
    public static void removeRecords(Context context)
    {
        Log.log(ContextUtils.class, "Removing all records from ", ObjectUtils.safeToString(context));
        for (String recordName : context.getRecordNames())
        {
            if (!ContextUtils.isSystemRecordName(recordName))
            {
                context.removeRecord(recordName);
            }
        }
    }

    /**
     * Utility to do a clean destroy by removing all records (listeners are notified) then
     * destroying the context.
     * 
     * @param context
     *            the context to remove records from and then destroy
     * @see #removeRecords(Context)
     */
    public static void removeRecordsAndDestroyContext(Context context)
    {
        removeRecords(context);
        context.destroy();
    }

    /**
     * @return <code>true</code> if the name includes a pattern used by the protocol between a
     *         Context and a ProxyContext
     */
    static boolean isProtocolPrefixed(String name)
    {
        if (name.startsWith(PROTOCOL_PREFIX, 0))
        {
            return name.startsWith(ProxyContext.ACK, 0) || name.startsWith(ProxyContext.NOK, 0)
                || name.startsWith(RpcInstance.RPC_RECORD_RESULT_PREFIX, 0);
        }
        return false;
    }

    final static long[] FRAMEWORK_THREAD_IDS;
    final static long[] CORE_THREAD_IDS;
    final static long[] SYSTEM_THREAD_IDS;
    final static long[] RPC_THREAD_IDS;
    static
    {
        long[] coreThreadIds = new long[0];
        long[] rpcThreadIds = new long[0];
        long[] systemThreadIds = new long[0];
        long[] frameworkThreadIds = new long[0];
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final long[] allThreadIds = threadMXBean.getAllThreadIds();
        for (int i = 0; i < allThreadIds.length; i++)
        {
            long threadId = allThreadIds[i];
            final ThreadInfo threadInfo = threadMXBean.getThreadInfo(threadId);
            final String name = threadInfo.getThreadName();
            if (name.startsWith(FISSION_CORE, 0))
            {
                coreThreadIds = ArrayUtils.add(threadId, coreThreadIds);
                frameworkThreadIds = ArrayUtils.add(threadId, frameworkThreadIds);
            }
            else if (name.startsWith(FISSION_RPC, 0))
            {
                rpcThreadIds = ArrayUtils.add(threadId, rpcThreadIds);
                frameworkThreadIds = ArrayUtils.add(threadId, frameworkThreadIds);
            }
            else if (name.startsWith(FISSION_SYSTEM, 0))
            {
                systemThreadIds = ArrayUtils.add(threadId, systemThreadIds);
                frameworkThreadIds = ArrayUtils.add(threadId, frameworkThreadIds);
            }
        }
        Arrays.sort(coreThreadIds);
        Arrays.sort(rpcThreadIds);
        Arrays.sort(systemThreadIds);
        Arrays.sort(frameworkThreadIds);
        CORE_THREAD_IDS = coreThreadIds;
        RPC_THREAD_IDS = rpcThreadIds;
        SYSTEM_THREAD_IDS = systemThreadIds;
        FRAMEWORK_THREAD_IDS = frameworkThreadIds;
    }
}