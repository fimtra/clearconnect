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
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.CharBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.channel.ChannelUtils;
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
import com.fimtra.executors.ContextExecutorFactory;
import com.fimtra.executors.IContextExecutor;
import com.fimtra.executors.ITaskStatistics;
import com.fimtra.util.CharBufferUtils;
import com.fimtra.util.FastDateFormat;
import com.fimtra.util.FileUtils;
import com.fimtra.util.FileUtils.ExtensionFileFilter;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.Pair;
import com.fimtra.util.RollingFileAppender;
import com.fimtra.util.StringAppender;
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
public final class ContextUtils {

    /**
     * This listener is attached to the {@link ISystemRecordNames#CONTEXT_RECORDS} and will register an inner
     * listener to any new records in the context.
     * <p>
     * To prevent a memory leak, the {@link #destroy()} <b>MUST</b> be called when application code no longer
     * requires the manager.
     *
     * @author Ramon Servadei
     */
    public static final class AllRecordsRegistrationManager implements IRecordListener {
        final IRecordListener allRecordsListener;
        final IObserverContext context;
        final Set<String> subscribed = new HashSet<>();

        AllRecordsRegistrationManager(IRecordListener allRecordsListener, IObserverContext context)
        {
            this.allRecordsListener = allRecordsListener;
            this.context = context;
            context.addObserver(this, ISystemRecordNames.CONTEXT_RECORDS);
        }

        public void destroy()
        {
            this.context.removeObserver(this, ISystemRecordNames.CONTEXT_RECORDS);
            List<String> temp = new LinkedList<>(this.subscribed);
            if (temp.size() > 0)
            {
                this.context.removeObserver(this.allRecordsListener, temp.toArray(new String[temp.size()]));
            }
            this.subscribed.clear();
        }

        @Override
        public void onChange(IRecord imageCopy, IRecordChange atomicChange)
        {
            List<String> temp = new LinkedList<>();
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

            temp = new LinkedList<>();
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

    static final double INVERSE_1000000 = 1d / 1000000;

    public static final Set<String> SYSTEM_RECORDS;

    static
    {
        Set<String> set = new HashSet<>();
        set.add(ISystemRecordNames.CONTEXT_RPCS);
        set.add(ISystemRecordNames.CONTEXT_STATUS);
        set.add(ISystemRecordNames.CONTEXT_RECORDS);
        set.add(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        set.add(ISystemRecordNames.CONTEXT_CONNECTIONS);
        SYSTEM_RECORDS = Collections.unmodifiableSet(set);
    }

    /**
     * This is the default shared {@link IContextExecutor} used for <b>system record</b> event handling.
     *
     * @see #SYSTEM_RECORDS
     */
    final static IContextExecutor SYSTEM_RECORD_EXECUTOR =
            ContextExecutorFactory.create(FISSION_SYSTEM, SYSTEM_RECORDS.size());

    /**
     * This is the default shared {@link IContextExecutor} that can be used by all contexts.
     */
    final static IContextExecutor CORE_EXECUTOR =
            ContextExecutorFactory.create(FISSION_CORE, DataFissionProperties.Values.CORE_THREAD_COUNT);

    /**
     * This is dedicated to handle RPC results. If RPC results are handled by the {@link #CORE_EXECUTOR}, a
     * timeout could occur if the result is placed onto the same queue of the thread that is waiting for the
     * result!
     */
    final static IContextExecutor RPC_EXECUTOR =
            ContextExecutorFactory.create(FISSION_RPC, DataFissionProperties.Values.RPC_THREAD_COUNT);

    final static RollingFileAppender qStatsLog = DataFissionProperties.Values.ENABLE_Q_STATS_LOGGING ?
            RollingFileAppender.createStandardRollingFileAppender("Qstats", UtilProperties.Values.LOG_DIR) :
            null;

    static final RollingFileAppender runtimeStatsLog =
            RollingFileAppender.createStandardRollingFileAppender("runtimeStats",
                    UtilProperties.Values.LOG_DIR);

    static long gcDutyCycle;

    public static long getGcDutyCycle()
    {
        return gcDutyCycle;
    }

    static final AtomicReference<long[]> coreStats = new AtomicReference<>(new long[] { 0, 0, 0 });

    static
    {
        ThreadUtils.scheduleWithFixedDelay(new Runnable() {
            // todo this needs to be sent to the registry in a new system-level record
            final FastDateFormat fdf = new FastDateFormat();
            long gcTimeLastPeriod;

            {
                final StringBuilder sb = new StringBuilder(1024);
                sb.append(
                        "Time, Memory use (Mb), GC duty, TX connections, TX Q, Max TX Q, Max TX Q connection, Event Qs overflow, Event Qs submitted, Msgs sent, Bytes sent, Msgs rcvd, Bytes rcvd").append(
                        SystemUtils.lineSeparator());
                try
                {
                    runtimeStatsLog.append(sb);
                    runtimeStatsLog.flush();
                }
                catch (Exception e)
                {
                    Log.log(ContextUtils.class, "Could not add header to runtime stats log", e);
                }
            }

            @Override
            public void run()
            {
                final Set<IContextExecutor> executors = ContextExecutorFactory.getExecutors();
                final StringBuilder sb = new StringBuilder(1024);
                final String yyyyMMddHHmmssSSS = this.fdf.yyyyMMddHHmmssSSS(System.currentTimeMillis());

                // QStats first
                if (qStatsLog != null)
                {
                    sb.append(yyyyMMddHHmmssSSS);
                }
                // context executor Qs
                long qOverflow = 0, qSubmitted = 0, qExecuted = 0;
                ITaskStatistics stats;
                long overflow;
                for (IContextExecutor executor : executors)
                {
                    final Map<Object, ITaskStatistics> coalescingTaskStatistics =
                            executor.getCoalescingTaskStatistics();
                    stats = coalescingTaskStatistics.get(IContextExecutor.QUEUE_LEVEL_STATS);
                    // we do not calculate overflow for coalescing and ignore coalesced counts
                    // otherwise we get skewed stats (ie. looks like a slow consumer)
                    qSubmitted += stats.getIntervalExecuted();
                    qExecuted += stats.getIntervalExecuted();

                    final Map<Object, ITaskStatistics> sequentialTaskStatistics =
                            executor.getSequentialTaskStatistics();
                    stats = sequentialTaskStatistics.get(IContextExecutor.QUEUE_LEVEL_STATS);
                    overflow = stats.getIntervalSubmitted() - stats.getIntervalExecuted();
                    qOverflow += (overflow < 0 ? 0 : overflow);
                    qSubmitted += stats.getIntervalSubmitted();
                    qExecuted += stats.getIntervalExecuted();

                    if (qStatsLog != null)
                    {
                        sb.append(", ").append(executor.getName()).append(" coalescing Q, ").append(
                                getStats(coalescingTaskStatistics));
                        sb.append(", ").append(executor.getName()).append(" sequential Q, ").append(
                                getStats(sequentialTaskStatistics));
                    }
                }

                coreStats.set(new long[] { qOverflow, qSubmitted, qExecuted });

                if (qStatsLog != null)
                {
                    try
                    {
                        qStatsLog.append(sb.toString()).append(SystemUtils.lineSeparator());
                        qStatsLog.flush();
                    }
                    catch (IOException e)
                    {
                        Log.log(ContextUtils.class, "Could not log to QStats file", e);
                    }
                }

                // now Summary stats
                sb.setLength(0);

                // time
                sb.append(yyyyMMddHHmmssSSS);

                // memory use
                final Runtime runtime = Runtime.getRuntime();
                final double MB = 1d / (1024 * 1024);
                final long memUsed = (long) ((runtime.totalMemory() - runtime.freeMemory()) * MB);
                sb.append(", ").append(memUsed);

                // compute the GC duty
                long gcMillisInPeriod = 0;
                long time;
                for (GarbageCollectorMXBean gcMxBean : ManagementFactory.getGarbageCollectorMXBeans())
                {
                    time = gcMxBean.getCollectionTime();
                    if (time > -1)
                    {
                        gcMillisInPeriod += time;
                    }
                }
                // store and work out delta of gc times
                time = this.gcTimeLastPeriod;
                this.gcTimeLastPeriod = gcMillisInPeriod;
                gcMillisInPeriod -= time;
                final double inverseLoggingPeriodSecs =
                        1d / DataFissionProperties.Values.STATS_LOGGING_PERIOD_SECS;
                // this is now the "% GC duty cycle per minute"
                gcDutyCycle = (long) (gcMillisInPeriod * inverseLoggingPeriodSecs * 0.1d);

                sb.append(", ").append(getGcDutyCycle());

                // TX Q and max TX Q connection
                final List<Pair<Integer, String>> channelStats = ChannelUtils.WATCHDOG.getChannelStats();
                int txQsize = 0;
                int maxTxQ = 0;
                String maxTxQName = "";
                int size;
                for (Pair<Integer, String> stat : channelStats)
                {
                    size = stat.getFirst().intValue();
                    txQsize += size;
                    if (maxTxQ < size)
                    {
                        maxTxQName = stat.getSecond();
                        maxTxQ = size;
                    }
                }
                sb.append(", ").append(channelStats.size()).append(", ").append(txQsize).append(", ").append(
                        maxTxQ).append(", ").append(maxTxQName).append(", ").append(qOverflow).append(
                        ", ").append(qSubmitted).append(", ").append(
                        Publisher.MESSAGES_PUBLISHED.getAndSet(0)).append(", ").append(
                        Publisher.BYTES_PUBLISHED.getAndSet(0)).append(", ").append(
                        ProxyContext.MESSAGES_RECEIVED.getAndSet(0)).append(", ").append(
                        ProxyContext.BYTES_RECEIVED.getAndSet(0));

                try
                {
                    runtimeStatsLog.append(sb.toString()).append(SystemUtils.lineSeparator());
                    runtimeStatsLog.flush();
                }
                catch (IOException e)
                {
                    Log.log(ContextUtils.class, "Could not log to runStats file", e);
                }
            }

            final String getStats(Map<Object, ITaskStatistics> taskStatisticsMap)
            {
                final ITaskStatistics stats = taskStatisticsMap.get(IContextExecutor.QUEUE_LEVEL_STATS);
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
    static final char PROTOCOL_PREFIX = '_';

    /**
     * @return a long[] for the sequential and coalescing tasks statistics, format {queue-overflow,
     * queue-total-submitted, queue-total-executed}
     */
    public static long[] getCoreStats()
    {
        return coreStats.get();
    }

    /**
     * Serialise the state of a context to the directory. Each record in the context is serialized into a
     * distinct file in the directory called {record-name}.record. This does not serialize system records.
     * <p>
     * This will create a backup of the current directory at the same level called {directory-name}-backup,
     * then write the context records to a temp directory at the same level called {directory-name}-temp, then
     * rename the temp directory to the directory argument.
     * <p>
     * <b>This is a non-atomic operation.</b>
     *
     * @param context   the context to serialise
     * @param directory the directory for the data files for each record in the context
     * @see #serializeRecordToFile(IRecord, File)
     */
    public static void serializeContextToDirectory(IPublisherContext context, File directory)
            throws IOException
    {
        final File backupDir = new File(directory.getParent(), directory.getName() + "-backup");
        FileUtils.clearDirectory(backupDir);
        FileUtils.copyRecursive(directory, backupDir);

        final File tmpDir =
                FileUtils.createDir(new File(directory.getParent(), directory.getName() + "-temp"));
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
     * Resolve a context's internal records by loading them from data files in directory. This does not
     * resolve system records.
     * <p>
     * <b>NOTE:</b> records that currently exist in the context will have their state merged with
     * the data held in the record file that is loaded.
     *
     * @param context   the context to resolve
     * @param directory the directory for the data files for each record in the context
     * @see #resolveRecordFromFile(IRecord, File)
     */
    public static void resolveContextFromDirectory(IPublisherContext context, File directory)
            throws IOException
    {
        File[] recordFiles = FileUtils.readFiles(directory, RECORD_FILE_FILTER);
        String recordName;
        IRecord record;
        for (File recordFile : recordFiles)
        {
            recordName = recordFile.getName().substring(0,
                    recordFile.getName().length() - RECORD_FILE_EXTENSION.length());
            record = context.getOrCreateRecord(recordName);
            resolveRecordFromFile(record, directory);
        }
    }

    /**
     * Convenience method to serialise a record to the directory. The record contents are serialised to a flat
     * file {record-name}.record in the directory.
     *
     * @param record    the record to serialise
     * @param directory the directory for the data file
     */
    public static void serializeRecordToFile(IRecord record, File directory) throws IOException
    {
        final File f = new File(directory, record.getName() + RECORD_FILE_EXTENSION + ".tmp");
        if (f.exists() || f.createNewFile())
        {
            try (Writer writer = new BufferedWriter(new FileWriter(f)))
            {
                record.serializeToStream(writer);
                writer.flush();
            }
            FileUtils.move(f, new File(directory, record.getName() + RECORD_FILE_EXTENSION));
        }
        else
        {
            Log.log(ContextUtils.class, "Could not create data file for record ", record.getName());
        }
    }

    /**
     * Convenience method to resolve a record's internal data from a data file in directory. The record
     * contents are serialised in a flat file {record-name}.record in the directory.
     *
     * @param record    the record to resolve
     * @param directory the directory for the data file
     */
    public static void resolveRecordFromFile(IRecord record, File directory) throws IOException
    {
        File f = new File(directory, record.getName() + RECORD_FILE_EXTENSION);
        if (f.exists())
        {
            try (Reader reader = new BufferedReader(new FileReader(f)))
            {
                record.resolveFromStream(reader);
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
     * @param recordName the name of the record to be deleted
     * @param directory  the directory for the data file
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
     * Convenience method to get the record name from a record file. This expects the file name to be in the
     * format: {record name}.record
     *
     * @param recordFile the record file
     * @return the record name of the file, <code>null</code> if not a record file
     */
    public static String getRecordNameFromFile(File recordFile)
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
     * CONTEXT_XXX record names)
     * @see ISystemRecordNames#CONTEXT_RPCS
     * @see ISystemRecordNames#CONTEXT_STATUS
     * @see ISystemRecordNames#CONTEXT_RECORDS
     * @see ISystemRecordNames#CONTEXT_CONNECTIONS
     * @see ISystemRecordNames#CONTEXT_SUBSCRIPTIONS
     */
    public static boolean isSystemRecordName(String name)
    {
        if (name != null && name.length() > 7 && name.charAt(0) == 'C' && name.charAt(6) == 't')
        {
            return SYSTEM_RECORDS.contains(name);
        }
        return false;
    }

    /**
     * A utility to register a listener that will be registered against all (non-system) records in a context.
     * This creates an adapter listener that is registered to the context's {@link
     * ISystemRecordNames#CONTEXT_RECORDS} record; when new records are added the allRecordsListener is added
     * as an observer to the new record. <b>When finished with this, the {@link
     * AllRecordsRegistrationManager#destroy} method MUST be called otherwise there may be a memory leak.
     * </b>
     *
     * @param context            the context with the records that will be observed
     * @param allRecordsListener the observer that will be attached to every (non-system) record in the
     *                           context //
     * @return the object that will automatically manage registering the allRecordsListener to any new records
     * in the context.
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
    public static void fieldCopy(Map<String, IValue> source, String sourceField, IRecord target,
            String targetField)
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
                default:
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
        final StringAppender sb = new StringAppender();
        String key;
        IValue value;

        final CharArrayReference chars = new CharArrayReference(new char[StringProtocolCodec.CHARRAY_SIZE]);

        final char[] escapedChars = new char[2];
        escapedChars[0] = StringProtocolCodec.CHAR_ESCAPE;

        for (Map.Entry<String, IValue> entry : map.entrySet())
        {
            key = entry.getKey();
            value = entry.getValue();
            StringProtocolCodec.escape(key, sb, chars, escapedChars);
            sb.append('=');
            StringProtocolCodec.escape(value.toString(), sb, chars, escapedChars);
            sb.append(ContextUtils.LINE_SEPARATOR);
        }
        writer.write(sb.toString());
    }

    /**
     * @return a map containing all the String-IValue key-pairs in the data map and sub-maps
     * @see #demergeMaps(Map)
     */
    static Map<String, IValue> mergeMaps(final Map<String, IValue> dataMap,
            final Map<String, Map<String, IValue>> subMaps)
    {
        Map<String, IValue> flatMap = new HashMap<>(dataMap);

        String subMapKey;
        Map<String, IValue> value;
        String key2;
        IValue value2;
        for (Map.Entry<String, Map<String, IValue>> entry : subMaps.entrySet())
        {
            subMapKey = entry.getKey();
            value = entry.getValue();

            for (Map.Entry<String, IValue> entry2 : value.entrySet())
            {
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
     * @return first index is a Map&lt;String, IValue>, second index is a Map&lt;String, Map&lt;String,
     * IValue>> ...
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static Map<?, ?>[] demergeMaps(final Map<String, IValue> mergedMap)
    {
        Map[] result = new Map[] { new HashMap<>(), new HashMap<>(2) };

        String key;
        IValue value;
        String[] subMapKeys;
        Map<String, IValue> subMap;
        for (Map.Entry<String, IValue> entry : mergedMap.entrySet())
        {
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
            final SubscriptionManager<String, IRecordListener> recordSubscribers,
            Map<String, String> tokenPerRecord, String... recordNames)
    {
        Map<String, IRecordListener[]> subscribers = new HashMap<>(recordNames.length);
        // remove all subscribers from these records
        IRecordListener[] subscribersFor;
        int i;
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
        String permissionToken;
        for (String recordName : recordNames)
        {
            permissionToken = tokenPerRecord.get(recordName);
            permissionToken =
                    permissionToken == null ? IPermissionFilter.DEFAULT_PERMISSION_TOKEN : permissionToken;
            subscribersFor = subscribers.get(recordName);
            for (i = 0; i < subscribersFor.length; i++)
            {
                context.addObserver(permissionToken, subscribersFor[i], recordName);
            }
        }
    }

    /**
     * Get the string representation of the map, expressing {@link BlobValue} instances as their byte sizes
     * rather than the literal translation of the blob's internal byte[]
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
        String key;
        IValue value;
        for (Map.Entry<String, IValue> entry : map.entrySet())
        {
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
                            sb.append("TextValue[").append(value.textValue().subSequence(0, 64)).append(
                                    "...]");
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
            if (ContextUtils.isSystemRecordName(recordName) || ContextUtils.isSystemRecordName(
                    ProxyContext.substituteRemoteNameWithLocalName(recordName)) || is.eq(recordName,
                    ProxyContext.RECORD_CONNECTION_STATUS_NAME))
            {
                continue;
            }
            record = context.getRecord(recordName);
            if (record != null)
            {
                record.clear();
                context.publishAtomicChange(record);
            }
        }
    }

    /**
     * @return <code>true</code> if the thread is a system record context thread
     */
    public static boolean isSystemThread()
    {
        return SYSTEM_RECORD_EXECUTOR.isExecutorThread(Thread.currentThread().getId());
    }

    /**
     * @return <code>true</code> if the thread is a core thread
     */
    public static boolean isCoreThread()
    {
        return CORE_EXECUTOR.isExecutorThread(Thread.currentThread().getId());
    }

    /**
     * @return <code>true</code> if the thread is a core RPC thread
     */
    public static boolean isRpcThread()
    {
        return RPC_EXECUTOR.isExecutorThread(Thread.currentThread().getId());
    }

    /**
     * @return <code>true</code> if the thread is an internal thread, this covers core, rpc and
     * system threads
     * @see #isCoreThread()
     * @see #isRpcThread()
     * @see #isSystemThread()
     */
    public static boolean isFrameworkThread()
    {
        final long id = Thread.currentThread().getId();
        final boolean isCoreThread = CORE_EXECUTOR.isExecutorThread(id);
        if (CORE_EXECUTOR == RPC_EXECUTOR)
        {
            return isCoreThread;
        }
        return isCoreThread || RPC_EXECUTOR.isExecutorThread(id) || SYSTEM_RECORD_EXECUTOR.isExecutorThread(
                id);
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
     * @param discoveryTimeoutMillis the timeout to wait for the RPC to be available
     * @param rpcName                the RPC name
     * @return the RPC
     * @throws TimeOutException if the RPC is not found in the given time
     */
    public static IRpcInstance getRpc(final ProxyContext proxy, long discoveryTimeoutMillis,
            final String rpcName) throws TimeOutException
    {
        final IRpcInstance rpc = proxy.getRpc(rpcName);
        if (rpc != null)
        {
            return rpc;
        }

        // NOTE: even though the ProxyContext subscribes for the ContextRpcs record on construction,
        // race conditions may mean that the record has not been fully received yet, hence if the
        // record does not have the rpc name we add our own listener (then remove it).
        final AtomicReference<IRpcInstance> rpcRef = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final IRecordListener observer = (imageCopy, atomicChange) -> {
            final IRpcInstance rpc1 = proxy.getRpc(rpcName);
            if (rpc1 != null)
            {
                try
                {
                    rpcRef.set(rpc1);
                }
                finally
                {
                    latch.countDown();
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
                    throw new TimeOutException(
                            "No RPC found with name [" + rpcName + "] during discovery period "
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
     * @param context the context to remove records
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
     * Utility to do a clean destroy by removing all records (listeners are notified) then destroying the
     * context.
     *
     * @param context the context to remove records from and then destroy
     * @see #removeRecords(Context)
     */
    public static void removeRecordsAndDestroyContext(Context context)
    {
        removeRecords(context);
        context.destroy();
    }

    /**
     * @return <code>true</code> if the name includes a pattern used by the protocol between a
     * Context and a ProxyContext
     */
    static boolean isProtocolPrefixed(String name)
    {
        if (name.charAt(0) == PROTOCOL_PREFIX)
        {
            return name.startsWith(ProxyContext.ACK, 0) || name.startsWith(ProxyContext.NOK, 0)
                    || name.startsWith(RpcInstance.RPC_RECORD_RESULT_PREFIX, 0);
        }
        return false;
    }
}