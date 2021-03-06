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
package com.fimtra.datafission.core;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IPermissionFilter;
import com.fimtra.datafission.IPublisherContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IValidator;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.executors.ICoalescingRunnable;
import com.fimtra.executors.IContextExecutor;
import com.fimtra.executors.ISequentialRunnable;
import com.fimtra.util.CollectionUtils;
import com.fimtra.util.DeadlockDetector;
import com.fimtra.util.DeadlockDetector.ThreadInfoWrapper;
import com.fimtra.util.FileUtils;
import com.fimtra.util.LazyObject;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SubscriptionManager;
import com.fimtra.util.SystemUtils;
import com.fimtra.util.ThreadUtils;
import com.fimtra.util.UtilProperties;

/**
 * A context is the home for a group of records. The definition of the context is application specific.
 * <p>
 * A context can publish changes to its records to one or more {@link IRecordListener} objects in the local
 * runtime.
 * <p>
 * To publish changes to remote observers, a {@link Publisher} must be created and attached to the context.
 * The publisher can then publish changes to one or more {@link ProxyContext} instances.
 * <p>
 * Operations that mutate any record are performed using the {@link IRecord#getWriteLock()} associated with
 * the name of the record. This allows operations on different records to run in parallel.
 *
 * @author Ramon Servadei
 * @see IRecord
 */
public final class Context implements IPublisherContext
{
    /**
     * Controls logging of:
     * <ul>
     * <li>Created/removed records
     * <li>subscriber changes
     * <li>add/remove listener
     * <li>notify initial image
     * </ul>
     */
    public static boolean log = SystemUtils.getProperty("log." + Context.class.getCanonicalName(), false);

    static final String GET_REMOTE_RECORD_RPC = "getRemoteRecord";

    static final AtomicInteger eventCount = new AtomicInteger();

    static
    {
        if (DataFissionProperties.Values.ENABLE_THREAD_DEADLOCK_CHECK)
        {
            DeadlockDetector.newDeadlockDetectorTask(
                    DataFissionProperties.Values.THREAD_DEADLOCK_CHECK_PERIOD_MILLIS, deadlocks -> {
                        StringBuilder sb = new StringBuilder();
                        sb.append("DEADLOCKED THREADS FOUND!").append(SystemUtils.lineSeparator());
                        for (ThreadInfoWrapper deadlock : deadlocks)
                        {
                            sb.append(deadlock.toString());
                        }
                        System.err.println(sb.toString());
                    }, UtilProperties.Values.USE_ROLLING_THREADDUMP_FILE);
        }

        // resolves ClassNotFoundException on shutdown
        final DeadlockDetector deadlockDetector = new DeadlockDetector();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Log.log(Context.class, "JVM shutting down...");
            final String filePrefix = ThreadUtils.getMainMethodClassSimpleName() + "-threadDumpOnExit";
            final File threadDumpOnShutdownFile =
                    FileUtils.createLogFile_yyyyMMddHHmmss(UtilProperties.Values.LOG_DIR, filePrefix);
            final ThreadInfoWrapper[] threads = deadlockDetector.getThreadInfoWrappers();
            if (threads != null)
            {
                try (PrintWriter pw = new PrintWriter(threadDumpOnShutdownFile))
                {
                    for (ThreadInfoWrapper thread : threads)
                    {
                        pw.print(thread.toString());
                        pw.flush();
                    }
                    Log.log(Context.class, "Thread dump successful: ", threadDumpOnShutdownFile.toString());
                }
                catch (Exception e)
                {
                    Log.log(Context.class, "Could not produce thread dump file on exit", e);
                }
            }
        }, "datafission-shutdown"));
    }

    /**
     * Bypass any checks and get a writable record - <b>only used for internals</b>
     */
    static IRecord getRecordInternal(IObserverContext context, String name)
    {
        return ((Context) context).records.get(name);
    }

    /** Holds all records in this context */
    final ConcurrentMap<String, Record> records;

    /**
     * Maintains a map of {@link Record} images and {@link ImmutableRecord} instances backed by the images.
     * Changes are applied to the images which can be viewed by the immutable instances.
     *
     * @author Ramon Servadei
     */
    final static class ImageCache
    {
        /**
         * The image of each record that is updated with atomic changes and used to construct an {@link
         * ImmutableRecord} to pass in to {@link IRecordListener#onChange(IRecord, IRecordChange)}
         * <p>
         * <b>NOTE: the images are only ever updated with atomic changes in the
         * {@link ISequentialRunnable} that notifies the {@link IRecordListener} instances. This ensures that
         * the listener instances only see an image that reflects the atomic change that caused the listener
         * to be notified.</b>
         */
        final ConcurrentMap<String, Record> images;

        final ConcurrentMap<String, ImmutableRecord> immutableImages;

        ImageCache(int size)
        {
            this.images = new ConcurrentHashMap<>(size);
            this.immutableImages = new ConcurrentHashMap<>(size);
        }

        void put(String recordName, Record record)
        {
            this.images.put(recordName, record);
            this.immutableImages.put(recordName, new ImmutableRecord(record));
        }

        IRecord remove(String name)
        {
            this.immutableImages.remove(name);
            return this.images.remove(name);
        }

        Set<String> keySet()
        {
            return this.images.keySet();
        }

        IRecord updateInstance(String name, IRecordChange change)
        {
            final Record record = this.images.get(name);
            if (record != null)
            {
                record.applyChangeAndSetSequence(change);
            }
            // the Record in the images map backs the ImmutableRecord in the immutableImages map
            return this.immutableImages.get(name);
        }

        /**
         * @return an {@link ImmutableRecord} for the record name, <code>null</code> if the record does not
         * exist
         */
        ImmutableRecord getImmutableInstance(String name)
        {
            return this.immutableImages.get(name);
        }
    }

    /**
     * Manages the images of the context's records.
     * <p>
     * <b>NOTE: the images are only ever updated with atomic changes in the
     * {@link ISequentialRunnable} that notifies the {@link IRecordListener} instances. This ensures that the
     * listener instances only see an image that reflects the atomic change that caused the listener to be
     * notified.</b>
     */
    final ImageCache imageCache;

    /** Tracks the observers for the records in this context */
    final SubscriptionManager<String, IRecordListener> recordObservers;
    /** cheap read-write lock semantics */
    volatile Map<String, IRpcInstance> rpcInstances;
    final Set<IValidator> validators;
    volatile boolean active;
    final String name;
    final IContextExecutor rpcExecutor;
    final IContextExecutor coreExecutor;
    final IContextExecutor systemExecutor;
    final LazyObject<ScheduledExecutorService> utilityExecutor;
    final Object recordCreateLock;
    /**
     * Tracks what records have been deleted and need to be removed from system records - this is an
     * efficiency optimisation for bulk remove operations
     */
    final Set<String> recordsToRemoveFromSystemRecords;
    final String recordsToRemoveContext;
    IPermissionFilter permissionFilter;
    /** The permission token used for subscribing each record */
    final Map<String, String> tokenPerRecord;

    /**
     * This is populated when a listener is registered and prevents duplicate updates being sent to the
     * listener during its phase of receiving initial images whilst any concurrent updates may also be
     * occurring.
     */
    final Map<IRecordListener, Set<String>> listenersToNotifyWithInitialImages;
    volatile int listenersBeingNotifiedWithInitialImages;

    final ContextThrottle throttle;

    /** Construct the context with the given name */
    public Context(String name)
    {
        this(name, null, null, null);
    }

    /**
     * Construct the context
     *
     * @param name            the name of the context
     * @param eventExecutor   the executor for handling events, if <code>null</code> the default event
     *                        executor is used
     * @param rpcExecutor     the executor for handling RPCs, if <code>null</code> the default RPC executor is
     *                        used
     * @param utilityExecutor the utility {@link ScheduledExecutorService}, if <code>null</code> the default
     *                        utility executor is used
     */
    public Context(String name, IContextExecutor eventExecutor, IContextExecutor rpcExecutor,
            ScheduledExecutorService utilityExecutor)
    {
        super();

        this.name = name;
        this.throttle = new ContextThrottle(DataFissionProperties.Values.PENDING_EVENT_THROTTLE_THRESHOLD,
                eventCount);
        this.rpcExecutor = rpcExecutor == null ? ContextUtils.RPC_EXECUTOR : rpcExecutor;
        this.coreExecutor = eventExecutor == null ? ContextUtils.CORE_EXECUTOR : eventExecutor;
        this.systemExecutor = ContextUtils.SYSTEM_RECORD_EXECUTOR;
        this.utilityExecutor = utilityExecutor == null ?
                new LazyObject<>(() -> ThreadUtils.newScheduledExecutorService(name + "-utility", 1),
                        ExecutorService::shutdown) : new LazyObject<>(() -> utilityExecutor, (s) -> {
        });
        this.recordCreateLock = new Object();
        this.recordObservers = new SubscriptionManager<>(IRecordListener.class);
        this.recordsToRemoveFromSystemRecords = new HashSet<>();
        this.recordsToRemoveContext = "recordsToRemoveFromSystemRecords:" + name
                // this MUST be added to ensure complete uniqueness for coalescing in case more than one
                // Context share the same name!
                + "-" + UUID.randomUUID();

        final int initialSize = 1024;
        this.imageCache = new ImageCache(initialSize);
        this.records = new ConcurrentHashMap<>(initialSize);
        this.tokenPerRecord = new ConcurrentHashMap<>(initialSize);
        this.rpcInstances = new HashMap<>();
        this.validators = new CopyOnWriteArraySet<>();

        this.listenersToNotifyWithInitialImages = Collections.synchronizedMap(new HashMap<>());

        // create the special records by hand
        createSystemRecord(ISystemRecordNames.CONTEXT_RECORDS);
        createSystemRecord(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        createSystemRecord(ISystemRecordNames.CONTEXT_STATUS);
        createSystemRecord(ISystemRecordNames.CONTEXT_RPCS);
        createSystemRecord(ISystemRecordNames.CONTEXT_CONNECTIONS);

        this.active = true;

        // create AFTER setting active
        createGetRemoteRecordImageAsMapRpc();
    }

    private void createGetRemoteRecordImageAsMapRpc()
    {
        final RpcInstance rpc =
                new RpcInstance(IValue.TypeEnum.TEXT, GET_REMOTE_RECORD_RPC, IValue.TypeEnum.TEXT);
        rpc.setHandler(args -> {
            IRecord record = getRecord(args[0].textValue());
            if (record != null)
            {
                try
                {
                    final StringWriter sw = new StringWriter();
                    ContextUtils.serializeRecordMapToStream(sw, record.asFlattenedMap());
                    return TextValue.valueOf(sw.toString());
                }
                catch (IOException e)
                {
                    throw new IRpcInstance.ExecutionException(e);
                }
            }
            return null;
        });
        createRpc(rpc);
    }

    private void createSystemRecord(String recordName)
    {
        this.imageCache.put(recordName, new Record(recordName, ContextUtils.EMPTY_MAP, this));
        final Record record = new Record(recordName, ContextUtils.EMPTY_MAP, this);
        this.records.put(recordName, record);

        // add to the context record
        this.records.get(ISystemRecordNames.CONTEXT_RECORDS).put(recordName, LongValue.valueOf(0));
    }

    @Override
    public void destroy()
    {
        try
        {
            this.utilityExecutor.destroy();
            if (ContextUtils.CORE_EXECUTOR != this.coreExecutor)
            {
                this.coreExecutor.destroy();
            }
            if (ContextUtils.RPC_EXECUTOR != this.rpcExecutor)
            {
                this.rpcExecutor.destroy();
            }
            this.records.clear();
            this.recordObservers.destroy();
            this.active = false;
        }
        catch (Exception e)
        {
            Log.log(this, "Could not destroy context " + this.name, e);
        }
    }

    @Override
    public boolean isActive()
    {
        return this.active;
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public String toString()
    {
        return "Context [" + this.name + " records=" + this.records.size() + "]";
    }

    @Override
    public IRecord createRecord(String name)
    {
        return createRecord(name, ContextUtils.EMPTY_MAP);
    }

    @Override
    public IRecord createRecord(final String name, Map<String, IValue> initialData)
    {
        if (ContextUtils.isSystemRecordName(name) || ContextUtils.isProtocolPrefixed(name)
                || AtomicChangeTeleporter.startsWithFragmentPrefix(name))
        {
            throw new IllegalArgumentException("The name [" + name + "] contains illegal characters");
        }

        // publish the update to the ContextRecords before publishing to observers - one of the
        // observers may check the ContextRecords so the created record MUST be in there before
        final IRecord contextRecords = this.records.get(ISystemRecordNames.CONTEXT_RECORDS);
        if (!isSystemRecordReady(contextRecords))
        {
            throw new IllegalStateException(
                    "Cannot create new record [" + name + "] in shutdown context " + ObjectUtils.safeToString(
                            this));
        }

        final Record record;
        synchronized (this.recordCreateLock)
        {
            record = createRecordInternal_callWithLock(name, initialData);
        }

        synchronized (contextRecords.getWriteLock())
        {
            // important to access subscriptions whilst holding the write lock of CONTEXT_RECORDS -
            // see addDeltaToSubscriptionCount - this ensures we create the record with the correct
            // subscriptions count
            final IRecord contextSubscriptions = this.records.get(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
            final IValue subscriptionCount = contextSubscriptions.get(name);
            contextRecords.put(name,
                    LongValue.valueOf(subscriptionCount == null ? 0 : subscriptionCount.longValue()));
            publishAtomicChange(ISystemRecordNames.CONTEXT_RECORDS);
        }

        // always force a publish for the initial create - guaranteed to be sequence 0
        publishAtomicChange(name, true);

        return record;
    }

    /**
     * Create a blank record with this name - no listeners are notified of the record's existence in this
     * method.
     */
    IRecord createRecordSilently_callInRecordContext(final String name)
    {
        synchronized (this.recordCreateLock)
        {
            /*
             * This is only called when receiving a non-empty remote record for the first time. We
             * need to insert a blank image because the sequence will not be 0 so an image would
             * never be inserted when publishing the change. Note: this method is called in the
             * record context so the image is inserted by the same thread context as it would be for
             * the publish change logic.
             */
            this.imageCache.put(name, new Record(name, ContextUtils.EMPTY_MAP, this));
            return createRecordInternal_callWithLock(name, ContextUtils.EMPTY_MAP);
        }
    }

    private Record createRecordInternal_callWithLock(final String name, Map<String, IValue> initialData)
    {
        if (this.records.get(name) != null)
        {
            throw new IllegalStateException(
                    "A record with the name [" + name + "] already exists in this context");
        }

        //
        // DO NOT ALTER THE ORDER OF THESE STATEMENTS
        //

        final Record record = new Record(name, ContextUtils.EMPTY_MAP, this);
        this.records.put(name, record);

        record.getPendingAtomicChange().setScope(IRecordChange.IMAGE_SCOPE_CHAR);

        // this will set off an atomic change for the construction
        record.putAll(initialData);

        if (log)
        {
            Log.log(this, "Created record [", record.getName(), "] in ", record.getContextName());
        }

        return record;
    }

    @Override
    public IRecord removeRecord(final String name)
    {
        if (ContextUtils.isSystemRecordName(name))
        {
            throw new IllegalArgumentException("Cannot remove [" + name + "]");
        }

        final IRecord removed = this.records.remove(name);
        if (removed != null)
        {
            synchronized (removed.getWriteLock())
            {
                // NOTE: do not remove subscribers - they are INDEPENDENT of record existence
                // this.recordObservers.removeSubscribersFor(name);

                this.imageCache.remove(name);

                if (log)
                {
                    Log.log(this, "Removed record [", removed.getName(), "] from ", removed.getContextName());
                }

                synchronized (this.recordsToRemoveFromSystemRecords)
                {
                    this.recordsToRemoveFromSystemRecords.add(name);
                }

                // remove the record name from the relevant system records,
                // bulk process multiple deletes by using a coalescing runnable
                this.coreExecutor.execute(new ICoalescingRunnable()
                {
                    @Override
                    public void run()
                    {
                        final Set<String> recordsToProcess;
                        synchronized (Context.this.recordsToRemoveFromSystemRecords)
                        {
                            recordsToProcess =
                                    CollectionUtils.newHashSet(Context.this.recordsToRemoveFromSystemRecords);
                            Context.this.recordsToRemoveFromSystemRecords.clear();
                        }

                        if (recordsToProcess.size() == 0)
                        {
                            return;
                        }

                        final IRecord contextRecords =
                                Context.this.records.get(ISystemRecordNames.CONTEXT_RECORDS);
                        if (isSystemRecordReady(contextRecords))
                        {
                            synchronized (contextRecords.getWriteLock())
                            {
                                for (String name : recordsToProcess)
                                {
                                    contextRecords.remove(name);
                                }
                                publishAtomicChange(ISystemRecordNames.CONTEXT_RECORDS);
                            }
                        }
                    }

                    @Override
                    public Object context()
                    {
                        return Context.this.recordsToRemoveContext;
                    }
                });
            }
        }
        return removed;
    }

    @Override
    public IRecord getRecord(String name)
    {
        IRecord record = this.records.get(name);
        if (ContextUtils.isSystemRecordName(name))
        {
            return record.getImmutableInstance();
        }
        return record;
    }

    @Override
    public IRecord getOrCreateRecord(String name)
    {
        IRecord record = getRecord(name);
        if (record == null)
        {
            synchronized (this.recordCreateLock)
            {
                // another thread may have created it, so check once we hold the lock
                record = getRecord(name);
                if (record == null)
                {
                    return createRecord(name);
                }
            }
        }
        return record;
    }

    @Override
    public Set<String> getRecordNames()
    {
        // ConcurrentHashMap.keySet() ok to use (no copy needed)
        return Collections.unmodifiableSet(this.records.keySet());
    }

    @Override
    public CountDownLatch publishAtomicChange(IRecord record)
    {
        return publishAtomicChange(record.getName());
    }

    @Override
    public CountDownLatch publishAtomicChange(final String name)
    {
        return publishAtomicChange(name, false);
    }

    CountDownLatch publishAtomicChange(final String name, final boolean forcePublish)
    {
        if (name == null)
        {
            throw new NullPointerException("Null record name not allowed");
        }

        this.throttle.eventStart(name, forcePublish);

        try
        {
            final CountDownLatch latch = new CountDownLatch(1);
            final Record record = this.records.get(name);
            if (record == null)
            {
                Log.log(this, "Ignoring publish of non-existent record [", name, "]");
                latch.countDown();
                this.throttle.eventFinish();
                return latch;
            }

            synchronized (record.getWriteLock())
            {
                final AtomicChange atomicChange = record.atomicChange;
                record.atomicChange = null;

                // Note: theory for how atomicChange can be null here:
                // 1. T1 created record
                // 2. T2 calls publish atomicChange with name - publishes
                // 3. T1 has created record and finishes createRecord which calls
                // publishAtomicChange - pending change will have been used

                // update the sequence (version) of the record when publishing
                if (atomicChange != null)
                {
                    record.setSequence(atomicChange.getSequence());
                }

                // prevent empty changes BUT also allow the initial create if it had blank data
                if (atomicChange == null || (!forcePublish && atomicChange.isEmpty()))
                {
                    latch.countDown();
                    this.throttle.eventFinish();
                    return latch;
                }


                atomicChange.preparePublish(latch, this);

                // this will call doPublishChange
                executeSequentialCoreTask(atomicChange);

                return latch;
            }
        }
        catch (RuntimeException e)
        {
            this.throttle.eventFinish();
            throw e;
        }
    }

    @Override
    public Future<Map<String, Boolean>> addObserver(final IRecordListener observer,
            final String... recordNames)
    {
        return addObserver(IPermissionFilter.DEFAULT_PERMISSION_TOKEN, observer, recordNames);
    }

    @Override
    public Future<Map<String, Boolean>> addObserver(final String permissionToken,
            final IRecordListener observer, final String... recordNames)
    {
        if (recordNames == null || recordNames.length == 0)
        {
            throw new IllegalArgumentException(
                    "Null or zero-length subscriptions " + Arrays.toString(recordNames));
        }

        final Map<String, Boolean> resultMap = new HashMap<>(recordNames.length);
        final FutureTask<Map<String, Boolean>> futureResult = new FutureTask<>(() -> {
            final List<String> permissionedRecords = new LinkedList<>();
            for (String recordName : recordNames)
            {
                final String checkedPermissionToken =
                        permissionToken == null ? IPermissionFilter.DEFAULT_PERMISSION_TOKEN :
                                permissionToken;
                if (recordName != null && permissionTokenValidForRecord(checkedPermissionToken, recordName))
                {
                    permissionedRecords.add(recordName);
                    Context.this.tokenPerRecord.put(recordName, checkedPermissionToken);
                    resultMap.put(recordName, Boolean.TRUE);
                }
                else
                {
                    resultMap.put(recordName, Boolean.FALSE);
                }
            }

            // perform the add for the records with valid permission tokens
            addSingleObserver(observer, permissionedRecords);
        }, resultMap);

        futureResult.run();
        return futureResult;
    }

    void addSingleObserver(final IRecordListener observer, Collection<String> recordNames)
    {
        // NOTE: use a single lock to ensure thread-safe access to recordObservers
        synchronized (this.recordCreateLock)
        {
            final Set<String> initialImagePending;
            synchronized (this.listenersToNotifyWithInitialImages)
            {
                final Set<String> exists = this.listenersToNotifyWithInitialImages.get(observer);
                if (exists != null)
                {
                    initialImagePending = exists;
                }
                else
                {
                    initialImagePending = Collections.synchronizedSet(new HashSet<>());
                    this.listenersToNotifyWithInitialImages.put(observer, initialImagePending);
                }
                this.listenersBeingNotifiedWithInitialImages = this.listenersToNotifyWithInitialImages.size();
            }

            // first pass, whilst holding the initialImagePending lock, work out if the listener
            // needs to be added
            final List<String> subscriberAdded = new LinkedList<>();
            synchronized (initialImagePending)
            {
                for (String name : recordNames)
                {
                    // check if the observer has already been registered
                    if (this.recordObservers.addSubscriberFor(name, observer))
                    {
                        subscriberAdded.add(name);
                        initialImagePending.add(name);
                    }
                }
                updateListenerCountsForInitialImages(observer, initialImagePending);
            }

            // second pass, trigger the initial image notification for the names that were added for
            // subscribing
            for (final String name : subscriberAdded)
            {
                if (log)
                {
                    Log.log(this, "Added listener to [", name, "] listener=",
                            ObjectUtils.safeToString(observer));
                }

                executeSequentialCoreTask(new ISequentialRunnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            // happens if a removeObserver is called before the add completes
                            if (!initialImagePending.remove(name))
                            {
                                return;
                            }

                            if (log)
                            {
                                Log.log(this, "Notifying initial image [", name, "], listener=",
                                        ObjectUtils.safeToString(observer));
                            }

                            final IRecord imageSnapshot = getLastPublishedImage_callInRecordContext(name);
                            if (imageSnapshot != null)
                            {
                                final long start = System.nanoTime();
                                observer.onChange(imageSnapshot, new AtomicChange(imageSnapshot));
                                ContextUtils.measureTask(name, "record image-on-subscribe", observer,
                                        (System.nanoTime() - start));
                            }
                            else
                            {
                                if (log)
                                {
                                    Log.log(this, "No initial image available [", name, "], listener=",
                                            ObjectUtils.safeToString(observer));
                                }
                            }
                        }
                        finally
                        {
                            synchronized (initialImagePending)
                            {
                                updateListenerCountsForInitialImages(observer, initialImagePending);
                            }
                        }
                    }

                    @Override
                    public Object context()
                    {
                        return name;
                    }
                });
            }

            addDeltaToSubscriptionCount(1, subscriberAdded);
        }
    }

    @Override
    public CountDownLatch removeObserver(IRecordListener observer, String... recordNames)
    {
        removeSingleObserver(observer, recordNames);
        return new CountDownLatch(0);
    }

    private void removeSingleObserver(IRecordListener observer, String... names)
    {
        // NOTE: use a single lock to ensure thread-safe access to recordObservers
        synchronized (this.recordCreateLock)
        {
            final Set<String> initialImagePending = this.listenersToNotifyWithInitialImages.get(observer);
            if (initialImagePending != null)
            {
                synchronized (initialImagePending)
                {
                    initialImagePending.removeAll(Arrays.asList(names));
                    updateListenerCountsForInitialImages(observer, initialImagePending);
                }
            }

            final List<String> toRemove = new LinkedList<>();
            for (String name : names)
            {
                if (this.recordObservers.removeSubscriberFor(name, observer))
                {
                    if (log)
                    {
                        Log.log(this, "Removed listener from [", name, "] listener=",
                                ObjectUtils.safeToString(observer));
                    }
                    toRemove.add(name);
                }
            }
            addDeltaToSubscriptionCount(-1, toRemove);
        }
    }

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        destroy();
    }

    void addDeltaToSubscriptionCount(final int delta, final Collection<String> recordNames)
    {
        final Map<String, LongValue> countsPerRecord = new HashMap<>(recordNames.size());
        final IRecord contextSubscriptions =
                Context.this.records.get(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        if (isSystemRecordReady(contextSubscriptions))
        {
            synchronized (contextSubscriptions.getWriteLock())
            {
                LongValue observerCount;
                for (String recordName : recordNames)
                {
                    observerCount = contextSubscriptions.get(recordName);
                    if (observerCount == null)
                    {
                        observerCount = LongValue.valueOf(0);
                    }
                    observerCount = LongValue.valueOf(observerCount.longValue() + delta);
                    if (observerCount.longValue() <= 0)
                    {
                        observerCount = LongValue.valueOf(0);
                        contextSubscriptions.remove(recordName);
                        this.tokenPerRecord.remove(recordName);
                    }
                    else
                    {
                        contextSubscriptions.put(recordName, observerCount);
                    }
                    countsPerRecord.put(recordName, observerCount);
                }
                publishAtomicChange(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
            }
        }
        final IRecord contextRecords = Context.this.records.get(ISystemRecordNames.CONTEXT_RECORDS);
        if (isSystemRecordReady(contextRecords))
        {
            synchronized (contextRecords.getWriteLock())
            {
                for (String recordName : recordNames)
                {
                    if (contextRecords.containsKey(recordName))
                    {
                        contextRecords.put(recordName, countsPerRecord.get(recordName));
                    }
                }
                publishAtomicChange(ISystemRecordNames.CONTEXT_RECORDS);
            }
        }
    }

    void updateContextStatusAndPublishChange(IStatusAttribute statusAttribute)
    {
        final IRecord contextStatus = this.records.get(ISystemRecordNames.CONTEXT_STATUS);
        if (isSystemRecordReady(contextStatus))
        {
            synchronized (contextStatus.getWriteLock())
            {
                IStatusAttribute.Utils.setStatus(statusAttribute, contextStatus);
                publishAtomicChange(ISystemRecordNames.CONTEXT_STATUS);
            }
        }
    }

    @Override
    public void createRpc(IRpcInstance rpc)
    {
        final IRecord contextRpcs = this.records.get(ISystemRecordNames.CONTEXT_RPCS);
        if (isSystemRecordReady(contextRpcs))
        {
            synchronized (contextRpcs.getWriteLock())
            {
                if (this.rpcInstances.containsKey(rpc.getName()))
                {
                    throw new IllegalStateException(
                            "An RPC already exists with name [" + rpc.getName() + "]");
                }
                final Map<String, IRpcInstance> copy = new HashMap<>(this.rpcInstances);
                copy.put(rpc.getName(), rpc);
                this.rpcInstances = copy;
                contextRpcs.put(rpc.getName(),
                        TextValue.valueOf(RpcInstance.constructDefinitionFromInstance(rpc)));
                publishAtomicChange(ISystemRecordNames.CONTEXT_RPCS);
                Log.log(this, "Created RPC ", ObjectUtils.safeToString(rpc), " in ", getName());
            }
        }
    }

    @Override
    public void removeRpc(String rpcName)
    {
        final IRecord contextRpcs = this.records.get(ISystemRecordNames.CONTEXT_RPCS);
        if (isSystemRecordReady(contextRpcs))
        {
            synchronized (contextRpcs.getWriteLock())
            {
                final Map<String, IRpcInstance> copy = new HashMap<>(this.rpcInstances);
                final IRpcInstance rpc = copy.remove(rpcName);
                this.rpcInstances = copy;
                if (rpc != null)
                {
                    Log.log(this, "Removing RPC ", ObjectUtils.safeToString(rpc), " from ", getName());
                    this.records.get(ISystemRecordNames.CONTEXT_RPCS).remove(rpcName);
                    publishAtomicChange(ISystemRecordNames.CONTEXT_RPCS);
                }
            }
        }
    }

    @Override
    public IRpcInstance getRpc(String name)
    {
        return this.rpcInstances.get(name);
    }

    @Override
    public ScheduledExecutorService getUtilityExecutor()
    {
        return this.utilityExecutor.get();
    }

    @Override
    public void resubscribe(String... recordNames)
    {
        ContextUtils.resubscribeRecordsForContext(this, this.recordObservers, this.tokenPerRecord,
                recordNames);
    }

    /**
     * <b>ONLY CALL THIS IN AN {@link ISequentialRunnable} RUNNING IN THE SAME CONTEXT AS THE RECORD
     * NAME! OTHERWISE YOU ARE NOT GUARANTEED TO GET THE LAST PUBLISHED IMAGE.</b>
     * <p>
     * Get an immutable record that represents the state of the named record at the last time the {@link
     * #publishAtomicChange(IRecord)} was called on it.
     *
     * @param name the name of the record
     * @return an immutable record that represents the state of the record at the last time of a call to
     * {@link #publishAtomicChange(IRecord)}, <code>null</code> if no image has been published
     */
    IRecord getLastPublishedImage_callInRecordContext(String name)
    {
        return this.imageCache.getImmutableInstance(name);
    }

    @Override
    public boolean addValidator(final IValidator validator)
    {
        final boolean added = this.validators.add(validator);
        if (added)
        {
            this.coreExecutor.execute(() -> validator.onRegistration(Context.this));
        }
        return added;
    }

    @Override
    public void updateValidator(final IValidator validator)
    {
        // NOTE: don't iterate over the map entry set - this will not allow interleaved updates to
        // be handled properly whilst going through the entire map of records
        final Set<String> recordNames = this.imageCache.keySet();
        for (final String name : recordNames)
        {
            executeSequentialCoreTask(new ISequentialRunnable()
            {
                @Override
                public void run()
                {
                    final IRecord record = getLastPublishedImage_callInRecordContext(name);
                    if (record != null)
                    {
                        validator.validate(record, new AtomicChange(record));
                    }
                }

                @Override
                public Object context()
                {
                    return name;
                }
            });
        }
    }

    @Override
    public boolean removeValidator(final IValidator validator)
    {
        final boolean removed = this.validators.remove(validator);
        if (removed)
        {
            this.coreExecutor.execute(() -> validator.onDeregistration(Context.this));
        }
        return removed;
    }

    @Override
    public List<String> getSubscribedRecords()
    {
        synchronized (this.recordCreateLock)
        {
            return this.recordObservers.getAllSubscriptionKeys();
        }
    }

    @Override
    public void executeSequentialCoreTask(ISequentialRunnable sequentialRunnable)
    {
        final Object context = sequentialRunnable.context();
        if (context instanceof String && ContextUtils.isSystemRecordName(context.toString()))
        {
            this.systemExecutor.execute(sequentialRunnable);
        }
        else
        {
            this.coreExecutor.execute(sequentialRunnable);
        }
    }

    void executeRpcTask(ISequentialRunnable sequentialRunnable)
    {
        this.rpcExecutor.execute(sequentialRunnable);
    }

    final boolean permissionTokenValidForRecord(String permissionToken, String recordName)
    {
        if (ContextUtils.isSystemRecordName(recordName) || RpcInstance.isRpcResultRecord(recordName))
        {
            // no permission checks for system records or RPC result records
            return true;
        }
        else
        {
            return this.permissionFilter == null || this.permissionFilter.accept(permissionToken, recordName);
        }
    }

    @Override
    public void setPermissionFilter(IPermissionFilter filter)
    {
        this.permissionFilter = filter;
    }

    final boolean isSystemRecordReady(IRecord systemRecord)
    {
        return systemRecord != null && this.active;
    }

    final void updateListenerCountsForInitialImages(IRecordListener listener, Set<String> initialImagePending)
    {
        if (initialImagePending.size() == 0)
        {
            synchronized (this.listenersToNotifyWithInitialImages)
            {
                this.listenersToNotifyWithInitialImages.remove(listener);
                this.listenersBeingNotifiedWithInitialImages = this.listenersToNotifyWithInitialImages.size();
            }
        }
    }

    public Map<String, IRpcInstance> getAllRpcs()
    {
        return Collections.unmodifiableMap(this.rpcInstances);
    }

    void doPublishChange(final String recordName, final IRecordChange atomicChange, long sequence)
    {
        if (sequence == 0)
        {
            this.imageCache.put(recordName, new Record(recordName, ContextUtils.EMPTY_MAP, this));
        }

        // update the image with the atomic changes in the runnable
        final IRecord notifyImage = this.imageCache.updateInstance(recordName, atomicChange);

        // this can happen if there is a concurrent delete
        if (notifyImage == null)
        {
            return;
        }

        if (this.validators.size() > 0)
        {
            for (IValidator validator : this.validators)
            {
                validator.validate(notifyImage, atomicChange);
            }
        }

        long start;

        // NOTE: always get the subscribers to notify in the context of the handling the record
        // change! If we had a snapshot of the subscribers taken outside of the context, we would
        // miss the new listener and if the new listener was added and got its initial image
        // first (could happen), it would never get this update - see addSingleObserver
        IRecordListener[] listenersToNotify = this.recordObservers.getSubscribersFor(recordName);

        // if there are any pending initial images waiting, we need to ensure we
        // don't notify this update to the registered listener
        if (this.listenersBeingNotifiedWithInitialImages > 0)
        {
            // work out who to notify, i.e. listeners NOT expecting an image
            Set<String> initialImagePending;
            final List<IRecordListener> listenersNotExpectingImage = new LinkedList<>();
            for (IRecordListener listener : listenersToNotify)
            {
                // NOTE: cannot optimise by locking
                // listenersToNotifyWithInitialImages outside the loop - this can
                // lead to a deadlock as the lock order with initialImagePending
                // will then become broken
                initialImagePending = this.listenersToNotifyWithInitialImages.get(listener);
                if (initialImagePending != null && initialImagePending.contains(recordName))
                {
                    // don't notify - let the initial image task do this
                    // as it will pass in a full image as the atomic change
                    // (thus simulating the initial image)
                }
                else
                {
                    listenersNotExpectingImage.add(listener);
                }
            }
            listenersToNotify = listenersNotExpectingImage.toArray(new IRecordListener[0]);
        }

        for (IRecordListener listener : listenersToNotify)
        {
            try
            {
                start = System.nanoTime();
                listener.onChange(notifyImage, atomicChange);
                ContextUtils.measureTask(recordName, "local record update", listener,
                        (System.nanoTime() - start));
            }
            catch (Exception e)
            {
                Log.log(this, "Could not notify " + listener + " with " + atomicChange, e);
            }
        }
    }
}