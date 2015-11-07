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
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
import com.fimtra.thimble.ICoalescingRunnable;
import com.fimtra.thimble.ISequentialRunnable;
import com.fimtra.thimble.ThimbleExecutor;
import com.fimtra.util.DeadlockDetector;
import com.fimtra.util.DeadlockDetector.DeadlockObserver;
import com.fimtra.util.DeadlockDetector.ThreadInfoWrapper;
import com.fimtra.util.FileUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SubscriptionManager;
import com.fimtra.util.SystemUtils;
import com.fimtra.util.ThreadUtils;
import com.fimtra.util.UtilProperties;

/**
 * A context is the home for a group of records. The definition of the context is application
 * specific.
 * <p>
 * A context can publish changes to its records to one or more {@link IRecordListener} objects in
 * the local runtime.
 * <p>
 * To publish changes to remote observers, a {@link Publisher} must be created and attached to the
 * context. The publisher can then publish changes to one or more {@link ProxyContext} instances.
 * <p>
 * Operations that mutate any record are performed using a {@link Lock} associated with the name of
 * the record. This allows operations on different records to run in parallel.
 * 
 * @author Ramon Servadei
 */
public final class Context implements IPublisherContext, IAtomicChangeManager
{

    static
    {
        DeadlockDetector.newDeadlockDetectorThread("deadlock-detector", 60000, new DeadlockObserver()
        {
            @Override
            public void onDeadlockFound(ThreadInfoWrapper[] deadlocks)
            {
                StringBuilder sb = new StringBuilder();
                sb.append("DEADLOCKED THREADS FOUND!").append(SystemUtils.lineSeparator());
                for (int i = 0; i < deadlocks.length; i++)
                {
                    sb.append(deadlocks[i].toString());
                }
                System.err.println(sb.toString());
            }
        }, UtilProperties.Values.USE_ROLLING_THREADDUMP_FILE);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                Log.log(Context.class, "JVM shutting down...");
                final DeadlockDetector deadlockDetector = new DeadlockDetector();
                final String filePrefix = ThreadUtils.getMainMethodClassSimpleName() + "-threadDumpOnExit";
                // delete old files
                FileUtils.deleteFiles(new File(UtilProperties.Values.LOG_DIR),
                    TimeUnit.MINUTES.convert(1, TimeUnit.DAYS), filePrefix);
                final File threadDumpOnShutdownFile =
                    FileUtils.createLogFile_yyyyMMddHHmmss(UtilProperties.Values.LOG_DIR, filePrefix);
                final ThreadInfoWrapper[] threads = deadlockDetector.getThreadInfoWrappers();
                if (threads != null)
                {
                    PrintWriter pw = null;
                    try
                    {
                        pw = new PrintWriter(threadDumpOnShutdownFile);
                        for (int i = 0; i < threads.length; i++)
                        {
                            pw.print(threads[i].toString());
                            pw.flush();
                        }
                        Log.log(Context.class, "Thread dump successful: ", threadDumpOnShutdownFile.toString());
                    }
                    catch (Exception e)
                    {
                        Log.log(Context.class, "Could not produce threaddump file on exit", e);
                    }
                    finally
                    {
                        if (pw != null)
                        {
                            pw.close();
                        }
                    }
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

    /**
     * Noop implementation
     * 
     * @author Ramon Servadei
     */
    static final class NoopAtomicChangeManager implements IAtomicChangeManager
    {
        private final String name;

        NoopAtomicChangeManager(String name)
        {
            this.name = name;
        }

        @Override
        public String getName()
        {
            return this.name;
        }

        @Override
        public void addEntryUpdatedToAtomicChange(Record record, String key, IValue current, IValue previous)
        {
        }

        @Override
        public void addEntryRemovedToAtomicChange(Record record, String key, IValue value)
        {
        }

        @Override
        public void addSubMapEntryUpdatedToAtomicChange(Record record, String subMapKey, String key, IValue current,
            IValue previous)
        {
        }

        @Override
        public void addSubMapEntryRemovedToAtomicChange(Record record, String subMapKey, String key, IValue value)
        {
        }
    }

    /** Holds all records in this context */
    final ConcurrentMap<String, IRecord> records;

    /**
     * Maintains a map of {@link Record} images and {@link ImmutableRecord} instances backed by the
     * images. Changes are applied to the images which can be viewed by the immutable instances.
     * 
     * @author Ramon Servadei
     */
    final static class ImageCache
    {
        /**
         * The image of each record that is updated with atomic changes and used to construct an
         * {@link ImmutableRecord} to pass in to
         * {@link IRecordListener#onChange(IRecord, IRecordChange)}
         * <p>
         * <b>NOTE: the images are only ever updated with atomic changes in the
         * {@link ISequentialRunnable} that notifies the {@link IRecordListener} instances. This
         * ensures that the listener instances only see an image that reflects the atomic change
         * that caused the listener to be notified.</b>
         */
        final ConcurrentMap<String, Record> images;

        final ConcurrentMap<String, ImmutableRecord> immutableImages;

        ImageCache(int size)
        {
            this.images = new ConcurrentHashMap<String, Record>(size);
            this.immutableImages = new ConcurrentHashMap<String, ImmutableRecord>(size);
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
                change.applyCompleteAtomicChangeToRecord(record);
            }
            // the Record in the images map backs the ImmutableRecord in the immutableImages map
            return this.immutableImages.get(name);
        }

        /**
         * @return an {@link ImmutableRecord} for the record name, <code>null</code> if the record
         *         does not exist
         */
        ImmutableRecord getImmutableInstance(String name)
        {
            return this.immutableImages.get(name);
        }
    }

    /**
     * Manages the images of the contex's records.
     * <p>
     * <b>NOTE: the images are only ever updated with atomic changes in the
     * {@link ISequentialRunnable} that notifies the {@link IRecordListener} instances. This ensures
     * that the listener instances only see an image that reflects the atomic change that caused the
     * listener to be notified.</b>
     */
    final ImageCache imageCache;

    /** Tracks the observers for the records in this context */
    final SubscriptionManager<String, IRecordListener> recordObservers;
    /**
     * Tracks pending atomic changes to records. An entry for a record only exists if there is an
     * observer for the record, otherwise there is no need to track changes.
     */
    final ConcurrentMap<String, AtomicChange> pendingAtomicChanges;
    final ConcurrentMap<String, IRpcInstance> rpcInstances;
    final ConcurrentMap<String, AtomicLong> sequences;
    final Set<IValidator> validators;
    volatile boolean active;
    final String name;
    final Executor rpcExecutor;
    final Executor coreExecutor;
    final ScheduledExecutorService utilityExecutor;
    final Lock recordCreateLock;
    final IAtomicChangeManager noopChangeManager;
    /**
     * Tracks what records have been deleted and need to be removed from system records - this is an
     * efficiency optimisation for bulk remove operations
     */
    final Set<String> recordsToRemoveFromSystemRecords;
    final String recordsToRemoveContext;
    IPermissionFilter permissionFilter;
    /** The permission token used for subscribing each record */
    final Map<String, String> tokenPerRecord;

    /** Construct the context with the given name */
    public Context(String name)
    {
        this(name, null, null, null);
    }

    /**
     * Construct the context
     * 
     * @param name
     *            the name of the context
     * @param eventExecutor
     *            the executor for handling events, if <code>null</code> the default event executor
     *            is used
     * @param rpcExecutor
     *            the executor for handling RPCs, if <code>null</code> the default RPC executor is
     *            used
     * @param utilityExecutor
     *            the utility {@link ScheduledExecutorService}, if <code>null</code> the default
     *            utility executor is used
     */
    public Context(String name, ThimbleExecutor eventExecutor, ThimbleExecutor rpcExecutor,
        ScheduledExecutorService utilityExecutor)
    {
        super();

        this.name = name;
        this.noopChangeManager = new NoopAtomicChangeManager(this.name);
        this.rpcExecutor = rpcExecutor == null ? ContextUtils.RPC_EXECUTOR : rpcExecutor;
        this.coreExecutor = eventExecutor == null ? ContextUtils.CORE_EXECUTOR : eventExecutor;
        this.utilityExecutor = utilityExecutor == null ? ContextUtils.UTILITY_SCHEDULER : utilityExecutor;
        this.recordCreateLock = new ReentrantLock();
        this.recordObservers = new SubscriptionManager<String, IRecordListener>(IRecordListener.class);
        this.recordsToRemoveFromSystemRecords = new HashSet<String>();
        this.recordsToRemoveContext = "recordsToRemoveFromSystemRecords:" + name
        // this MUST be added to ensure complete uniqueness for coalescing in case more than one
        // Context share the same name!
            + "-" + UUID.randomUUID();

        final int initialSize = 1024;
        this.sequences = new ConcurrentHashMap<String, AtomicLong>(initialSize);
        this.imageCache = new ImageCache(initialSize);
        this.records = new ConcurrentHashMap<String, IRecord>(initialSize);
        this.pendingAtomicChanges = new ConcurrentHashMap<String, AtomicChange>(initialSize);
        this.tokenPerRecord = new ConcurrentHashMap<String, String>(initialSize);

        this.rpcInstances = new ConcurrentHashMap<String, IRpcInstance>();
        this.validators = new CopyOnWriteArraySet<IValidator>();

        // create the special records by hand
        createSystemRecord(ISystemRecordNames.CONTEXT_RECORDS);
        createSystemRecord(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        createSystemRecord(ISystemRecordNames.CONTEXT_STATUS);
        createSystemRecord(ISystemRecordNames.CONTEXT_RPCS);
        createSystemRecord(ISystemRecordNames.CONTEXT_CONNECTIONS);

        this.active = true;
    }

    private Record createSystemRecord(String recordName)
    {
        this.sequences.put(recordName, new AtomicLong());
        this.pendingAtomicChanges.put(recordName, new AtomicChange(recordName));
        this.pendingAtomicChanges.get(recordName).setSequence(this.sequences.get(recordName).incrementAndGet());
        this.imageCache.put(recordName, new Record(recordName, ContextUtils.EMPTY_MAP, this.noopChangeManager));
        Record record = new Record(recordName, ContextUtils.EMPTY_MAP, this);
        this.records.put(recordName, record);

        // add to the context record
        this.records.get(ISystemRecordNames.CONTEXT_RECORDS).put(recordName, LongValue.valueOf(0));

        return record;
    }

    @Override
    public void destroy()
    {
        try
        {
            if (ContextUtils.UTILITY_SCHEDULER != this.utilityExecutor)
            {
                this.utilityExecutor.shutdown();
            }
            if (ContextUtils.CORE_EXECUTOR != this.coreExecutor)
            {
                ((ThimbleExecutor) this.coreExecutor).destroy();
            }
            this.pendingAtomicChanges.clear();
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
        if (ContextUtils.isSystemRecordName(name) && !ContextUtils.checkLegalCharacters(name))
        {
            throw new IllegalArgumentException("The name '" + name + "' is reserved");
        }

        final Record record;
        final IRecordListener[] subscribersForInstance;
        this.recordCreateLock.lock();
        try
        {
            record = createRecordInternal_callWithLock(name, initialData);
            subscribersForInstance = this.recordObservers.getSubscribersFor(name);
        }
        finally
        {
            this.recordCreateLock.unlock();
        }

        // publish the update to the ContextRecords before publishing to observers - one of the
        // observers may check the ContextRecords so the created record MUST be in there before
        final IRecord contextRecords = this.records.get(ISystemRecordNames.CONTEXT_RECORDS);
        contextRecords.getWriteLock().lock();
        try
        {
            contextRecords.put(name, LongValue.valueOf(subscribersForInstance.length));
            publishAtomicChange(ISystemRecordNames.CONTEXT_RECORDS);
        }
        finally
        {
            contextRecords.getWriteLock().unlock();
        }

        // now notify observers of the record with its initial image
        if (subscribersForInstance.length > 0)
        {
            Log.log(this, "Subscriber count is ", Integer.toString(subscribersForInstance.length),
                " for created record '", name, "'");

            record.getWriteLock().lock();
            try
            {
                // create and trigger the image publish whilst holding the record's lock - prevents
                // (however unlikely) concurrent publishing occurring whilst creating
                this.coreExecutor.execute(new ISequentialRunnable()
                {
                    @Override
                    public void run()
                    {
                        long start;
                        final IRecord imageToPublish = getLastPublishedImage(name);
                        final IRecordChange atomicChange = new AtomicChange(imageToPublish);

                        for (int i = 0; i < subscribersForInstance.length; i++)
                        {
                            start = System.nanoTime();
                            subscribersForInstance[i].onChange(imageToPublish, atomicChange);
                            ContextUtils.measureTask(name, "notify created record", subscribersForInstance[i],
                                (System.nanoTime() - start));
                        }
                    }

                    @Override
                    public Object context()
                    {
                        return name;
                    }
                });
            }
            finally
            {
                record.getWriteLock().unlock();
            }
        }
        return record;
    }

    /**
     * Create a blank record with this name - no listeners are notified of the record's existence in
     * this method.
     */
    IRecord createRecordSilently(String name)
    {
        this.recordCreateLock.lock();
        try
        {
            return createRecordInternal_callWithLock(name, ContextUtils.EMPTY_MAP);
        }
        finally
        {
            this.recordCreateLock.unlock();
        }
    }

    private Record createRecordInternal_callWithLock(final String name, Map<String, IValue> initialData)
    {
        final Record record;
        if (this.records.get(name) != null)
        {
            throw new IllegalStateException("A record with the name '" + name + "' already exists in this context");
        }

        this.sequences.put(name, new AtomicLong());
        this.imageCache.put(name, new Record(name, initialData, this.noopChangeManager));
        record = new Record(name, initialData, this);
        if (!ContextUtils.isSystemRecordName(record.getName()))
        {
            Log.log(this, "Created record '", record.getName(), "' in context '", record.getContextName(), "'");
        }
        this.records.put(name, record);
        return record;
    }

    @Override
    public IRecord removeRecord(final String name)
    {
        if (ContextUtils.isSystemRecordName(name))
        {
            throw new IllegalArgumentException("Cannot remove '" + name + "'");
        }

        if (!this.records.containsKey(name))
        {
            return null;
        }

        final IRecord removed = this.records.remove(name);
        if (removed != null)
        {
            removed.getWriteLock().lock();
            try
            {
                // NOTE: do not remove subscribers - they are INDEPENDENT of record existence
                // this.recordObservers.removeSubscribersFor(name);

                this.pendingAtomicChanges.remove(name);
                this.sequences.remove(name);
                this.imageCache.remove(name);
                Log.log(this, "Removed '", removed.getName(), "' from context '", removed.getContextName(), "'");

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
                            recordsToProcess = new HashSet<String>(Context.this.recordsToRemoveFromSystemRecords);
                            Context.this.recordsToRemoveFromSystemRecords.clear();
                        }

                        if (recordsToProcess.size() == 0)
                        {
                            return;
                        }

                        final IRecord contextRecords = Context.this.records.get(ISystemRecordNames.CONTEXT_RECORDS);
                        contextRecords.getWriteLock().lock();
                        try
                        {
                            for (String name : recordsToProcess)
                            {
                                contextRecords.remove(name);
                            }
                            publishAtomicChange(ISystemRecordNames.CONTEXT_RECORDS);
                        }
                        finally
                        {
                            contextRecords.getWriteLock().unlock();
                        }
                        final IRecord contextSubscriptions =
                            Context.this.records.get(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
                        contextSubscriptions.getWriteLock().lock();
                        try
                        {
                            for (String name : recordsToProcess)
                            {
                                contextSubscriptions.remove(name);
                            }
                            publishAtomicChange(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
                        }
                        finally
                        {
                            contextSubscriptions.getWriteLock().unlock();
                        }
                    }

                    @Override
                    public Object context()
                    {
                        return Context.this.recordsToRemoveContext;
                    }
                });
            }
            finally
            {
                removed.getWriteLock().unlock();
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
            this.recordCreateLock.lock();
            try
            {
                // another thread may have created it, so check once we hold the lock
                record = getRecord(name);
                if (record == null)
                {
                    return createRecord(name);
                }
            }
            finally
            {
                this.recordCreateLock.unlock();
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
        final CountDownLatch latch = new CountDownLatch(1);
        final IRecord record = this.records.get(name);
        if (record == null)
        {
            Log.log(this, "Ignoring publish of non-existent record: ", name);
            latch.countDown();
            return latch;
        }

        record.getWriteLock().lock();
        try
        {
            final IRecordChange atomicChange = this.pendingAtomicChanges.remove(name);
            if (atomicChange == null || atomicChange.isEmpty())
            {
                latch.countDown();
                return latch;
            }

            // update the sequence (version) of the record when publishing
            ((Record) record).setSequence(atomicChange.getSequence());

            final ISequentialRunnable notifyTask = new ISequentialRunnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        // update the image with the atomic changes in the runnable
                        final IRecord notifyImage = Context.this.imageCache.updateInstance(name, atomicChange);

                        if (Context.this.validators.size() > 0)
                        {
                            for (IValidator validator : Context.this.validators)
                            {
                                validator.validate(notifyImage, atomicChange);
                            }
                        }

                        long start;
                        IRecordListener iAtomicChangeObserver = null;
                        final IRecordListener[] subscribersFor = Context.this.recordObservers.getSubscribersFor(name);
                        for (int i = 0; i < subscribersFor.length; i++)
                        {
                            try
                            {
                                iAtomicChangeObserver = subscribersFor[i];
                                start = System.nanoTime();
                                iAtomicChangeObserver.onChange(notifyImage, atomicChange);
                                ContextUtils.measureTask(name, "local record update", iAtomicChangeObserver,
                                    (System.nanoTime() - start));
                            }
                            catch (Exception e)
                            {
                                Log.log(Context.this, "Could not notify " + iAtomicChangeObserver + " with "
                                    + atomicChange, e);
                            }
                        }
                    }
                    finally
                    {
                        latch.countDown();
                    }
                }

                @Override
                public Object context()
                {
                    return name;
                }
            };
            this.coreExecutor.execute(notifyTask);
            return latch;
        }
        finally
        {
            record.getWriteLock().unlock();
        }
    }

    @Override
    public Future<Map<String, Boolean>> addObserver(final IRecordListener observer, final String... recordNames)
    {
        return addObserver(IPermissionFilter.DEFAULT_PERMISSION_TOKEN, observer, recordNames);
    }

    @Override
    public Future<Map<String, Boolean>> addObserver(final String permissionToken, final IRecordListener observer,
        final String... recordNames)
    {
        if (recordNames == null || recordNames.length == 0)
        {
            throw new IllegalArgumentException("Null or zero-length subscriptions " + Arrays.toString(recordNames));
        }

        final Map<String, Boolean> resultMap = new HashMap<String, Boolean>(recordNames.length);
        final FutureTask<Map<String, Boolean>> futureResult = new FutureTask<Map<String, Boolean>>(new Runnable()
        {
            @Override
            public void run()
            {
                for (int i = 0; i < recordNames.length; i++)
                {
                    if (permissionTokenValidForRecord(permissionToken, recordNames[i]))
                    {
                        Context.this.tokenPerRecord.put(recordNames[i], permissionToken);
                        doAddSingleObserver(recordNames[i], observer);
                        resultMap.put(recordNames[i], Boolean.TRUE);
                    }
                    else
                    {
                        resultMap.put(recordNames[i], Boolean.FALSE);
                    }
                }
            }
        }, resultMap);

        futureResult.run();
        return futureResult;
    }

    void doAddSingleObserver(final String name, final IRecordListener observer)
    {
        final IRecord record = this.records.get(name);
        final Lock lock;
        if (record != null)
        {
            lock = record.getWriteLock();
        }
        else
        {
            lock = this.recordCreateLock;
        }
        lock.lock();
        try
        {
            if (this.recordObservers.addSubscriberFor(name, observer))
            {
                Log.log(this, "Added listener to '", name, "' listener=", ObjectUtils.safeToString(observer));

                // Check if there is an image before creating a task to notify with the image.
                // Don't try an optimise by re-use the published image we get here - its not safe to
                // cache, see javadocs on the method - we only want to know if there is an image
                if (getLastPublishedImage(name) != null)
                {
                    this.coreExecutor.execute(new ISequentialRunnable()
                    {
                        @Override
                        public void run()
                        {
                            Log.log(this, "Notifying initial image '", name, "', listener=",
                                ObjectUtils.safeToString(observer));
                            final long start = System.nanoTime();
                            final IRecord imageSnapshot = getLastPublishedImage(name);
                            observer.onChange(imageSnapshot, new AtomicChange(imageSnapshot));
                            ContextUtils.measureTask(name, "record image-on-subscribe", observer,
                                (System.nanoTime() - start));
                        }

                        @Override
                        public Object context()
                        {
                            return name;
                        }
                    });
                }
                addDeltaToSubscriptionCount(name, 1);
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public CountDownLatch removeObserver(IRecordListener observer, String... recordNames)
    {
        for (int i = 0; i < recordNames.length; i++)
        {
            doRemoveSingleObserver(recordNames[i], observer);
        }
        return new CountDownLatch(0);
    }

    private void doRemoveSingleObserver(final String name, IRecordListener observer)
    {
        final IRecord record = this.records.get(name);
        final Lock lock;
        if (record != null)
        {
            lock = record.getWriteLock();
        }
        else
        {
            lock = this.recordCreateLock;
        }
        lock.lock();
        try
        {
            if (this.recordObservers.removeSubscriberFor(name, observer))
            {
                Log.log(this, "Removed listener from '", name, "' listener=", ObjectUtils.safeToString(observer));
                addDeltaToSubscriptionCount(name, -1);
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        destroy();
    }

    void addDeltaToSubscriptionCount(final String recordName, final int delta)
    {
        /*
         * There is a danger that a remove and add observer could run concurrently and we could get
         * deltas out of sequence...e.g. if the count was initially 0 and we added then removed the
         * observer, instead of a sequence 0, 1, 0 we could get 0, -1, 0....but we don't want to
         * lock the record name, then lock the context_subscriptions record within the other lock
         * (could open up deadlocks), therefore, we do the delta updates on the thimble threads
         * using a sequential runnable and submit the tasks whilst holding the record name lock to
         * ensure ordering
         */
        this.coreExecutor.execute(new ISequentialRunnable()
        {
            @Override
            public void run()
            {
                final IRecord contextSubscriptions = Context.this.records.get(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
                if (contextSubscriptions != null)
                {
                    contextSubscriptions.getWriteLock().lock();
                    try
                    {
                        LongValue observerCount = (LongValue) contextSubscriptions.get(recordName);
                        if (observerCount == null)
                        {
                            observerCount = LongValue.valueOf(0);
                        }
                        final long count = observerCount.longValue() + delta;
                        if (count <= 0)
                        {
                            contextSubscriptions.remove(recordName);
                            Context.this.tokenPerRecord.remove(recordName);
                        }
                        else
                        {
                            contextSubscriptions.put(recordName, LongValue.valueOf(count));
                        }
                        publishAtomicChange(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
                    }
                    finally
                    {
                        contextSubscriptions.getWriteLock().unlock();
                    }
                }
                final IRecord contextRecords = Context.this.records.get(ISystemRecordNames.CONTEXT_RECORDS);
                if (contextRecords != null)
                {
                    contextRecords.getWriteLock().lock();
                    try
                    {
                        if (contextRecords.containsKey(recordName))
                        {
                            contextRecords.put(recordName,
                                LongValue.valueOf(contextRecords.get(recordName).longValue() + delta));
                            publishAtomicChange(ISystemRecordNames.CONTEXT_RECORDS);
                        }
                    }
                    finally
                    {
                        contextRecords.getWriteLock().unlock();
                    }
                }
            }

            @Override
            public Object context()
            {
                return recordName;
            }
        });
    }

    private AtomicChange getPendingAtomicChangesForWrite(String name)
    {
        AtomicChange atomicChange = this.pendingAtomicChanges.get(name);
        if (atomicChange == null && this.records.containsKey(name))
        {
            atomicChange = new AtomicChange(name);
            atomicChange.setSequence(this.sequences.get(name).incrementAndGet());
            this.pendingAtomicChanges.put(name, atomicChange);
        }
        return atomicChange;
    }

    @Override
    public void addEntryUpdatedToAtomicChange(Record record, String key, IValue current, IValue previous)
    {
        final AtomicChange atomicChange = getPendingAtomicChangesForWrite(record.getName());
        if (atomicChange != null)
        {
            atomicChange.mergeEntryUpdatedChange(key, current, previous);
        }
    }

    @Override
    public void addEntryRemovedToAtomicChange(Record record, String key, IValue value)
    {
        final AtomicChange atomicChange = getPendingAtomicChangesForWrite(record.getName());
        if (atomicChange != null)
        {
            atomicChange.mergeEntryRemovedChange(key, value);
        }
    }

    @Override
    public void addSubMapEntryUpdatedToAtomicChange(Record record, String subMapKey, String key, IValue current,
        IValue previous)
    {
        final AtomicChange atomicChange = getPendingAtomicChangesForWrite(record.getName());
        if (atomicChange != null)
        {
            atomicChange.mergeSubMapEntryUpdatedChange(subMapKey, key, current, previous);
        }
    }

    @Override
    public void addSubMapEntryRemovedToAtomicChange(Record record, String subMapKey, String key, IValue value)
    {
        final AtomicChange atomicChange = getPendingAtomicChangesForWrite(record.getName());
        if (atomicChange != null)
        {
            atomicChange.mergeSubMapEntryRemovedChange(subMapKey, key, value);
        }
    }

    @SuppressWarnings("null")
    void updateContextStatusAndPublishChange(IStatusAttribute statusAttribute)
    {
        Log.log(this, ObjectUtils.safeToString(statusAttribute), " ", this.getName());
        final IRecord contextStatus = this.records.get(ISystemRecordNames.CONTEXT_STATUS);
        if (contextStatus == null && !this.active)
        {
            // on shutdown, contextStatus can be null
            return;
        }
        contextStatus.getWriteLock().lock();
        try
        {
            IStatusAttribute.Utils.setStatus(statusAttribute, contextStatus);
            publishAtomicChange(ISystemRecordNames.CONTEXT_STATUS);
        }
        finally
        {
            contextStatus.getWriteLock().unlock();
        }
    }

    @Override
    public void createRpc(IRpcInstance rpc)
    {
        final IRecord contextRpcs = this.records.get(ISystemRecordNames.CONTEXT_RPCS);
        contextRpcs.getWriteLock().lock();
        try
        {
            if (this.rpcInstances.containsKey(rpc.getName()))
            {
                throw new IllegalStateException("An RPC already exists with name '" + rpc.getName() + "'");
            }
            this.rpcInstances.put(rpc.getName(), rpc);
            contextRpcs.put(rpc.getName(), new TextValue(RpcInstance.constructDefinitionFromInstance(rpc)));
            publishAtomicChange(ISystemRecordNames.CONTEXT_RPCS);
            Log.log(this, "Created RPC ", ObjectUtils.safeToString(rpc));
        }
        finally
        {
            contextRpcs.getWriteLock().unlock();
        }
    }

    @Override
    public void removeRpc(String rpcName)
    {
        final IRecord contextRpcs = this.records.get(ISystemRecordNames.CONTEXT_RPCS);
        contextRpcs.getWriteLock().lock();
        try
        {
            final IRpcInstance rpc = this.rpcInstances.remove(rpcName);
            if (rpc != null)
            {
                Log.log(this, "Removing RPC ", ObjectUtils.safeToString(rpc));
                this.records.get(ISystemRecordNames.CONTEXT_RPCS).remove(rpcName);
                publishAtomicChange(ISystemRecordNames.CONTEXT_RPCS);
            }
        }
        finally
        {
            contextRpcs.getWriteLock().unlock();
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
        return this.utilityExecutor;
    }

    @Override
    public void resubscribe(String... recordNames)
    {
        ContextUtils.resubscribeRecordsForContext(this, this.recordObservers, this.tokenPerRecord, recordNames);
    }

    /**
     * <b>ONLY CALL THIS IN AN {@link ISequentialRunnable} RUNNING IN THE SAME CONTEXT AS THE RECORD
     * NAME! OTHERWISE YOU ARE NOT GUARANTEED TO GET THE LAST PUBLISHED IMAGE.</b>
     * <P>
     * Get an immutable record that represents the state of the named record at the last time the
     * {@link #publishAtomicChange(IRecord)} was called on it.
     * 
     * @param name
     *            the name of the record
     * @return an immutable record that represents the state of the record at the last time of a
     *         call to {@link #publishAtomicChange(IRecord)}, <code>null</code> if no image has been
     *         published
     */
    IRecord getLastPublishedImage(String name)
    {
        return this.imageCache.getImmutableInstance(name);
    }

    @Override
    public boolean addValidator(final IValidator validator)
    {
        final boolean added = this.validators.add(validator);
        if (added)
        {
            this.coreExecutor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    validator.onRegistration(Context.this);
                }
            });
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
            this.coreExecutor.execute(new ISequentialRunnable()
            {
                @Override
                public void run()
                {
                    final IRecord record = getLastPublishedImage(name);
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
            this.coreExecutor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    validator.onDeregistration(Context.this);
                }
            });
        }
        return removed;
    }

    @Override
    public Set<String> getSubscribedRecords()
    {
        return this.recordObservers.getAllSubscriptionKeys();
    }

    @Override
    public void executeSequentialCoreTask(ISequentialRunnable sequentialRunnable)
    {
        this.coreExecutor.execute(sequentialRunnable);
    }

    void executeRpcTask(ISequentialRunnable sequentialRunnable)
    {
        this.rpcExecutor.execute(sequentialRunnable);
    }

    /**
     * Provides the means for a {@link ProxyContext} to tell its internal {@link Context} what
     * sequence to use when processing a received record change
     */
    void setSequence(String recordName, long sequence)
    {
        this.sequences.get(recordName).set(sequence);
        getPendingAtomicChangesForWrite(recordName).setSequence(sequence);
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
            return this.permissionFilter == null ? true : this.permissionFilter.accept(permissionToken, recordName);
        }
    }

    @Override
    public void setPermissionFilter(IPermissionFilter filter)
    {
        this.permissionFilter = filter;
    }
}

/**
 * This is an internal interface for managing the adding/removing atomic changes for a record.
 * 
 * @author Ramon Servadei
 */
interface IAtomicChangeManager
{
    String getName();

    void addEntryUpdatedToAtomicChange(Record record, String key, IValue current, IValue previous);

    void addEntryRemovedToAtomicChange(Record record, String key, IValue value);

    void addSubMapEntryUpdatedToAtomicChange(Record record, String subMapKey, String key, IValue current,
        IValue previous);

    void addSubMapEntryRemovedToAtomicChange(Record record, String subMapKey, String key, IValue value);
}