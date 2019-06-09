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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ISubscribingChannel;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.channel.ITransportChannelBuilder;
import com.fimtra.channel.ITransportChannelBuilderFactory;
import com.fimtra.channel.StaticEndPointAddressFactory;
import com.fimtra.channel.TransportChannelBuilderFactoryLoader;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.DataFissionProperties.Values;
import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IPermissionFilter;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.ISessionProtocol.SyncResponse;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.AtomicChangeTeleporter.IncorrectSequenceException;
import com.fimtra.datafission.core.IStatusAttribute.Connection;
import com.fimtra.datafission.core.session.ISessionListener;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannel;
import com.fimtra.thimble.ISequentialRunnable;
import com.fimtra.util.ByteArrayPool;
import com.fimtra.util.IReusableObjectBuilder;
import com.fimtra.util.IReusableObjectFinalizer;
import com.fimtra.util.Log;
import com.fimtra.util.MultiThreadReusableObjectPool;
import com.fimtra.util.NotifyingCache;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.Pair;
import com.fimtra.util.SubscriptionManager;
import com.fimtra.util.ThreadUtils;

/**
 * A proxy context allows a local runtime to observe records from a single {@link Context} in a
 * remote runtime.
 * <p>
 * A proxy context connects to a single remote {@link Publisher} instance using a 'transport
 * channel' ({@link ITransportChannel}). On construction of the proxy context, the channel is
 * established. If the channel is interrupted, the proxy context re-initialises the channel and
 * re-subscribes for all currently subscribed records being observed. This will continue
 * indefinitely until the channel is made or the {@link #destroy()} method is called.
 * <p>
 * <b>Note:</b> during the disconnection period, remote records (records that were subscribed for
 * from the remote publisher) are not cleared but the data in the record should be considered stale.
 * Subscribing for the {@link #RECORD_CONNECTION_STATUS_NAME} record allows application code to
 * detect when records are stale (effectively connection to the remote has been lost). If
 * application code needs to 'clear' records when they are disconnected in anticipation for
 * re-population on connection, call the {@link #resubscribe(String...)} method.
 * <p>
 * A proxy context receives changes to records from a {@link Publisher}. The proxy context requests
 * the records that should be observed by the publisher for changes. There is a 1:n relationship
 * between a publisher and a proxy context; one publisher can be publishing to many proxy contexts.
 * <p>
 * When a remote record is subscribed for, a local 'proxy' is created that reflects all changes from
 * the remote. When there are no more observers of the remote record, this local proxy record is
 * deleted.
 * <p>
 * A proxy context has a special record; a remote context registry which shows the context registry
 * of the remote context. Use this to find out what records exist in the remote context. Add an
 * observer for the {@link IRemoteSystemRecordNames#REMOTE_CONTEXT_RECORDS} record to get the
 * context registry of the remote context.
 * <p>
 * A proxy context has another special record; a remote connection status record. This shows the
 * connection status on a per record basis. Subscribing for this record (
 * {@link #RECORD_CONNECTION_STATUS_NAME}) provides application code with the ability to detect
 * connection status changes for a record.
 * 
 * @see ISystemRecordNames#CONTEXT_RECORDS
 * @author Ramon Servadei
 */
@SuppressWarnings("rawtypes")
public final class ProxyContext implements IObserverContext
{
    /**
     * Controls logging of:
     * <ul>
     * <li>Subscription responses
     * <li>RPC call responses
     * </ul>
     */
    public static boolean log = Boolean.getBoolean("log." + ProxyContext.class.getCanonicalName());

    /**
     * Controls logging of:
     * <ul>
     * <li>Inbound messages
     * </ul>
     */
    public static boolean logRx = Boolean.getBoolean("logRx." + ProxyContext.class.getCanonicalName());

    /**
     * Controls logging of:
     * <ul>
     * <li>Subscribes sent
     * <li>First update received
     * <ul>
     */
    public static boolean logVerboseSubscribes =
        Boolean.getBoolean("logVerboseSubscribes." + ProxyContext.class.getCanonicalName());

    /**
     * The default reconnect task scheduler used by all {@link ProxyContext} instances for reconnect
     * tasks
     */
    private final static ScheduledExecutorService RECONNECT_TASKS = ThreadUtils.newPermanentScheduledExecutorService(
        "fission-reconnect", DataFissionProperties.Values.RECONNECT_THREAD_COUNT);

    /** Acknowledges the successful completion of a subscription */
    static final String ACK = ContextUtils.PROTOCOL_PREFIX + "ACK_";
    /** Signals that a subscription is not OK (failed due to permissions or already subscribed) */
    static final String NOK = ContextUtils.PROTOCOL_PREFIX + "NOK_";
    private static final int ACK_LEN = ACK.length();

    static final String SUBSCRIBE = "subscribe";
    static final String UNSUBSCRIBE = "unsubscribe";
    
    static final AtomicLong MESSAGES_RECEIVED = new AtomicLong(); 
    static final AtomicLong BYTES_RECEIVED = new AtomicLong();
    
    private static final CountDownLatch DEFAULT_COUNT_DOWN_LATCH = new CountDownLatch(0);

    private static final int MINIMUM_RECONNECT_PERIOD_MILLIS = 50;

    /**
     * Encapsulates all the remote system records. These are effectively the system records in a
     * remote context.
     * 
     * @author Ramon Servadei
     */
    public static interface IRemoteSystemRecordNames
    {
        String REMOTE = "Remote";

        /**
         * This record shows the context record of the remote context. Use this to find out what
         * records exist in the remote context.
         * <p>
         * Use {@link #getRecord(String)} to obtain the remote context record using this string.
         * 
         * @see ISystemRecordNames#CONTEXT_RECORDS
         */
        String REMOTE_CONTEXT_RECORDS = REMOTE + ISystemRecordNames.CONTEXT_RECORDS;

        /**
         * This record shows the context subscriptions of the remote context. Use this to find out
         * what records are being subscribed for in the remote context.
         * <p>
         * Use {@link #getRecord(String)} to obtain the remote context subscriptions using this
         * string.
         * 
         * @see ISystemRecordNames#CONTEXT_SUBSCRIPTIONS
         */
        String REMOTE_CONTEXT_SUBSCRIPTIONS = REMOTE + ISystemRecordNames.CONTEXT_SUBSCRIPTIONS;

        /**
         * This record shows the available RPCs in the remote context.
         * <p>
         * Use {@link #getRecord(String)} to obtain the available RPCs in the remote context using
         * this string.
         * 
         * @see ISystemRecordNames#CONTEXT_RPCS
         */
        String REMOTE_CONTEXT_RPCS = REMOTE + ISystemRecordNames.CONTEXT_RPCS;

        /**
         * This record shows the connections of the remote context.
         * <p>
         * Use {@link #getRecord(String)} to obtain the remote context connections using this
         * string.
         * 
         * @see ISystemRecordNames#CONTEXT_CONNECTIONS
         */
        String REMOTE_CONTEXT_CONNECTIONS = REMOTE + ISystemRecordNames.CONTEXT_CONNECTIONS;
    }

    /**
     * <pre>
     * KEY=record name
     * VALUE="CONNECTED" or "DISCONNECTED"
     * </pre>
     * 
     * A special record that holds the connection status per record subscribed from the remote
     * context. The keys are the record names, the value is a string indicating the connection
     * status for the record, one of: {@link #RECORD_CONNECTED} or {@link #RECORD_DISCONNECTED}.
     * <p>
     * There are occasions where a record will have a different connection status to other records
     * from the same remote context - typically during a record re-sync operation.
     * 
     * @see ProxyContext#isRecordConnected(String)
     */
    public static final String RECORD_CONNECTION_STATUS_NAME = "RecordConnectionStatus";
    /**
     * The value used to indicate a record is connected in the
     * {@link #RECORD_CONNECTION_STATUS_NAME} record
     */
    public static final TextValue RECORD_CONNECTED = TextValue.valueOf("CONNECTED");
    /**
     * The value used to indicate a record is (re)connecting in the
     * {@link #RECORD_CONNECTION_STATUS_NAME} record
     */
    public static final TextValue RECORD_CONNECTING = TextValue.valueOf("CONNECTING");
    /**
     * The value used to indicate a record is disconnected in the
     * {@link #RECORD_CONNECTION_STATUS_NAME} record
     */
    public static final TextValue RECORD_DISCONNECTED = TextValue.valueOf("DISCONNECTED");

    final static Executor SYNCHRONOUS_EXECUTOR = new Executor()
    {
        @Override
        public void execute(Runnable command)
        {
            command.run();
        }
    };

    // constructs to handle mapping of local system record names to remote names
    static final Map<String, String> remoteToLocalSystemRecordNameConversions;
    static final Map<String, String> localToRemoteSystemRecordNameConversions;

    static
    {
        Map<String, String> mapping = null;
        mapping = new HashMap<String, String>();
        mapping.put(IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS, ISystemRecordNames.CONTEXT_RPCS);
        mapping.put(IRemoteSystemRecordNames.REMOTE_CONTEXT_RECORDS, ISystemRecordNames.CONTEXT_RECORDS);
        mapping.put(IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS, ISystemRecordNames.CONTEXT_CONNECTIONS);
        mapping.put(IRemoteSystemRecordNames.REMOTE_CONTEXT_SUBSCRIPTIONS, ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        remoteToLocalSystemRecordNameConversions = Collections.unmodifiableMap(mapping);

        mapping = new HashMap<String, String>();
        mapping.put(ISystemRecordNames.CONTEXT_RPCS, IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
        mapping.put(ISystemRecordNames.CONTEXT_RECORDS, IRemoteSystemRecordNames.REMOTE_CONTEXT_RECORDS);
        mapping.put(ISystemRecordNames.CONTEXT_CONNECTIONS, IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS);
        mapping.put(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS, IRemoteSystemRecordNames.REMOTE_CONTEXT_SUBSCRIPTIONS);
        localToRemoteSystemRecordNameConversions = Collections.unmodifiableMap(mapping);
    }

    static String substituteRemoteNameWithLocalName(final String name)
    {
        if (name.startsWith(IRemoteSystemRecordNames.REMOTE, 0))
        {
            return remoteToLocalSystemRecordNameConversions.get(name);
        }
        else
        {
            return name;
        }
    }

    static String substituteLocalNameWithRemoteName(final String name)
    {
        if (name.startsWith(ISystemRecordNames.CONTEXT, 0))
        {
            return localToRemoteSystemRecordNameConversions.get(name);
        }
        else
        {
            return name;
        }
    }

    static String[] getEligibleRecords(SubscriptionManager<String, IRecordListener> subscriptionManager, int count,
        String... recordNames)
    {
        final List<String> records = new ArrayList<String>(recordNames.length);
        for (int i = 0; i < recordNames.length; i++)
        {
            if (!ContextUtils.isSystemRecordName(recordNames[i])
                && !RECORD_CONNECTION_STATUS_NAME.equals(recordNames[i]))
            {
                if (subscriptionManager.getSubscribersFor(recordNames[i]).length == count)
                {
                    records.add(substituteRemoteNameWithLocalName(recordNames[i]));
                }
            }
        }
        return records.toArray(new String[records.size()]);
    }

    static String[] insertPermissionToken(final String permissionToken, final String[] recordsToSubscribeFor)
    {
        // insert the permission token
        final String[] permissionAndRecords = new String[recordsToSubscribeFor.length + 1];
        System.arraycopy(recordsToSubscribeFor, 0, permissionAndRecords, 1, recordsToSubscribeFor.length);
        permissionAndRecords[0] =
            (permissionToken == null ? IPermissionFilter.DEFAULT_PERMISSION_TOKEN : permissionToken);
        return permissionAndRecords;
    }

    /**
     * Handles RPC results
     * 
     * @author Ramon Servadei
     */
    private final class RpcResultHandler implements ISequentialRunnable
    {
        private final IRecordChange changeToApply;
        private final String changeName;

        RpcResultHandler(IRecordChange changeToApply, String changeName)
        {
            this.changeToApply = changeToApply;
            this.changeName = changeName;
        }

        @Override
        public void run()
        {
            if (log)
            {
                if (!logRx)
                {
                    Log.log(ProxyContext.this, "(<-) RPC result ", ObjectUtils.safeToString(this.changeToApply));
                }
            }
            final IRecordListener[] subscribersFor =
                ProxyContext.this.context.recordObservers.getSubscribersFor(this.changeName);
            IRecordListener iAtomicChangeObserver = null;
            long start;
            final int size = subscribersFor.length;
            if (size == 0)
            {
                Log.log(ProxyContext.this, "*** Unexpected RPC result for ", this.changeName);
            }
            for (int i = 0; i < size; i++)
            {
                try
                {
                    iAtomicChangeObserver = subscribersFor[i];
                    start = System.nanoTime();
                    iAtomicChangeObserver.onChange(null, this.changeToApply);
                    ContextUtils.measureTask(this.changeName, "remote record update", iAtomicChangeObserver,
                        (System.nanoTime() - start));
                }
                catch (Exception e)
                {
                    Log.log(ProxyContext.this,
                        "Could not notify " + iAtomicChangeObserver + " with " + this.changeToApply, e);
                }
            }
        }

        @Override
        public Object context()
        {
            return this.changeName;
        }
    }

    /**
     * The receiver for handling socket events and data for the {@link ProxyContext}
     * 
     * @author Ramon Servadei
     */
    private static final class ProxyContextReceiver implements IReceiver
    {
        final Object receiverToken;
        final ProxyContext proxyContext;

        ITransportChannel localChannelRef;
        boolean codecSyncExpected = true;

        ProxyContextReceiver(Object newToken, ProxyContext proxyContext)
        {
            this.receiverToken = newToken;
            this.proxyContext = proxyContext;
        }

        @Override
        public void onChannelConnected(final ITransportChannel channel)
        {
            // check the channel is the currently active one - failed re-connects can
            // have a channel with the same receiver but we must ignore events from it
            // as it was a previous (failed) attempt
            if (this.proxyContext.channelToken == this.receiverToken)
            {
                this.proxyContext.executeSequentialCoreTask(new ISequentialRunnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            ProxyContextReceiver.this.localChannelRef = channel;

                            // clear records before dispatching further messages (this assumes
                            // single-threaded dispatching)
                            ContextUtils.clearNonSystemRecords(ProxyContextReceiver.this.proxyContext.context);

                            // when connecting, initialise a new codec instance to reset any state
                            // held by the previous codec
                            ProxyContextReceiver.this.proxyContext.codec =
                                ProxyContextReceiver.this.proxyContext.codec.newInstance();
                            ProxyContextReceiver.this.proxyContext.codec.getSessionProtocol().setSessionListener(
                                ProxyContextReceiver.this.proxyContext.sessionListener);

                            // proxy initiates the codec-sync operation
                            // THIS MUST BE THE FIRST MESSAGE SENT

                            ProxyContextReceiver.this.localChannelRef.send(
                                ProxyContextReceiver.this.proxyContext.codec.getSessionProtocol().getSessionSyncStartMessage(
                                    ProxyContextReceiver.this.proxyContext.sessionContextName));
                            Log.log(ProxyContextReceiver.this.proxyContext, "(->) START SESSION SYNC ",
                                ObjectUtils.safeToString(ProxyContextReceiver.this.localChannelRef));
                        }
                        catch (Exception e)
                        {
                            channel.destroy("Could not intialise session", e);
                        }
                    }

                    @Override
                    public Object context()
                    {
                        return ProxyContextReceiver.this.proxyContext;
                    }
                });
            }
        }

        @Override
        public void onDataReceived(final ByteBuffer data, final ITransportChannel source)
        {
            // NOTE: channelToken is volatile so will slow message handling speed...but
            // there is no alternative - a local flag is not an option - setting it
            // during onChannelConnected is not guaranteed to work as that can happen on
            // a different thread
            // todo is this pattern still needed?
            if (this.proxyContext.channelToken == this.receiverToken)
            {
                this.proxyContext.executeSequentialCoreTask(RX_FRAME_HANDLER_POOL.get().initialise(data, source, this));
            }
        }

        @Override
        public void onChannelClosed(ITransportChannel channel)
        {
            if (this.proxyContext.channelToken == this.receiverToken)
            {
                this.proxyContext.executeSequentialCoreTask(new ISequentialRunnable()
                {
                    @Override
                    public void run()
                    {
                        ProxyContextReceiver.this.proxyContext.codec.getSessionProtocol().destroy();
                        ProxyContextReceiver.this.proxyContext.onChannelClosed();
                    }

                    @Override
                    public Object context()
                    {
                        return ProxyContextReceiver.this.proxyContext;
                    }
                });
            }
        }
    }

    /**
     * Handles the received TCP frame data. If the data is an atomic change, this will be passed to
     * a {@link RxAtomicChangeHandler}
     * 
     * @author Ramon Servadei
     */
    private static final class RxFrameHandler implements ISequentialRunnable
    {
        final RxAtomicChangeHandler atomicChangeHandler;
        ByteBuffer data;
        ITransportChannel source;
        ProxyContext proxyContext;
        ProxyContextReceiver receiver;

        RxFrameHandler()
        {
            this.atomicChangeHandler = new RxAtomicChangeHandler(this);
        }

        RxFrameHandler initialise(ByteBuffer data, ITransportChannel source, ProxyContextReceiver receiver)
        {
            this.data = data;
            this.source = source;
            this.receiver = receiver;
            if(this.receiver != null)
            {
                this.proxyContext = receiver.proxyContext;
            }
            return this;
        }

        void clear()
        {
            this.data = null;
            this.source = null;
            this.receiver = null;
            this.proxyContext = null;
            this.atomicChangeHandler.initialise(null, null, null);
        }
        
        @Override
        public void run()
        {
            boolean onDataReceivedCalled = false;
            try
            {
                BYTES_RECEIVED.addAndGet(this.data.limit() - this.data.position());
                if (this.receiver.codecSyncExpected)
                {
                    final SyncResponse response =
                        this.proxyContext.codec.getSessionProtocol().handleSessionSyncData(this.data);
                    if (!response.syncFailed)
                    {
                        Log.log(this.proxyContext, "(<-) SYNC RESP ", ObjectUtils.safeToString(this.source));
                        if (response.syncDataResponse != null)
                        {
                            this.receiver.localChannelRef.send(response.syncDataResponse);
                            Log.log(this.proxyContext, "(->) SYNC RESP ", ObjectUtils.safeToString(this.source));
                        }
                        if (response.syncComplete)
                        {
                            this.receiver.codecSyncExpected = false;
                            Log.log(this.proxyContext, "SESSION SYNCED ", ObjectUtils.safeToString(this.source));
                            this.proxyContext.onChannelConnected();
                        }
                    }
                    else
                    {
                        final String reason = "SESSION SYNC FAILED " + ObjectUtils.safeToString(this.source);
                        Log.log(this.proxyContext, reason, new IllegalStateException(reason));
                        this.proxyContext.destroy();
                    }
                    return;
                }

                try
                {
                    onDataReceivedCalled = this.proxyContext.onDataReceived(this);
                }
                catch (IncorrectSequenceException e)
                {
                    final String recordName = ProxyContext.substituteLocalNameWithRemoteName(
                        AtomicChangeTeleporter.getRecordName(e.recordName));
                    if (this.proxyContext.resync(recordName))
                    {
                        Log.log(this.receiver,
                            "Resync of " + recordName + " started due to error processing received change", e);
                    }
                    else
                    {
                        Log.log(this.receiver, "Resync of ", recordName,
                            " in progress, error handling previous unsynced change: ", e.toString(),
                            " (this should be resolved when the next image is received)");
                    }
                }
            }
            finally
            {
                if (!onDataReceivedCalled)
                {
                    free();
                }
            }
        }

        void free()
        {
            ByteArrayPool.offer(this.data.array());
            RX_FRAME_HANDLER_POOL.offer(this);
        }

        @Override
        public Object context()
        {
            return this.proxyContext;
        }
    }

    /**
     * Handles a single {@link IRecordChange} created from a received frame decoded from a
     * {@link RxFrameHandler}
     * 
     * @author Ramon Servadei
     */
    private final static class RxAtomicChangeHandler implements ISequentialRunnable
    {
        final RxFrameHandler frameHandler;
        String changeName;
        ProxyContext proxyContext;
        IRecordChange changeToApply;

        RxAtomicChangeHandler(RxFrameHandler rxFrameHandler)
        {
            this.frameHandler = rxFrameHandler;
        }

        RxAtomicChangeHandler initialise(String changeName, IRecordChange changeToApply, ProxyContext proxyContext)
        {
            this.changeName = changeName;
            this.proxyContext = proxyContext;
            this.changeToApply = changeToApply;
            return this;
        }

        @Override
        public void run()
        {
            try
            {
                MESSAGES_RECEIVED.incrementAndGet();
                final String name = substituteLocalNameWithRemoteName(this.changeName);

                final boolean isImage = this.changeToApply.getScope() == IRecordChange.IMAGE_SCOPE_CHAR;
                if (isImage)
                {
                    synchronized (this.proxyContext.resyncs)
                    {
                        if (this.proxyContext.resyncs.remove(name))
                        {
                            this.proxyContext.resyncInProgress = this.proxyContext.resyncs.size() > 0;
                        }
                    }
                }
                else
                {
                    // NOTE: this block is hit for every delta received, the resyncInProgress
                    // flag helps to optimise redundant hits to the resyncs.contains(name) call
                    if (this.proxyContext.resyncInProgress && this.proxyContext.resyncs.contains(name))
                    {
                        Log.log(this.proxyContext, "Ignoring delta change for record=", this.changeName,
                            ", resync in progress");
                        return;
                    }
                }

                // check if the record is subscribed
                final IRecordListener[] subscribers = this.proxyContext.context.recordObservers.getSubscribersFor(name);
                if (!(subscribers.length > 0))
                {
                    Log.log(this.proxyContext, "Received record but no subscription exists - ignoring record=",
                        this.changeName, " seq=", (isImage ? "i" : "d"),
                        Long.toString(this.changeToApply.getSequence()));
                    return;
                }

                IRecord record = this.proxyContext.context.getRecord(name);
                if (record == null)
                {
                    if (this.changeToApply.isEmpty())
                    {
                        // this creates the record AND notifies any listeners
                        // (publishAtomicChange would publish nothing)
                        record = this.proxyContext.context.createRecord(name);
                    }
                    else
                    {
                        record = this.proxyContext.context.createRecordSilently_callInRecordContext(name);
                    }
                }

                synchronized (record.getWriteLock())
                {
                    switch(this.proxyContext.imageDeltaProcessor.processRxChange(this.changeToApply, name, record))
                    {
                        case ImageDeltaChangeProcessor.PUBLISH:

                            // only images determine connection status - don't need to do this
                            // put for every delta that is received!
                            if (isImage && this.proxyContext.remoteConnectionStatusRecord.put(this.changeName,
                                RECORD_CONNECTED) != RECORD_CONNECTED)
                            {
                                this.proxyContext.context.publishAtomicChange(RECORD_CONNECTION_STATUS_NAME);
                            }

                            // note: use the record.getSequence() as this will be the DELTA
                            // sequence if an image was received and then cached deltas applied
                            // on top of it
                            this.proxyContext.context.setSequence(name, record.getSequence());
                            this.proxyContext.context.publishAtomicChange(name, false, subscribers);
                            break;
                        case ImageDeltaChangeProcessor.RESYNC:
                            this.proxyContext.resync(name);
                            break;
                    }
                }
            }
            catch (Exception e)
            {
                Log.log(this.proxyContext,
                    "Could not process received message " + ObjectUtils.safeToString(this.changeToApply), e);
            }
            finally
            {
                this.frameHandler.free();
            }
        }

        @Override
        public Object context()
        {
            return this.changeName;
        }
    }

    static final MultiThreadReusableObjectPool<RxFrameHandler> RX_FRAME_HANDLER_POOL =
        new MultiThreadReusableObjectPool<RxFrameHandler>("ProxyContext-RxFrameHandlerPool",
            new IReusableObjectBuilder<RxFrameHandler>()
            {
                @Override
                public RxFrameHandler newInstance()
                {
                    return new RxFrameHandler();
                }
            }, new IReusableObjectFinalizer<RxFrameHandler>()
            {
                @Override
                public void reset(RxFrameHandler instance)
                {
                    instance.clear();
                }
            }, DataFissionProperties.Values.PROXY_RX_FRAME_HANDLER_POOL_MAX_SIZE);

    final Object lock;
    volatile boolean active;
    volatile boolean connected;
    final Context context;
    ICodec<?> codec;
    final ImageDeltaChangeProcessor imageDeltaProcessor;
    ITransportChannel channel;
    ITransportChannelBuilderFactory channelBuilderFactory;

    /** @see #RECORD_CONNECTION_STATUS_NAME */
    final IRecord remoteConnectionStatusRecord;
    /** Signals if a reconnection is in progress */
    volatile ScheduledFuture reconnectTask;
    /**
     * The period in milliseconds to wait before trying a reconnect, default is
     * {@link Values#PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS}
     */
    int reconnectPeriodMillis = DataFissionProperties.Values.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS;
    /**
     * A flag that allows receivers to know whether their channel is still valid for the context;
     * when a socket disconnects, the receiver is still attached to the socket and the proxy - we
     * don't want this receiver to invoke events on the proxy as they are no longer valid.
     */
    volatile Object channelToken;
    /**
     * Tracks responses to subscription actions so that a subscription is known to have been
     * processed by the receiver.
     */
    final ConcurrentMap<String, Queue<CountDownLatch>> actionResponseLatches;
    /**
     * When all responses for a subscribe action are received, the future in this map is triggered.
     */
    final ConcurrentMap<CountDownLatch, RunnableFuture<?>> actionSubscribeFutures;
    /**
     * Holds the subscribe results per subscribe action.
     */
    final ConcurrentMap<CountDownLatch, Map<String, Boolean>> actionSubscribeResults;

    final AtomicChangeTeleporter teleportReceiver;
    /** The permission token used for subscribing each record */
    final Map<String, String> tokenPerRecord;
    EndPointAddress currentEndPoint;

    /** Tracks records that have a re-sync operation pending */
    final Set<String> resyncs;
    /** Flags if there is any record undergoing a resync */
    volatile boolean resyncInProgress;

    /** The name given to the "session" between this proxy and its remote context. */
    final String sessionContextName;
    final ISessionListener sessionListener;
    final NotifyingCache<ISessionListener, Pair<String, Boolean>> sessionCache;
    final Set<String> firstUpdateExpected;
    /**
     * Keeps an instance of each RPC that is requested so it can be used for cloning on any
     * subsequent requests for the same RPC.
     */
    final Map<String, RpcInstance> rpcTemplates;

    /**
     * Construct the proxy context and connect it to a {@link Publisher} using the specified host
     * and port.
     * <p>
     * This uses the transport technology defined by
     * {@link TransportTechnologyEnum#getDefaultFromSystemProperty()}
     * 
     * @param name
     *            the name for this proxy context - this is used by the remote context to identify
     *            this proxy
     * @param codec
     *            the codec to use for sending/receiving messages from the {@link Publisher}
     * @param publisherNode
     *            the end-point node of the publisher process
     * @param publisherPort
     *            the end-point port of the publisher process
     */
    public ProxyContext(String name, ICodec codec, final String publisherNode, final int publisherPort)
    {
        this(name, codec, publisherNode, publisherPort, TransportTechnologyEnum.getDefaultFromSystemProperty(), name);
    }

    /**
     * Construct the proxy context and connect it to a {@link Publisher} using the specified host,
     * port and transport technology
     * 
     * @param name
     *            the name for this proxy context - this is used by the remote context to identify
     *            this proxy
     * @param codec
     *            the codec to use for sending/receiving messages from the {@link Publisher}
     * @param publisherNode
     *            the end-point node of the publisher process
     * @param publisherPort
     *            the end-point port of the publisher process
     * @param transportTechnology
     *            the transport technology to use
     * @param sessionContextName
     *            the name given to the "session" between this proxy and its remote context.
     */
    public ProxyContext(String name, ICodec codec, final String publisherNode, final int publisherPort,
        TransportTechnologyEnum transportTechnology, String sessionContextName)
    {
        this(name, codec,
            transportTechnology.constructTransportChannelBuilderFactory(codec.getFrameEncodingFormat(),
                new StaticEndPointAddressFactory(new EndPointAddress(publisherNode, publisherPort))),
            sessionContextName);
    }

    public ProxyContext(String name, ICodec codec, ITransportChannelBuilderFactory channelBuilderFactory,
        String sessionContextName)
    {
        super();
        this.codec = codec;
        this.sessionContextName = sessionContextName;
        this.rpcTemplates = new ConcurrentHashMap<String, RpcInstance>();

        this.sessionListener = new ISessionListener()
        {
            @Override
            public void onSessionOpen(String sessionContext, String sessionId)
            {
                ProxyContext.this.sessionCache.notifyListenersDataAdded(sessionContext,
                    new Pair<String, Boolean>(sessionId, Boolean.TRUE));
            }

            @Override
            public void onSessionClosed(String sessionContext, String sessionId)
            {
                // NOTE: we use the ADDED events to trigger notification of listeners
                ProxyContext.this.sessionCache.notifyListenersDataAdded(sessionContext,
                    new Pair<String, Boolean>(sessionId, Boolean.FALSE));
            }
        };
        this.sessionCache = new NotifyingCache<ISessionListener, Pair<String, Boolean>>()
        {
            @Override
            protected void notifyListenerDataRemoved(ISessionListener listener, String key, Pair<String, Boolean> data)
            {
                // noop
            }

            @Override
            protected void notifyListenerDataAdded(ISessionListener listener, String key, Pair<String, Boolean> data)
            {
                if (data.getSecond().booleanValue())
                {
                    listener.onSessionOpen(key, data.getFirst());
                }
                else
                {
                    listener.onSessionClosed(key, data.getFirst());
                }
            }
        };
        this.context = new Context(name);
        this.lock = new Object();
        this.actionSubscribeFutures = new ConcurrentHashMap<CountDownLatch, RunnableFuture<?>>();
        this.actionSubscribeResults = new ConcurrentHashMap<CountDownLatch, Map<String, Boolean>>();
        this.actionResponseLatches = new ConcurrentHashMap<String, Queue<CountDownLatch>>();
        this.teleportReceiver = new AtomicChangeTeleporter(0);
        this.imageDeltaProcessor = new ImageDeltaChangeProcessor();
        this.tokenPerRecord = new ConcurrentHashMap<String, String>();
        this.resyncs = Collections.synchronizedSet(new HashSet<String>());
        // note: unsynchronized
        this.firstUpdateExpected = new HashSet<String>();

        // add default permissions for system records
        for (String recordName : ContextUtils.SYSTEM_RECORDS)
        {
            this.tokenPerRecord.put(recordName, IPermissionFilter.DEFAULT_PERMISSION_TOKEN);
        }

        this.remoteConnectionStatusRecord = this.context.createRecord(RECORD_CONNECTION_STATUS_NAME);
        this.context.createRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
        this.context.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord image, IRecordChange atomicChange)
            {
                updateRpcTemplates(atomicChange);
            }
        }, IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
        this.context.updateContextStatusAndPublishChange(Connection.DISCONNECTED);

        this.channelBuilderFactory = channelBuilderFactory;
        this.active = true;

        // this allows the ProxyContext to be constructed and reconnects asynchronously
        reconnect();
    }

    /**
     * Add a session listener to receive session status updates from this proxy
     * 
     * @param listener
     *            the session listener to add
     * @return <code>true</code> if added, <code>false</code> if already added
     */
    public boolean addSessionListener(ISessionListener listener)
    {
        return this.sessionCache.addListener(listener);
    }

    /**
     * Remove a previously added session listener
     * 
     * @param listener
     *            the listener to remove
     * @return <code>true</code> if removed
     */
    public boolean removeSessionListener(ISessionListener listener)
    {
        return this.sessionCache.removeListener(listener);
    }

    /**
     * @return the {@link EndPointAddress} used for the connection this proxy is using.
     *         <p>
     *         Note that the connection may be broken, use {@link #isConnected()} to determine this.
     *         Be aware that there may be a race condition if calling {@link #isConnected()} on a
     *         channel whilst the connection is happening, so just because {@link #isConnected()}
     *         returns <code>false</code> does not necessarily mean that the end point address is
     *         not available. Only when {@link #isConnected()} returns <code>true</code> can this
     *         end point address be guaranteed.
     */
    public EndPointAddress getEndPointAddress()
    {
        return this.currentEndPoint;
    }

    /**
     * @return the period in milliseconds to wait before trying a reconnect to the context
     * @see Values#PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS
     */
    public int getReconnectPeriodMillis()
    {
        return this.reconnectPeriodMillis;
    }

    /**
     * Set the period to wait before attempting to reconnect after the connection has been
     * unexpectedly broken.
     * 
     * @param reconnectPeriodMillis
     *            the period in milliseconds to wait before trying a reconnect to the context,
     *            cannot be less than 50
     * @see Values#PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS
     */
    public void setReconnectPeriodMillis(int reconnectPeriodMillis)
    {
        this.reconnectPeriodMillis = reconnectPeriodMillis < MINIMUM_RECONNECT_PERIOD_MILLIS
            ? MINIMUM_RECONNECT_PERIOD_MILLIS : reconnectPeriodMillis;
    }

    /**
     * This reconnects the proxy context to the new publisher - used when a publisher changes. If
     * the proxy context is currently connected, calling this method will close the current
     * connection and attempt to open a connection to the new publisher.
     * 
     * @param node
     *            the node (hostname) of the publisher
     * @param port
     *            the port (transport specific) for the publisher
     */
    public void reconnect(final String node, final int port)
    {
        setTransportChannelBuilderFactory(TransportChannelBuilderFactoryLoader.load(this.codec.getFrameEncodingFormat(),
            new EndPointAddress(node, port)));

        // force a reconnect
        this.channel.destroy("Forced reconnect");
    }

    /**
     * Set the channel builder factory to use - this overrides any existing one.
     */
    public void setTransportChannelBuilderFactory(ITransportChannelBuilderFactory channelBuilderFactory)
    {
        this.channelBuilderFactory = channelBuilderFactory;
    }

    /**
     * A convenience method to get the <b>image</b> of a record from the remote context. This may
     * cause a remote network operation if the record is not locally subscribed for so a timeout is
     * passed in.
     * 
     * @param recordName
     *            the record name to get
     * @param timeoutMillis
     *            the timeout in milliseconds for the operation
     * @return an immutable image of the record, <code>null</code> if the timeout expires or there
     *         is no record
     */
    public IRecord getRemoteRecordImage(final String recordName, long timeoutMillis)
    {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<IRecord> image = new AtomicReference<IRecord>();
        final IRecordListener observer = new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                if (latch.getCount() != 0)
                {
                    image.set(ImmutableSnapshotRecord.create(imageCopy));
                    if (imageCopy.size() > 0)
                    {
                        latch.countDown();
                    }
                }
            }
        };
        addObserver(observer, recordName);
        try
        {
            if (!latch.await(timeoutMillis, TimeUnit.MILLISECONDS))
            {
                Log.log(this, "Got no response to getRemoteRecordImage for: ", recordName, " after waiting ",
                    Long.toString(timeoutMillis), "ms");
            }
        }
        catch (InterruptedException e)
        {
            Log.log(this, "Got interrupted whilst waiting for record: " + recordName, e);
        }
        finally
        {
            removeObserver(observer, recordName);
        }
        return image.get();
    }

    @Override
    public String toString()
    {
        return "ProxyContext [" + this.context.getName() + " subscriptions="
            + this.context.getSubscribedRecords().size() + (this.active ? " active " : " inactive ")
            + (this.connected ? " connected " : " disconnected ") + getChannelString() + "]";
    }

    @Override
    public IRecord getRecord(String name)
    {
        IRecord record = this.context.getRecord(name);
        if (record == null)
        {
            return null;
        }
        return record.getImmutableInstance();
    }

    @Override
    public void resubscribe(String... recordNames)
    {
        synchronized (this.lock)
        {
            ContextUtils.resubscribeRecordsForContext(this, this.context.recordObservers, this.tokenPerRecord,
                recordNames);
        }
    }

    @Override
    public Set<String> getRecordNames()
    {
        return this.context.getRecordNames();
    }

    @Override
    public Future<Map<String, Boolean>> addObserver(IRecordListener observer, String... recordNames)
    {
        return addObserver(IPermissionFilter.DEFAULT_PERMISSION_TOKEN, observer, recordNames);
    }

    @Override
    public Future<Map<String, Boolean>> addObserver(final String permissionToken, IRecordListener observer,
        String... recordNames)
    {
        final Map<String, Boolean> subscribeResults = new HashMap<String, Boolean>(recordNames.length);

        final RunnableFuture<Map<String, Boolean>> futureResult = new FutureTask<Map<String, Boolean>>(new Runnable()
        {
            @Override
            public void run()
            {
            }
        }, subscribeResults);

        synchronized (this.lock)
        {
            // find records that have 0 observers - these are the ones that will need a subscription
            // message sent to the remote
            final String[] recordsToSubscribeFor = getEligibleRecords(this.context.recordObservers, 0, recordNames);

            // always observe the record even if we did not send the subscribe message - it may be a
            // local system record that was being subscribed for or a second listener added to the
            // same remote record
            this.context.addObserver(observer, recordNames);

            // store the token used for the record incase a reconnect happens and we need to
            // re-subscribe
            for (String recordName : recordsToSubscribeFor)
            {
                this.tokenPerRecord.put(recordName, permissionToken);
            }

            if (recordsToSubscribeFor.length > 0)
            {
                final Runnable task;
                // only issue the subscribe if connected
                if (this.connected)
                {
                    task = new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            subscribe(permissionToken, recordsToSubscribeFor);
                        }
                    };
                }
                else
                {
                    // use a no-op to execute the subscribe task as we need a latch
                    task = new Runnable()
                    {
                        @Override
                        public void run()
                        {
                        }
                    };
                }

                executeTask(recordsToSubscribeFor, SUBSCRIBE, task, futureResult, subscribeResults);
            }
            else
            {
                futureResult.run();
            }
        }
        return futureResult;
    }

    @Override
    public CountDownLatch removeObserver(IRecordListener observer, String... recordNames)
    {
        CountDownLatch latch = DEFAULT_COUNT_DOWN_LATCH;

        synchronized (this.lock)
        {
            this.context.removeObserver(observer, recordNames);

            final String[] recordsToUnsubscribe = getEligibleRecords(this.context.recordObservers, 0, recordNames);
            if (recordsToUnsubscribe.length > 0)
            {
                // only send an unsubscribe if connected
                if (this.connected)
                {
                    final Runnable task = new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            unsubscribe(recordsToUnsubscribe);
                        }
                    };
                    latch = executeTask(recordsToUnsubscribe, UNSUBSCRIBE, task, null, null);
                }

                // remove the records that are no longer subscribed
                String recordName;
                for (int i = 0; i < recordsToUnsubscribe.length; i++)
                {
                    recordName = recordsToUnsubscribe[i];

                    // ignore system record names - these can be in here because if we subscribe for
                    // RemoteContextRpcs, say, we actually send ContextRpcs (we need the ContextRpcs
                    // of the remote context).
                    if (!ContextUtils.isSystemRecordName(recordName))
                    {
                        this.context.removeRecord(recordName);
                        this.tokenPerRecord.remove(recordName);
                    }
                }

                // mark the records as disconnected
                synchronized (this.remoteConnectionStatusRecord.getWriteLock())
                {
                    for (int i = 0; i < recordsToUnsubscribe.length; i++)
                    {
                        recordName = recordsToUnsubscribe[i];
                        this.remoteConnectionStatusRecord.put(recordName, RECORD_DISCONNECTED);
                    }
                    this.context.publishAtomicChange(RECORD_CONNECTION_STATUS_NAME);
                }
            }
        }

        return latch;
    }

    @Override
    public String getName()
    {
        return this.context.getName();
    }

    @Override
    public void destroy()
    {
        synchronized (this.lock)
        {
            if (this.active)
            {
                Log.log(this, "Destroying ", ObjectUtils.safeToString(this));
                this.active = false;
                cancelReconnectTask();
                // the channel can be null if destroying during a reconnection
                if (this.channel != null)
                {
                    this.channel.destroy(getShortName() + " destroyed");
                }
                else
                {
                    updateConnectionStatus(Connection.DISCONNECTED);
                }

                // destroy at the end so that connection notifications are received
                this.context.destroy();
            }
        }
    }

    @Override
    public boolean isActive()
    {
        return this.active;
    }

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        destroy();
    }

    ITransportChannel constructChannel() throws IOException
    {
        final Object newToken = new Object();
        this.channelToken = newToken;

        final IReceiver receiver = new ProxyContextReceiver(newToken, this);

        final ITransportChannelBuilder channelBuilder = this.channelBuilderFactory.nextBuilder();
        this.currentEndPoint = channelBuilder.getEndPointAddress();
        Log.log(this, "Constructing channel for ", getShortName(), " using ", ObjectUtils.safeToString(channelBuilder));
        final ITransportChannel channel = channelBuilder.buildChannel(receiver);
        return channel;

    }

    void onChannelConnected()
    {
        synchronized (ProxyContext.this.lock)
        {
            cancelReconnectTask();

            if (!ProxyContext.this.active)
            {
                ProxyContext.this.channel.destroy("ProxyContext not active");
                return;
            }

            // now identity the proxy with the publisher end
            finalEncodeAndSendToPublisher(ProxyContext.this.codec.getTxMessageForIdentify(getName()));

            ProxyContext.this.imageDeltaProcessor.reset();
            ProxyContext.this.teleportReceiver.reset();

            // update the connection status
            ProxyContext.this.context.updateContextStatusAndPublishChange(Connection.CONNECTED);

            final List<String> recordNames = ProxyContext.this.context.getSubscribedRecords();

            // remove any local system record subscriptions (the 'local' system records of
            // the remote context are subscribed for as RemoteContextXYZ, not ContextXYZ)
            recordNames.remove(RECORD_CONNECTION_STATUS_NAME);
            for (String systemRecordName : ContextUtils.SYSTEM_RECORDS)
            {
                recordNames.remove(systemRecordName);
            }

            // re-subscribe
            if (recordNames.size() > 0)
            {
                final String[] recordNamesToSubscribeFor = new String[recordNames.size()];
                int i = 0;
                for (String recordName : recordNames)
                {
                    recordNamesToSubscribeFor[i++] = (substituteRemoteNameWithLocalName(recordName));
                }
                doResubscribe(recordNamesToSubscribeFor);
            }

            ProxyContext.this.connected = true;
        }
    }

    boolean onDataReceived(RxFrameHandler frameHandler) throws IncorrectSequenceException
    {
        final IRecordChange changeToApply =
            this.teleportReceiver.combine((AtomicChange) this.codec.getAtomicChangeFromRxMessage(frameHandler.data));

        if (logRx)
        {
            Log.log(ProxyContext.this, "(<-) ", ObjectUtils.safeToString(changeToApply));
        }

        if (changeToApply == null)
        {
            return false;
        }

        // peek at the size before attempting the synchronize block
        if (logVerboseSubscribes && this.firstUpdateExpected.size() > 0)
        {
            synchronized (this.firstUpdateExpected)
            {
                if (this.firstUpdateExpected.size() > 0)
                {
                    if (this.firstUpdateExpected.remove(changeToApply.getName()))
                    {
                        Log.log(ProxyContext.this, "(<-) first update from [", this.channel.getEndPointDescription(),
                            "] for [", changeToApply.getName() + "]");
                    }
                }
            }
        }

        final String changeName = changeToApply.getName();

        if (changeName.charAt(0) == ContextUtils.PROTOCOL_PREFIX)
        {
            final Boolean subscribeResult = Boolean.valueOf(changeName.startsWith(ACK, 0));
            if (subscribeResult.booleanValue() || changeName.startsWith(NOK, 0))
            {
                handleSubscribeResult(changeToApply, changeName, subscribeResult);

                // EARLY RETURN
                return false;
            }
            else if (changeName.startsWith(RpcInstance.RPC_RECORD_RESULT_PREFIX, 0))
            {
                // RPC results must be handled by a dedicated thread
                this.context.executeRpcTask(new RpcResultHandler(changeToApply, changeName));

                // EARLY RETURN
                return false;
            }
        }

        executeSequentialCoreTask(frameHandler.atomicChangeHandler.initialise(changeName, changeToApply, this));

        return true;
    }

    private void handleSubscribeResult(final IRecordChange changeToApply, final String changeName,
        final Boolean subscribeResult)
    {
        if (log)
        {
            if (!logRx)
            {
                Log.log(this, "(<-) ", ObjectUtils.safeToString(changeToApply));
            }
        }

        final List<String> recordNames = new ArrayList<String>(changeToApply.getPutEntries().keySet());
        final String action = changeName.substring(ACK_LEN);
        // always ensure a NOK is logged
        if (!log && !logRx && !subscribeResult.booleanValue())
        {
            Log.log(this, "(<-) ", SUBSCRIBE, NOK, ObjectUtils.safeToString(recordNames));
        }
        Queue<CountDownLatch> latches;
        String recordName;
        final int recordNameCount = recordNames.size();
        for (int i = 0; i < recordNameCount; i++)
        {
            recordName = recordNames.get(i);
            latches = this.actionResponseLatches.remove(action + recordName);
            if (latches != null)
            {
                for (CountDownLatch latch : latches)
                {
                    if (latch != null)
                    {
                        latch.countDown();
                        if (action.equals(SUBSCRIBE))
                        {
                            this.actionSubscribeResults.get(latch).put(recordName, subscribeResult);
                            // if all responses have been received...
                            if (latch.getCount() == 0)
                            {
                                this.actionSubscribeResults.remove(latch);
                                this.actionSubscribeFutures.remove(latch).run();
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Performs a data re-sync of the named record. A re-sync operation differs from a
     * {@link #resubscribe(String...)} in that a re-sync may not trigger a notification to observing
     * {@link IRecordListener} instances when the re-sync receives its image. A re-subscribe, on the
     * other hand, will always trigger a notification as it does a complete tear-down then
     * re-subscribe.
     * 
     * @param name
     *            the record to re-sync.
     * @return <code>true</code> if the resync was sent, <code>false</code> if there is already a
     *         resync in progress
     */
    boolean resync(final String name)
    {
        final boolean resyncNeeded;
        synchronized (this.resyncs)
        {
            resyncNeeded = this.resyncs.add(name);
            this.resyncInProgress = this.resyncs.size() > 0;
        }
        if (resyncNeeded)
        {
            // mark the record as disconnected, then reconnecting
            synchronized (this.remoteConnectionStatusRecord.getWriteLock())
            {
                this.remoteConnectionStatusRecord.put(name, RECORD_DISCONNECTED);
                this.context.publishAtomicChange(RECORD_CONNECTION_STATUS_NAME);
                this.remoteConnectionStatusRecord.put(name, RECORD_CONNECTING);
                this.context.publishAtomicChange(RECORD_CONNECTION_STATUS_NAME);
            }

            Log.log(this, "(->) re-sync to ", getEndPoint(), " for [", name, "]");
            if (logVerboseSubscribes)
            {
                synchronized (this.firstUpdateExpected)
                {
                    this.firstUpdateExpected.add(name);
                }
            }
            finalEncodeAndSendToPublisher(ProxyContext.this.codec.getTxMessageForResync(
                new String[] { substituteRemoteNameWithLocalName(name) }));

            return true;
        }
        else
        {
            if (log)
            {
                Log.log(this, "Ignoring duplicate re-sync for ", name);
            }

            return false;
        }
    }

    void onChannelClosed()
    {
        this.channelToken = null;
        this.connected = false;
        updateConnectionStatus(Connection.DISCONNECTED);

        if (this.active)
        {
            setupReconnectTask();
        }
    }

    @Override
    public IRpcInstance getRpc(final String name)
    {
        final IValue definition = this.context.getRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS).get(name);
        if (definition == null)
        {
            this.rpcTemplates.remove(name);
            return null;
        }

        RpcInstance instance = this.rpcTemplates.get(name);
        if (instance == null)
        {
            instance = RpcInstance.constructInstanceFromDefinition(name, definition.textValue());
            this.rpcTemplates.put(name, instance);
            Log.log(this, "Created RPC template: ", instance.toString());
        }

        instance = instance.clone();
        instance.setHandler(new RpcInstance.Remote.Caller(name, this.codec, this.channel, this.context,
            instance.remoteExecutionStartTimeoutMillis, instance.remoteExecutionDurationTimeoutMillis));
        return instance;
    }

    /**
     * @return <code>true</code> if the proxy is connected to the remote context
     */
    public boolean isConnected()
    {
        return this.channel != null && this.channel.isConnected();
    }

    public String getChannelString()
    {
        return ObjectUtils.safeToString(this.channel);
    }

    @Override
    public ScheduledExecutorService getUtilityExecutor()
    {
        return this.context.getUtilityExecutor();
    }

    void updateRpcTemplates(IRecordChange atomicChange)
    {
        // remove RPC templates
        Map<String, IValue> entries = atomicChange.getRemovedEntries();
        if (entries.size() > 0)
        {
            for (String rpcName : entries.keySet())
            {
                this.rpcTemplates.remove(rpcName);
            }
        }

        // remove updated RPC templates (update them from the put entries)
        entries = atomicChange.getOverwrittenEntries();
        if (entries.size() > 0)
        {
            for (String rpcName : entries.keySet())
            {
                this.rpcTemplates.remove(rpcName);
            }
        }

        // add/update RPC templates
        entries = atomicChange.getPutEntries();
        if (entries.size() > 0)
        {
            for (String rpcName : entries.keySet())
            {
                getRpc(rpcName);
            }
        }
    }

    void cancelReconnectTask()
    {
        synchronized (this.lock)
        {
            if (this.reconnectTask != null)
            {
                this.reconnectTask.cancel(false);
                ProxyContext.this.reconnectTask = null;
            }
        }
    }

    void setupReconnectTask()
    {
        synchronized (this.lock)
        {
            if (!this.active)
            {
                Log.log(this, "Not setting up reconnect task for ", ObjectUtils.safeToString(this));
                return;
            }
            if (this.reconnectTask != null)
            {
                Log.log(this, "Reconnect still pending for ", getEndPoint());
                return;
            }

            // Remove RPCs
            final IRecord rpcRecord = this.context.getRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
            if (rpcRecord.size() > 0)
            {
                Log.log(this, "Removing RPCs ", ObjectUtils.safeToString(rpcRecord.keySet()), " from ", getShortName());
                synchronized (this.context.getRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS).getWriteLock())
                {
                    rpcRecord.clear();
                    this.rpcTemplates.clear();
                    this.context.publishAtomicChange(rpcRecord);
                }
            }

            Log.log(this, "Scheduling reconnect for ", getShortName(), " to ", getEndPoint(), " in ",
                Long.toString(this.reconnectPeriodMillis), "ms ");

            this.reconnectTask = RECONNECT_TASKS.schedule(new Runnable()
            {
                @Override
                public void run()
                {
                    reconnect();
                }
            }, this.reconnectPeriodMillis, TimeUnit.MILLISECONDS);
        }
    }

    String getEndPoint()
    {
        return "[" + (this.channel == null ? "not connected yet" : this.channel.getEndPointDescription()) + "]";
    }

    String getShortName()
    {
        return "ProxyContext [" + this.context.getName() + "]";
    }

    /**
     * Updates the connection status of all subscribed records and the ContextStatus record.
     */
    void updateConnectionStatus(Connection connectionStatus)
    {
        this.context.updateContextStatusAndPublishChange(connectionStatus);

        TextValue status = null;
        switch(connectionStatus)
        {
            case CONNECTED:
                status = RECORD_CONNECTED;
                break;
            case DISCONNECTED:
                status = RECORD_DISCONNECTED;
                break;
            case RECONNECTING:
                status = RECORD_CONNECTING;
                break;
        }

        // NOTE: we need to process SUBSCRIBED records, not the records in the context
        final List<String> recordNames = this.context.getSubscribedRecords();
        recordNames.remove(RECORD_CONNECTION_STATUS_NAME);

        synchronized (this.remoteConnectionStatusRecord.getWriteLock())
        {
            for (String recordName : recordNames)
            {
                try
                {
                    this.remoteConnectionStatusRecord.put(recordName, status);
                }
                catch (Exception e)
                {
                    Log.log(this,
                        "Could not update record status " + recordName + " to " + ObjectUtils.safeToString(status), e);
                }
            }
            this.context.publishAtomicChange(RECORD_CONNECTION_STATUS_NAME);
        }
    }

    void reconnect()
    {
        synchronized (this.lock)
        {
            this.connected = false;
            this.reconnectTask = null;

            if (!this.active)
            {
                Log.log(this, "Not reconnecting proxy as it is not active: ", getShortName());
                return;
            }
            
            updateConnectionStatus(Connection.RECONNECTING);

            String endPoint = ObjectUtils.safeToString(this.channel);
            try
            {
                // reconstruct the channel
                this.channel = constructChannel();
            }
            catch (Exception e)
            {
                Log.log(ProxyContext.class, "Could not reconnect ", endPoint, " (", e.getMessage(), ")");
                onChannelClosed();
            }
        }
    }

    CountDownLatch executeTask(final String[] recordNames, final String action, final Runnable task,
        RunnableFuture<Map<String, Boolean>> subscribeFutureResult, Map<String, Boolean> subscribeResults)
    {
        CountDownLatch latch = new CountDownLatch(recordNames.length);
        Queue<CountDownLatch> latches;
        Queue<CountDownLatch> pending;
        for (int i = 0; i < recordNames.length; i++)
        {
            pending = new ConcurrentLinkedQueue<CountDownLatch>();
            latches = this.actionResponseLatches.putIfAbsent(action + recordNames[i], pending);
            if (latches == null)
            {
                latches = pending;
            }
            latches.add(latch);
        }

        if (subscribeFutureResult != null)
        {
            this.actionSubscribeResults.put(latch, subscribeResults);
            this.actionSubscribeFutures.put(latch, subscribeFutureResult);
        }

        task.run();
        return latch;
    }

    /**
     * @return a short-hand string describing both connection points of the {@link TcpChannel} of
     *         this proxy
     */
    public String getShortSocketDescription()
    {
        return this.channel.getDescription();
    }

    @Override
    public List<String> getSubscribedRecords()
    {
        return this.context.getSubscribedRecords();
    }

    @Override
    public void executeSequentialCoreTask(ISequentialRunnable sequentialRunnable)
    {
        this.context.executeSequentialCoreTask(sequentialRunnable);
    }

    public boolean isRecordConnected(String recordName)
    {
        return RECORD_CONNECTED.equals(this.remoteConnectionStatusRecord.get(recordName));
    }

    void subscribe(final String permissionToken, final String[] recordsToSubscribeFor)
    {
        final int batchSize = DataFissionProperties.Values.SUBSCRIBE_BATCH_SIZE;
        List<String> batchSubscribeRecordNames = new ArrayList<String>(batchSize);
        final int size = recordsToSubscribeFor.length;
        int i;
        for (i = 0; i < size; i++)
        {
            if (batchSubscribeRecordNames.size() == batchSize)
            {
                subscribeBatch(permissionToken,
                    batchSubscribeRecordNames.toArray(new String[batchSubscribeRecordNames.size()]), i, size);
                batchSubscribeRecordNames = new ArrayList<String>(batchSize);
            }
            batchSubscribeRecordNames.add(recordsToSubscribeFor[i]);
        }
        if (batchSubscribeRecordNames.size() > 0)
        {
            subscribeBatch(permissionToken,
                batchSubscribeRecordNames.toArray(new String[batchSubscribeRecordNames.size()]), i, size);
        }
    }

    private void subscribeBatch(final String permissionToken, final String[] recordsToSubscribeFor, int current,
        int total)
    {
        int i;
        if (ProxyContext.this.channel instanceof ISubscribingChannel)
        {
            for (i = 0; i < recordsToSubscribeFor.length; i++)
            {
                ((ISubscribingChannel) ProxyContext.this.channel).contextSubscribed(recordsToSubscribeFor[i]);
            }
        }

        if (logVerboseSubscribes)
        {
            synchronized (this.firstUpdateExpected)
            {
                for (i = 0; i < recordsToSubscribeFor.length; i++)
                {
                    this.firstUpdateExpected.add(recordsToSubscribeFor[i]);
                }
            }
        }

        Log.log(this, "(->) subscribe to ", getEndPoint(), " (", Integer.toString(current), "/",
            Integer.toString(total), ")", (logVerboseSubscribes || total == 1 ? Arrays.toString(recordsToSubscribeFor) : ""));

        finalEncodeAndSendToPublisher(ProxyContext.this.codec.getTxMessageForSubscribe(
            insertPermissionToken(permissionToken, recordsToSubscribeFor)));
    }

    void unsubscribe(final String[] recordsToUnsubscribe)
    {
        final int batchSize = DataFissionProperties.Values.SUBSCRIBE_BATCH_SIZE;
        List<String> batchUnsubscribeRecordNames = new ArrayList<String>(batchSize);
        final int size = recordsToUnsubscribe.length;
        int i;
        for (i = 0; i < size; i++)
        {
            if (batchUnsubscribeRecordNames.size() == batchSize)
            {
                unsubscribeBatch(batchUnsubscribeRecordNames.toArray(new String[batchUnsubscribeRecordNames.size()]), i,
                    size);
                batchUnsubscribeRecordNames = new ArrayList<String>(batchSize);
            }
            batchUnsubscribeRecordNames.add(recordsToUnsubscribe[i]);
        }
        if (batchUnsubscribeRecordNames.size() > 0)
        {
            unsubscribeBatch(batchUnsubscribeRecordNames.toArray(new String[batchUnsubscribeRecordNames.size()]), i,
                size);
        }
    }

    private void unsubscribeBatch(final String[] recordsToUnsubscribe, int current, int total)
    {
        int i = 0;
        if (ProxyContext.this.channel instanceof ISubscribingChannel)
        {
            for (i = 0; i < recordsToUnsubscribe.length; i++)
            {
                ((ISubscribingChannel) ProxyContext.this.channel).contextUnsubscribed(recordsToUnsubscribe[i]);
            }
        }

        if (logVerboseSubscribes)
        {
            synchronized (this.firstUpdateExpected)
            {
                for (i = 0; i < recordsToUnsubscribe.length; i++)
                {
                    this.firstUpdateExpected.remove(recordsToUnsubscribe[i]);
                }
            }
        }

        Log.log(this, "(->) unsubscribe to ", getEndPoint(), " (", Integer.toString(current), "/",
            Integer.toString(total), ")", (logVerboseSubscribes || total == 1 ? Arrays.toString(recordsToUnsubscribe) : ""));

        finalEncodeAndSendToPublisher(ProxyContext.this.codec.getTxMessageForUnsubscribe(recordsToUnsubscribe));

        for (i = 0; i < recordsToUnsubscribe.length; i++)
        {
            ProxyContext.this.imageDeltaProcessor.unsubscribed(recordsToUnsubscribe[i]);
        }
    }

    void doResubscribe(final String[] recordNamesToSubscribeFor)
    {
        final Map<String, List<String>> recordsPerToken = new HashMap<String, List<String>>();
        String token = null;
        List<String> records;

        {
            String recordName;
            for (int i = 0; i < recordNamesToSubscribeFor.length; i++)
            {
                recordName = recordNamesToSubscribeFor[i];
                token = this.tokenPerRecord.get(recordName);
                records = recordsPerToken.get(token);
                if (records == null)
                {
                    records = new ArrayList<String>();
                    recordsPerToken.put(token, records);
                }
                records.add(recordName);
            }
        }

        Map.Entry<String, List<String>> entry = null;
        for (Iterator<Map.Entry<String, List<String>>> it = recordsPerToken.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            token = entry.getKey();
            records = entry.getValue();
            subscribe(token, records.toArray(new String[records.size()]));
        }
    }

    void finalEncodeAndSendToPublisher(byte[] data)
    {
        this.channel.send(this.codec.finalEncode(data));
    }
}