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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ISubscribingChannel;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.channel.ITransportChannelBuilder;
import com.fimtra.channel.ITransportChannelBuilderFactory;
import com.fimtra.channel.TransportChannelBuilderFactoryLoader;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.DataFissionProperties.Values;
import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IPermissionFilter;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.IStatusAttribute.Connection;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannel;
import com.fimtra.thimble.ISequentialRunnable;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.StringUtils;
import com.fimtra.util.SubscriptionManager;

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
    /** Acknowledges the successful completion of a subscription */
    static final String ACK = "_ACK_";
    /** Signals that a subscription is not OK (failed due to permissions or already subscribed) */
    static final String NOK = "_NOK_";
    static final String SUBSCRIBE = "subscribe";
    static final String UNSUBSCRIBE = "unsubscribe";
    static final String ACK_ACTION_ARGS_START = "?";
    static final char ACK_ARGS_DELIMITER = ',';

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
    public static final TextValue RECORD_CONNECTED = new TextValue("CONNECTED");
    /**
     * The value used to indicate a record is (re)connecting in the
     * {@link #RECORD_CONNECTION_STATUS_NAME} record
     */
    public static final TextValue RECORD_CONNECTING = new TextValue("CONNECTING");
    /**
     * The value used to indicate a record is disconnected in the
     * {@link #RECORD_CONNECTION_STATUS_NAME} record
     */
    public static final TextValue RECORD_DISCONNECTED = new TextValue("DISCONNECTED");

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
        List<String> records = new ArrayList<String>(recordNames.length);
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
        String[] permissionAndRecords = new String[recordsToSubscribeFor.length + 1];
        System.arraycopy(recordsToSubscribeFor, 0, permissionAndRecords, 1, recordsToSubscribeFor.length);
        permissionAndRecords[0] =
            (permissionToken == null ? IPermissionFilter.DEFAULT_PERMISSION_TOKEN : permissionToken);
        return permissionAndRecords;
    }

    final Lock lock;
    volatile boolean active;
    volatile boolean connected;
    final Context context;
    final ICodec codec;
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
     * <p>
     * TODO If there is no response for a subscription action, there is a memory leak as the list is
     * left in the map
     */
    final ConcurrentMap<String, List<CountDownLatch>> actionResponseLatches;
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

    /**
     * Construct the proxy context and connect it to a {@link Publisher} using the specified host
     * and port.
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
     * @throws IOException
     */
    public ProxyContext(String name, ICodec codec, final String publisherNode, final int publisherPort)
        throws IOException
    {
        this(name, codec, TransportChannelBuilderFactoryLoader.load(codec.getFrameEncodingFormat(),
            new EndPointAddress(publisherNode, publisherPort)));
    }

    public ProxyContext(String name, ICodec codec, ITransportChannelBuilderFactory channelBuilderFactory)
    {
        super();
        this.codec = codec;
        this.context = new Context(name);
        this.lock = new ReentrantLock();
        this.actionSubscribeFutures = new ConcurrentHashMap<CountDownLatch, RunnableFuture<?>>();
        this.actionSubscribeResults = new ConcurrentHashMap<CountDownLatch, Map<String, Boolean>>();
        this.actionResponseLatches = new ConcurrentHashMap<String, List<CountDownLatch>>();
        this.teleportReceiver = new AtomicChangeTeleporter(0);
        this.imageDeltaProcessor = new ImageDeltaChangeProcessor();
        this.tokenPerRecord = new ConcurrentHashMap<String, String>();
        // add default permissions for system records
        for (String recordName : ContextUtils.SYSTEM_RECORDS)
        {
            this.tokenPerRecord.put(recordName, IPermissionFilter.DEFAULT_PERMISSION_TOKEN);
        }

        this.remoteConnectionStatusRecord = this.context.createRecord(RECORD_CONNECTION_STATUS_NAME);
        this.context.createRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
        this.context.updateContextStatusAndPublishChange(Connection.DISCONNECTED);

        this.channelBuilderFactory = channelBuilderFactory;
        this.active = true;

        // this allows the ProxyContext to be constructed and reconnects asynchronously
        reconnect();
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
        this.reconnectPeriodMillis =
            reconnectPeriodMillis < MINIMUM_RECONNECT_PERIOD_MILLIS ? MINIMUM_RECONNECT_PERIOD_MILLIS
                : reconnectPeriodMillis;
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
        setTransportChannelBuilderFactory(TransportChannelBuilderFactoryLoader.load(
            this.codec.getFrameEncodingFormat(), new EndPointAddress(node, port)));

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
        this.lock.lock();
        try
        {
            ContextUtils.resubscribeRecordsForContext(this, this.context.recordObservers, this.tokenPerRecord,
                recordNames);
        }
        finally
        {
            this.lock.unlock();
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

        this.lock.lock();
        try
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
        }
        finally
        {
            this.lock.unlock();
        }
        return futureResult;

    }

    @Override
    public CountDownLatch removeObserver(IRecordListener observer, String... recordNames)
    {
        CountDownLatch latch = DEFAULT_COUNT_DOWN_LATCH;

        this.lock.lock();
        try
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
                            int i = 0;
                            if (ProxyContext.this.channel instanceof ISubscribingChannel)
                            {
                                for (i = 0; i < recordsToUnsubscribe.length; i++)
                                {
                                    ((ISubscribingChannel) ProxyContext.this.channel).contextUnsubscribed(recordsToUnsubscribe[i]);
                                }
                            }
                            ProxyContext.this.channel.sendAsync(ProxyContext.this.codec.getTxMessageForUnsubscribe(recordsToUnsubscribe));

                            for (i = 0; i < recordsToUnsubscribe.length; i++)
                            {
                                ProxyContext.this.imageDeltaProcessor.unsubscribed(recordsToUnsubscribe[i]);
                            }
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
                final Lock recordConnectionLock = this.context.getRecord(RECORD_CONNECTION_STATUS_NAME).getWriteLock();
                recordConnectionLock.lock();
                try
                {
                    for (int i = 0; i < recordsToUnsubscribe.length; i++)
                    {
                        recordName = recordsToUnsubscribe[i];
                        this.remoteConnectionStatusRecord.put(recordName, RECORD_DISCONNECTED);
                    }
                    this.context.publishAtomicChange(RECORD_CONNECTION_STATUS_NAME);
                }
                finally
                {
                    recordConnectionLock.unlock();
                }
            }
        }
        finally
        {
            this.lock.unlock();
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
        this.lock.lock();
        try
        {
            if (this.active)
            {
                Log.log(this, "Destroying ", ObjectUtils.safeToString(this));
                this.active = false;
                cancelReconnectTask();
                this.context.destroy();
                // the channel can be null if destroying during a reconnection
                if (this.channel != null)
                {
                    this.channel.destroy("ProxyContext destroyed");
                }
            }
        }
        finally
        {
            this.lock.unlock();
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

        final IReceiver receiver = new IReceiver()
        {
            final Object receiverToken = newToken;

            @Override
            public void onChannelConnected(ITransportChannel channel)
            {
                // check the channel is the currently active one - failed re-connects can have a
                // channel with the same receiver but we must ignore events from it as it was a
                // previous (failed) attempt
                if (ProxyContext.this.channelToken == this.receiverToken)
                {
                    // clear records before dispatching further messages (this assumes
                    // single-threaded dispatching)
                    ContextUtils.clearNonSystemRecords(ProxyContext.this.context);

                    ProxyContext.this.onChannelConnected();
                }
            }

            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                // NOTE: channelToken is volatile so will slow message handling speed...but there is
                // no alternative - a local flag is not an option - setting it during
                // onChannelConnected is not guaranteed to work as that can happen on a different
                // thread
                if (ProxyContext.this.channelToken == this.receiverToken)
                {
                    ProxyContext.this.onDataReceived(data);
                }
            }

            @Override
            public void onChannelClosed(ITransportChannel channel)
            {
                if (ProxyContext.this.channelToken == this.receiverToken)
                {
                    ProxyContext.this.onChannelClosed();
                }
            }
        };

        final ITransportChannelBuilder channelBuilder = this.channelBuilderFactory.nextBuilder();
        this.currentEndPoint = channelBuilder.getEndPointAddress();
        Log.log(this, "Constructing channel using ", ObjectUtils.safeToString(channelBuilder));
        final ITransportChannel channel = channelBuilder.buildChannel(receiver);
        channel.sendAsync(this.codec.getTxMessageForIdentify(getName()));
        return channel;
    }

    void onChannelConnected()
    {
        executeSequentialCoreTask(new ISequentialRunnable()
        {
            @Override
            public void run()
            {
                ProxyContext.this.lock.lock();
                try
                {
                    cancelReconnectTask();

                    if (!ProxyContext.this.active)
                    {
                        ProxyContext.this.channel.destroy("ProxyContext not active");
                        return;
                    }

                    // update the connection status
                    ProxyContext.this.context.updateContextStatusAndPublishChange(Connection.CONNECTED);

                    final Set<String> recordNames =
                        new HashSet<String>(ProxyContext.this.context.getSubscribedRecords());

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
                finally
                {
                    ProxyContext.this.lock.unlock();
                }
            }

            @Override
            public Object context()
            {
                return ProxyContext.this.context.getName();
            }
        });
    }

    void onDataReceived(byte[] data)
    {
        final IRecordChange changeToApply =
            this.teleportReceiver.combine((AtomicChange) this.codec.getAtomicChangeFromRxMessage(data));

        if (changeToApply == null)
        {
            return;
        }

        final String changeName = changeToApply.getName();
        if (changeName.startsWith(ACK, 0) || changeName.startsWith(NOK, 0))
        {
            Log.log(this, "(<-) ", changeName);

            final Boolean subscribeResult = Boolean.valueOf(changeName.startsWith(ACK, 0));

            final int startOfRecordNames = changeName.indexOf(ACK_ACTION_ARGS_START);
            final List<String> recordNames =
                StringUtils.split(
                    changeName.substring(startOfRecordNames + ACK_ACTION_ARGS_START.length(), changeName.length()),
                    ACK_ARGS_DELIMITER);
            final String action = changeName.substring(ACK.length(), startOfRecordNames);
            List<CountDownLatch> latches;
            for (String recordName : recordNames)
            {
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

            return;
        }

        if (changeName.startsWith(RpcInstance.RPC_RECORD_RESULT_PREFIX, 0))
        {
            // RPC results must be handled by a dedicated thread
            this.context.executeRpcTask(new ISequentialRunnable()
            {
                @Override
                public void run()
                {
                    Log.log(ProxyContext.this, "(<-) RPC result ", ObjectUtils.safeToString(changeToApply));
                    final IRecordListener[] subscribersFor =
                        ProxyContext.this.context.recordObservers.getSubscribersFor(changeName);
                    IRecordListener iAtomicChangeObserver = null;
                    long start;
                    final int size = subscribersFor.length;
                    if (size == 0)
                    {
                        Log.log(ProxyContext.this, "No RPC result expected");
                    }
                    for (int i = 0; i < size; i++)
                    {
                        try
                        {
                            iAtomicChangeObserver = subscribersFor[i];
                            start = System.nanoTime();
                            iAtomicChangeObserver.onChange(null, changeToApply);
                            ContextUtils.measureTask(changeName, "remote record update", iAtomicChangeObserver,
                                (System.nanoTime() - start));
                        }
                        catch (Exception e)
                        {
                            Log.log(ProxyContext.this, "Could not notify " + iAtomicChangeObserver + " with "
                                + changeToApply, e);
                        }
                    }
                }

                @Override
                public Object context()
                {
                    return changeName;
                }
            });
            return;
        }

        executeSequentialCoreTask(new ISequentialRunnable()
        {
            @Override
            public void run()
            {
                try
                {
                    final String name = substituteLocalNameWithRemoteName(changeName);

                    final boolean recordIsSubscribed =
                        ProxyContext.this.context.recordObservers.getSubscribersFor(name).length > 0;
                    if (!recordIsSubscribed)
                    {
                        Log.log(ProxyContext.this, "Received record but no subscription exists - ignoring ",
                            ObjectUtils.safeToString(changeToApply));
                        return;
                    }

                    IRecord record = ProxyContext.this.context.getRecord(name);
                    final boolean emptyChange = changeToApply.isEmpty();
                    if (record == null)
                    {
                        if (emptyChange)
                        {
                            // this creates the record AND notifies any listeners
                            // (publishAtomicChange would publish nothing)
                            record = ProxyContext.this.context.createRecord(name);
                        }
                        else
                        {
                            record = ProxyContext.this.context.createRecordSilently(name);
                        }
                    }

                    final Lock lock = record.getWriteLock();
                    lock.lock();
                    try
                    {
                        switch(ProxyContext.this.imageDeltaProcessor.processRxChange(changeToApply, name, record))
                        {
                            case ImageDeltaChangeProcessor.PUBLISH:

                                if (ProxyContext.this.remoteConnectionStatusRecord.put(changeName, RECORD_CONNECTED) != RECORD_CONNECTED)
                                {
                                    ProxyContext.this.context.publishAtomicChange(RECORD_CONNECTION_STATUS_NAME);
                                }

                                // note: use the record.getSequence() as this will be the DELTA
                                // sequence if an image was received and then cached deltas applied
                                // on top of it
                                ProxyContext.this.context.setSequence(name, record.getSequence());
                                ProxyContext.this.context.publishAtomicChange(name);
                                break;
                            case ImageDeltaChangeProcessor.RESYNC:
                                resync(name);
                                break;
                        }
                    }
                    finally
                    {
                        lock.unlock();
                    }
                }
                catch (Exception e)
                {
                    Log.log(ProxyContext.this,
                        "Could not process received message " + ObjectUtils.safeToString(changeToApply), e);
                }
            }

            @Override
            public Object context()
            {
                return changeName;
            }
        });
    }

    void resync(String name)
    {
        Log.log(this, "Re-syncing ", name);

        // mark the record as disconnected, then reconnecting
        Lock lock = this.context.getRecord(RECORD_CONNECTION_STATUS_NAME).getWriteLock();
        lock.lock();
        try
        {
            this.remoteConnectionStatusRecord.put(name, RECORD_DISCONNECTED);
            this.context.publishAtomicChange(RECORD_CONNECTION_STATUS_NAME);
            this.remoteConnectionStatusRecord.put(name, RECORD_CONNECTING);
            this.context.publishAtomicChange(RECORD_CONNECTION_STATUS_NAME);
        }
        finally
        {
            lock.unlock();
        }

        final String[] recordNames = new String[] { substituteRemoteNameWithLocalName(name) };
        ProxyContext.this.channel.sendAsync(ProxyContext.this.codec.getTxMessageForUnsubscribe(recordNames));
        ProxyContext.this.channel.sendAsync(ProxyContext.this.codec.getTxMessageForSubscribe(insertPermissionToken(
            this.tokenPerRecord.get(name), recordNames)));
    }

    void onChannelClosed()
    {
        this.channelToken = null;
        this.connected = false;

        if (!this.active)
        {
            return;
        }

        updateConnectionStatus(Connection.DISCONNECTED);

        Log.log(this, "Lost connection for ", ObjectUtils.safeToString(this), ", scheduling reconnect task");

        executeSequentialCoreTask(new ISequentialRunnable()
        {
            @Override
            public void run()
            {
                setupReconnectTask();
            }

            @Override
            public Object context()
            {
                return ProxyContext.this.getName();
            }
        });
    }

    @Override
    public IRpcInstance getRpc(final String name)
    {
        IValue definition = this.context.getRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS).get(name);
        if (definition == null)
        {
            return null;
        }
        RpcInstance instance = RpcInstance.constructInstanceFromDefinition(name, definition.textValue());
        instance.setHandler(new RpcInstance.Remote.Caller(name, this.codec, this.channel, this.context,
            instance.remoteExecutionStartTimeoutMillis, instance.remoteExecutionDurationTimeoutMillis));
        return instance;
    }

    /**
     * @return <code>true</code> if the proxy is connected to the remote context
     */
    public boolean isConnected()
    {
        return this.channel.isConnected();
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

    void cancelReconnectTask()
    {
        this.lock.lock();
        try
        {
            if (this.reconnectTask != null)
            {
                this.reconnectTask.cancel(false);
                ProxyContext.this.reconnectTask = null;
            }
        }
        finally
        {
            this.lock.unlock();
        }
    }

    void setupReconnectTask()
    {
        this.lock.lock();
        try
        {
            if (!this.active)
            {
                Log.log(this, "Not setting up reconnect task for proxy as it is not active: ",
                    ObjectUtils.safeToString(this));
                return;
            }
            if (this.reconnectTask != null)
            {
                Log.log(this, "Reconnect still pending for ", ObjectUtils.safeToString(this));
                return;
            }

            Log.log(this, "Setting up reconnection for ", ObjectUtils.safeToString(this));

            // Remove RPCs
            final IRecord rpcRecord = this.context.getRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
            if (rpcRecord.size() > 0)
            {
                Log.log(this, "Removing RPCs ", ObjectUtils.safeToString(rpcRecord), " from ",
                    ObjectUtils.safeToString(this));
                final Lock lock = this.context.getRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS).getWriteLock();
                lock.lock();
                try
                {
                    rpcRecord.clear();
                    this.context.publishAtomicChange(rpcRecord);
                }
                finally
                {
                    lock.unlock();
                }
            }

            Log.log(this, "Resubscribing in ", Long.toString(this.reconnectPeriodMillis), "ms ",
                ObjectUtils.safeToString(this));

            this.reconnectTask = ContextUtils.RECONNECT_TASKS.schedule(new Runnable()
            {
                @Override
                public void run()
                {
                    reconnect();
                }
            }, this.reconnectPeriodMillis, TimeUnit.MILLISECONDS);
        }
        finally
        {
            this.lock.unlock();
        }
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
        final Set<String> recordNames = new HashSet<String>(this.context.getSubscribedRecords());
        recordNames.remove(RECORD_CONNECTION_STATUS_NAME);

        Lock lock = this.context.getRecord(RECORD_CONNECTION_STATUS_NAME).getWriteLock();
        lock.lock();
        try
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
        finally
        {
            lock.unlock();
        }
    }

    void reconnect()
    {
        this.lock.lock();
        try
        {
            updateConnectionStatus(Connection.RECONNECTING);

            this.connected = false;
            this.reconnectTask = null;

            if (!this.active)
            {
                Log.log(this, "Not reconnecting proxy as it is not active: ", ObjectUtils.safeToString(this));
                return;
            }

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
        finally
        {
            this.lock.unlock();
        }
    }

    CountDownLatch executeTask(final String[] recordNames, final String action, final Runnable task,
        RunnableFuture<Map<String, Boolean>> subscribeFutureResult, Map<String, Boolean> subscribeResults)
    {
        CountDownLatch latch = new CountDownLatch(recordNames.length);
        List<CountDownLatch> latches;
        CopyOnWriteArrayList<CountDownLatch> pending;
        for (int i = 0; i < recordNames.length; i++)
        {
            pending = new CopyOnWriteArrayList<CountDownLatch>();
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
    public Set<String> getSubscribedRecords()
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
        if (ProxyContext.this.channel instanceof ISubscribingChannel)
        {
            for (int i = 0; i < recordsToSubscribeFor.length; i++)
            {
                ((ISubscribingChannel) ProxyContext.this.channel).contextSubscribed(recordsToSubscribeFor[i]);
            }
        }
        ProxyContext.this.channel.sendAsync(ProxyContext.this.codec.getTxMessageForSubscribe(insertPermissionToken(
            permissionToken, recordsToSubscribeFor)));
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
}
