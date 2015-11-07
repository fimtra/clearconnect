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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.EndPointServiceLoader;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.DataFissionProperties.Values;
import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.ICodec.CommandEnum;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.thimble.ISequentialRunnable;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.StringUtils;
import com.fimtra.util.SubscriptionManager;

/**
 * A publisher is actually a manager object for multiple {@link ProxyContextPublisher} objects.
 * There is one ProxyContextPublisher per {@link ProxyContext} that is connected. Each
 * ProxyContextPublisher is attached any number of record objects and publishes the changes to the
 * proxy context. The proxy context requests the records that should be observed by the publisher
 * for changes.
 * <p>
 * For efficiency, each ProxyContextPublisher actually submits its record subscriptions to a
 * {@link ProxyContextMultiplexer}. The multiplexer receives the record changes, converts them into
 * the wire-format and notifies the ProxyContextPublishers with the data packet to send. The
 * prevents redundant codec calls to transform the same record update into a wire-format when the
 * same record is published to multiple proxies.
 * <p>
 * The publisher opens up a single TCP server socket that the proxy contexts connect to.
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings("rawtypes")
public class Publisher
{
    /**
     * Delimiter for statistics attributes published for each proxy context connection in the
     * {@link ISystemRecordNames#CONTEXT_STATUS}
     */
    public static final String ATTR_DELIM = ",";

    final static char[][] IGNORE_RX_COMMANDS_PREFIX;

    static
    {
        final String[] stringPrefixes =
            Values.IGNORE_LOGGING_RX_COMMANDS_WITH_PREFIX.toArray(new String[Values.IGNORE_LOGGING_RX_COMMANDS_WITH_PREFIX.size()]);
        IGNORE_RX_COMMANDS_PREFIX = new char[stringPrefixes.length][];
        for (int i = 0; i < stringPrefixes.length; i++)
        {
            String prefix = stringPrefixes[i];
            IGNORE_RX_COMMANDS_PREFIX[i] = prefix.toCharArray();
        }
    }

    static boolean canLog(char[] decodedMessage)
    {
        for (int i = 0; i < IGNORE_RX_COMMANDS_PREFIX.length; i++)
        {
            if (StringUtils.startsWith(IGNORE_RX_COMMANDS_PREFIX[i], decodedMessage))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * @return the field name for the transmission statistics for a connection to a single
     *         {@link ProxyContext}
     */
    static String getTransmissionStatisticsFieldName(ITransportChannel channel)
    {
        return channel.getDescription();
    }

    final Lock lock;

    /**
     * This converts each record's atomic change into the <code>byte[]</code> to transmit and
     * notifies the relevant {@link ProxyContextPublisher} objects that have subscribed for the
     * record.
     * <p>
     * This prevents the same atomic change being converted to a <code>byte[]</code> multiple times
     * to send to multiple proxy contexts.
     * 
     * @author Ramon Servadei
     */
    private final class ProxyContextMultiplexer implements IRecordListener
    {
        final IEndPointService service;
        final AtomicChangeTeleporter teleporter;
        final SubscriptionManager<String, ProxyContextPublisher> subscribers;

        ProxyContextMultiplexer(IEndPointService service)
        {
            super();
            this.subscribers = new SubscriptionManager<String, ProxyContextPublisher>(ProxyContextPublisher.class);
            this.teleporter =
                new AtomicChangeTeleporter(DataFissionProperties.Values.PUBLISHER_MAXIMUM_CHANGES_PER_MESSAGE);
            this.service = service;
        }

        @Override
        public void onChange(IRecord imageCopy, IRecordChange atomicChange)
        {
            final AtomicChange[] parts = this.teleporter.split((AtomicChange) atomicChange);
            byte[] txMessage;
            final ProxyContextPublisher[] clients = this.subscribers.getSubscribersFor(atomicChange.getName());
            int j = 0;
            for (int i = 0; i < parts.length; i++)
            {
                txMessage = Publisher.this.mainCodec.getTxMessageForAtomicChange(parts[i]);

                int broadcastCount = this.service.broadcast(atomicChange.getName(), txMessage, clients);

                Publisher.this.messagesPublished += broadcastCount;
                Publisher.this.bytesPublished += (broadcastCount * txMessage.length);

                // even if the service is broadcast capable, perform this loop to capture stats
                for (j = 0; j < clients.length; j++)
                {
                    clients[j].publish(txMessage, false);
                }
            }
        }

        void addSubscriberFor(final String name, final ProxyContextPublisher publisher, final String permissionToken,
            final List<String> ackSubscribes, final List<String> nokSubscribes, final Runnable task)
        {
            Publisher.this.context.executeSequentialCoreTask(new ISequentialRunnable()
            {
                @Override
                public void run()
                {
                    Publisher.this.lock.lock();
                    try
                    {
                        if (ProxyContextMultiplexer.this.subscribers.addSubscriberFor(name, publisher))
                        {
                            if (ProxyContextMultiplexer.this.subscribers.getSubscribersFor(name).length == 1)
                            {
                                try
                                {
                                    // this will call addDeltaToSubscriptionCount and publish the
                                    // image
                                    if (Publisher.this.context.addObserver(permissionToken,
                                        ProxyContextMultiplexer.this, name).get().get(name).booleanValue())
                                    {
                                        ackSubscribes.add(name);
                                    }
                                    else
                                    {
                                        nokSubscribes.add(name);
                                    }
                                }
                                catch (Exception e)
                                {
                                    Log.log(Publisher.this.context,
                                        "Could not get result from addObserver call for permissionToken="
                                            + permissionToken + ", recordName=" + name, e);
                                    nokSubscribes.add(name);
                                }
                            }
                            else
                            {
                                try
                                {
                                    if (Publisher.this.context.permissionTokenValidForRecord(permissionToken, name))
                                    {
                                        Publisher.this.context.addDeltaToSubscriptionCount(name, 1);

                                        // we must send an initial image to the new client if it is
                                        // not the first one to register
                                        final IRecord record = Publisher.this.context.getLastPublishedImage(name);
                                        if (record != null)
                                        {
                                            final AtomicChange[] parts =
                                                ProxyContextMultiplexer.this.teleporter.split(new AtomicChange(record));
                                            for (int i = 0; i < parts.length; i++)
                                            {
                                                publisher.publish(
                                                    publisher.codec.getTxMessageForAtomicChange(parts[i]), true);
                                            }
                                        }
                                        ackSubscribes.add(name);
                                    }
                                    else
                                    {
                                        nokSubscribes.add(name);
                                    }
                                }
                                catch (Exception e)
                                {
                                    Log.log(Publisher.this.context, "Could not add subscriber for permissionToken="
                                        + permissionToken + ", recordName=" + name, e);
                                    nokSubscribes.add(name);
                                }
                            }
                        }
                        else
                        {
                            nokSubscribes.add(name);
                        }
                        task.run();
                    }
                    finally
                    {
                        Publisher.this.lock.unlock();
                    }
                }

                @Override
                public Object context()
                {
                    return name;
                }
            });
        }

        void removeSubscriberFor(final String name, final ProxyContextPublisher publisher)
        {
            Publisher.this.context.executeSequentialCoreTask(new ISequentialRunnable()
            {
                @Override
                public void run()
                {
                    Publisher.this.lock.lock();
                    try
                    {
                        ProxyContextMultiplexer.this.subscribers.removeSubscriberFor(name, publisher);
                        if (ProxyContextMultiplexer.this.subscribers.getSubscribersFor(name).length == 0)
                        {
                            Publisher.this.context.removeObserver(ProxyContextMultiplexer.this, name);
                            ProxyContextMultiplexer.this.service.endBroadcast(name);
                        }
                        else
                        {
                            Publisher.this.context.addDeltaToSubscriptionCount(name, -1);
                        }
                    }
                    finally
                    {
                        Publisher.this.lock.unlock();
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

    /**
     * This is the actual publisher object that publishes record changes to a single
     * {@link ProxyContext}.
     * <p>
     * A scheduled task runs periodically to update the publishing statistics of this.
     * 
     * @author Ramon Servadei
     */
    private final class ProxyContextPublisher implements ITransportChannel
    {
        final ITransportChannel client;
        final CopyOnWriteArraySet<String> subscriptions = new CopyOnWriteArraySet<String>();
        final long start;
        final ICodec codec;
        ScheduledFuture statsUpdateTask;
        volatile long messagesPublished;
        volatile long bytesPublished;
        String identity;
        volatile boolean active;

        ProxyContextPublisher(ITransportChannel client, ICodec codec)
        {
            this.active = true;
            this.codec = codec;
            this.start = System.currentTimeMillis();
            this.client = client;

            // add the connection record static parts
            final Map<String, IValue> submapConnections =
                Publisher.this.connectionsRecord.getOrCreateSubMap(getTransmissionStatisticsFieldName(client));
            final EndPointAddress endPointAddress = Publisher.this.server.getEndPointAddress();
            final String clientSocket = client.getEndPointDescription();
            submapConnections.put(IContextConnectionsRecordFields.PUBLISHER_ID,
                new TextValue(Publisher.this.context.getName()));
            submapConnections.put(IContextConnectionsRecordFields.PUBLISHER_NODE,
                new TextValue(endPointAddress.getNode()));
            submapConnections.put(IContextConnectionsRecordFields.PUBLISHER_PORT,
                LongValue.valueOf(endPointAddress.getPort()));
            submapConnections.put(IContextConnectionsRecordFields.PROXY_ENDPOINT, new TextValue(clientSocket));
            submapConnections.put(IContextConnectionsRecordFields.PROTOCOL, new TextValue(
                this.codec.getClass().getSimpleName()));

            scheduleStatsUpdateTask();

            Log.log(this, "Constructed for ", ObjectUtils.safeToString(client));
        }

        void scheduleStatsUpdateTask()
        {
            if (this.statsUpdateTask != null)
            {
                this.statsUpdateTask.cancel(false);
            }
            this.statsUpdateTask = Publisher.this.context.getUtilityExecutor().schedule(new Runnable()
            {
                long lastMessagesPublished = 0;
                long lastBytesPublished = 0;

                @Override
                public void run()
                {
                    final Map<String, IValue> submapConnections =
                        Publisher.this.connectionsRecord.getOrCreateSubMap(getTransmissionStatisticsFieldName(ProxyContextPublisher.this.client));

                    final double perMin = 60000d / (Publisher.this.contextConnectionsRecordPublishPeriodMillis / 2);
                    submapConnections.put(
                        IContextConnectionsRecordFields.MSGS_PER_MIN,
                        LongValue.valueOf((long) ((ProxyContextPublisher.this.messagesPublished - this.lastMessagesPublished) * perMin)));
                    submapConnections.put(
                        IContextConnectionsRecordFields.KB_PER_MIN,
                        LongValue.valueOf((long) (((ProxyContextPublisher.this.bytesPublished - this.lastBytesPublished) / 1024) * perMin)));

                    submapConnections.put(IContextConnectionsRecordFields.MESSAGE_COUNT,
                        LongValue.valueOf(ProxyContextPublisher.this.messagesPublished));
                    submapConnections.put(IContextConnectionsRecordFields.KB_COUNT,
                        LongValue.valueOf(ProxyContextPublisher.this.bytesPublished / 1024));
                    submapConnections.put(IContextConnectionsRecordFields.SUBSCRIPTION_COUNT,
                        LongValue.valueOf(ProxyContextPublisher.this.subscriptions.size()));
                    submapConnections.put(IContextConnectionsRecordFields.UPTIME,
                        LongValue.valueOf((System.currentTimeMillis() - ProxyContextPublisher.this.start) / 1000));

                    this.lastMessagesPublished = ProxyContextPublisher.this.messagesPublished;
                    this.lastBytesPublished = ProxyContextPublisher.this.bytesPublished;

                    if (ProxyContextPublisher.this.active)
                    {
                        ProxyContextPublisher.this.statsUpdateTask =
                            Publisher.this.context.getUtilityExecutor().schedule(this,
                                Publisher.this.contextConnectionsRecordPublishPeriodMillis / 2, TimeUnit.MILLISECONDS);
                    }
                }
            }, Publisher.this.contextConnectionsRecordPublishPeriodMillis / 2, TimeUnit.MILLISECONDS);
        }

        void publish(byte[] txMessage, boolean pointToPoint)
        {
            if (pointToPoint)
            {
                this.client.sendAsync(txMessage);
            }
            this.bytesPublished += txMessage.length;
            this.messagesPublished++;
        }

        void subscribe(String name, String permissionToken, List<String> ackSubscribes, List<String> nokSubscribes,
            Runnable task)
        {
            try
            {
                this.subscriptions.add(name);
                Publisher.this.multiplexer.addSubscriberFor(name, this, permissionToken, ackSubscribes, nokSubscribes,
                    task);
            }
            catch (Exception e)
            {
                Log.log(ProxyContextPublisher.this, "Could not subscribe " + name, e);
            }
        }

        void unsubscribe(String name)
        {
            try
            {
                this.subscriptions.remove(name);
                Publisher.this.multiplexer.removeSubscriberFor(name, this);
            }
            catch (Exception e)
            {
                Log.log(ProxyContextPublisher.this, "Could not unsubscribe " + name, e);
            }
        }

        void destroy()
        {
            this.active = false;
            this.statsUpdateTask.cancel(false);
            for (String name : this.subscriptions)
            {
                unsubscribe(name);
            }
            Log.log(this, "Destroyed");
        }

        void setProxyContextIdentity(String identity)
        {
            this.identity = identity;
            Publisher.this.connectionsRecord.getOrCreateSubMap(getTransmissionStatisticsFieldName(this.client)).put(
                IContextConnectionsRecordFields.PROXY_ID, new TextValue(this.identity));
        }

        @Override
        public boolean sendAsync(byte[] toSend)
        {
            return this.client.sendAsync(toSend);
        }

        @Override
        public boolean isConnected()
        {
            return this.client.isConnected();
        }

        @Override
        public String getEndPointDescription()
        {
            return this.client.getEndPointDescription();
        }

        @Override
        public String getDescription()
        {
            return this.client.getDescription();
        }

        @Override
        public void destroy(String reason, Exception... e)
        {
            this.client.destroy(reason, e);
        }

        @Override
        public boolean hasRxData()
        {
            return this.client.hasRxData();
        }
    }

    final Map<ITransportChannel, ProxyContextPublisher> proxyContextPublishers;
    final Context context;
    final ICodec mainCodec;
    final IEndPointService server;
    final IRecord connectionsRecord;
    final ProxyContextMultiplexer multiplexer;
    volatile long contextConnectionsRecordPublishPeriodMillis = 10000;
    ScheduledFuture contextConnectionsRecordPublishTask;
    volatile long messagesPublished;
    volatile long bytesPublished;

    /**
     * Constructs the publisher and creates an {@link IEndPointService} to accept connections from
     * {@link ProxyContext} objects.
     * 
     * @param context
     *            the context the publisher is for
     * @param codec
     *            the codec to use for sending/receiving messages from the {@link ProxyContext}
     * @param node
     *            the node for the {@link EndPointAddress} of this publisher
     * @param port
     *            the port for the {@link EndPointAddress} of this publisher
     */
    public Publisher(Context context, ICodec codec, String node, int port)
    {
        super();
        this.context = context;
        this.lock = new ReentrantLock();
        this.proxyContextPublishers = new ConcurrentHashMap<ITransportChannel, Publisher.ProxyContextPublisher>();
        this.connectionsRecord = Context.getRecordInternal(this.context, ISystemRecordNames.CONTEXT_CONNECTIONS);

        // prepare to periodically publish status changes
        this.publishContextConnectionsRecordAtPeriod(this.contextConnectionsRecordPublishPeriodMillis);

        this.mainCodec = codec;
        this.server =
            EndPointServiceLoader.load(this.mainCodec.getFrameEncodingFormat(), new EndPointAddress(node, port)).buildService(
                new IReceiver()
                {
                    @Override
                    public void onChannelConnected(ITransportChannel channel)
                    {
                        // construct the ProxyContextPublisher
                        Publisher.this.proxyContextPublishers.put(channel, new ProxyContextPublisher(channel,
                            Publisher.this.mainCodec.newInstance()));
                    }

                    @Override
                    public void onDataReceived(final byte[] data, final ITransportChannel source)
                    {
                        Publisher.this.context.executeSequentialCoreTask(new ISequentialRunnable()
                        {
                            @SuppressWarnings("unchecked")
                            @Override
                            public void run()
                            {
                                final ICodec channelsCodec = getProxyContextPublisher(source).codec;
                                Object decodedMessage = channelsCodec.decode(data);
                                final CommandEnum command = channelsCodec.getCommand(decodedMessage);
                                final int maxLogLength = 128;
                                if (decodedMessage instanceof char[])
                                {
                                    if (canLog((char[]) decodedMessage))
                                    {
                                        if (((char[]) decodedMessage).length < maxLogLength)
                                        {
                                            Log.log(Publisher.class, "(<-) '", new String((char[]) decodedMessage),
                                                "' from ", ObjectUtils.safeToString(source));
                                        }
                                        else
                                        {
                                            Log.log(Publisher.class, "(<-) '", new String((char[]) decodedMessage, 0,
                                                maxLogLength), "...(too long)' from ", ObjectUtils.safeToString(source));
                                        }
                                    }
                                }
                                else
                                {
                                    Log.log(Publisher.class, "(<-) ", command.toString(), " from ",
                                        ObjectUtils.safeToString(source));
                                }
                                switch(command)
                                {
                                    case RPC:
                                        rpc(decodedMessage, source);
                                        break;
                                    case IDENTIFY:
                                        identify(channelsCodec.getIdentityArgumentFromDecodedMessage(decodedMessage),
                                            source);
                                        break;
                                    case SHOW:
                                        show(source);
                                        break;
                                    case SUBSCRIBE:
                                        subscribe(
                                            channelsCodec.getSubscribeArgumentsFromDecodedMessage(decodedMessage),
                                            source);
                                        break;
                                    case UNSUBSCRIBE:
                                        unsubscribe(
                                            channelsCodec.getUnsubscribeArgumentsFromDecodedMessage(decodedMessage),
                                            source);
                                        break;
                                    case NOOP:
                                        break;
                                }
                            }

                            @Override
                            public Object context()
                            {
                                return Publisher.this;
                            }
                        });
                    }

                    @Override
                    public void onChannelClosed(ITransportChannel channel)
                    {
                        ProxyContextPublisher clientPublisher = Publisher.this.proxyContextPublishers.remove(channel);
                        if (clientPublisher != null)
                        {
                            clientPublisher.destroy();
                        }
                    }
                });

        this.multiplexer = new ProxyContextMultiplexer(this.server);
    }

    public long getContextConnectionsRecordPublishPeriodMillis()
    {
        return this.contextConnectionsRecordPublishPeriodMillis;
    }

    /**
     * Publish the {@link ISystemRecordNames#CONTEXT_CONNECTIONS} record at the given period in
     * milliseconds
     */
    public synchronized void publishContextConnectionsRecordAtPeriod(long contextConnectionsRecordPublishPeriodMillis)
    {
        this.contextConnectionsRecordPublishPeriodMillis = contextConnectionsRecordPublishPeriodMillis;
        if (this.contextConnectionsRecordPublishTask != null)
        {
            this.contextConnectionsRecordPublishTask.cancel(false);
        }
        this.contextConnectionsRecordPublishTask =
            this.context.getUtilityExecutor().scheduleWithFixedDelay(
                new Runnable()
                {
                    CountDownLatch publishAtomicChange = new CountDownLatch(0);

                    @Override
                    public void run()
                    {
                        if (this.publishAtomicChange.getCount() == 0)
                        {
                            // check each connection is still active - remove if not
                            final Set<String> connectionIds =
                                new HashSet<String>(Publisher.this.connectionsRecord.getSubMapKeys());
                            final Set<ITransportChannel> channels = Publisher.this.proxyContextPublishers.keySet();
                            for (ITransportChannel channel : channels)
                            {
                                connectionIds.remove(getTransmissionStatisticsFieldName(channel));
                            }
                            for (String connectionId : connectionIds)
                            {
                                Publisher.this.connectionsRecord.removeSubMap(connectionId);
                            }

                            this.publishAtomicChange =
                                Publisher.this.context.publishAtomicChange(ISystemRecordNames.CONTEXT_CONNECTIONS);
                        }
                    }
                }, this.contextConnectionsRecordPublishPeriodMillis, this.contextConnectionsRecordPublishPeriodMillis,
                TimeUnit.MILLISECONDS);

        // reschedule the stats update tasks at the new period
        for (ProxyContextPublisher proxyContextPublisher : this.proxyContextPublishers.values())
        {
            proxyContextPublisher.scheduleStatsUpdateTask();
        }
    }

    @Override
    public String toString()
    {
        return "Publisher [" + this.context.getName() + ", " + this.server + ", clients="
            + this.proxyContextPublishers.keySet().size() + ", messages published=" + this.messagesPublished
            + ", bytes published=" + this.bytesPublished + "]";
    }

    public void destroy()
    {
        for (ProxyContextPublisher proxyContextPublisher : this.proxyContextPublishers.values())
        {
            proxyContextPublisher.destroy();
        }
        this.proxyContextPublishers.clear();

        this.server.destroy();
        this.contextConnectionsRecordPublishTask.cancel(true);
    }

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        destroy();
    }

    /**
     * Invoke the RPC. The RPC execution will occur in a thread bound to the client channel.
     */
    void rpc(final Object data, final ITransportChannel client)
    {
        this.context.executeRpcTask(new ISequentialRunnable()
        {
            @Override
            public void run()
            {
                new RpcInstance.Remote.CallReceiver(getProxyContextPublisher(client).codec, client,
                    Publisher.this.context).execute(data);
            }

            @Override
            public Object context()
            {
                return client;
            }
        });
    }

    @SuppressWarnings("unchecked")
    void show(ITransportChannel client)
    {
        client.sendAsync(getProxyContextPublisher(client).codec.getTxMessageForShow(this.context.getRecordNames()));
    }

    void unsubscribe(List<String> recordNames, ITransportChannel client)
    {
        ProxyContextPublisher proxyContextPublisher = getProxyContextPublisher(client);
        for (String name : recordNames)
        {
            proxyContextPublisher.unsubscribe(name);
        }
        sendAck(recordNames, client, proxyContextPublisher, ProxyContext.UNSUBSCRIBE);
    }

    void subscribe(final List<String> recordNames, final ITransportChannel client)
    {
        // the first item is always the permission token
        final String permissionToken = recordNames.remove(0);
        final ProxyContextPublisher proxyContextPublisher = getProxyContextPublisher(client);
        final List<String> ackSubscribes = new ArrayList<String>(recordNames.size());
        final List<String> nokSubscribes = new ArrayList<String>(recordNames.size());

        for (String name : recordNames)
        {
            proxyContextPublisher.subscribe(name, permissionToken, ackSubscribes, nokSubscribes, new Runnable()
            {
                @Override
                public void run()
                {
                    if (ackSubscribes.size() + nokSubscribes.size() == recordNames.size())
                    {
                        sendAck(ackSubscribes, client, proxyContextPublisher, ProxyContext.SUBSCRIBE);
                        sendNok(nokSubscribes, client, proxyContextPublisher, ProxyContext.SUBSCRIBE);
                    }
                }
            });
        }
    }

    void sendAck(List<String> recordNames, ITransportChannel client, ProxyContextPublisher proxyContextPublisher,
        String responseAction)
    {
        sendSubscribeResult(ProxyContext.ACK, recordNames, client, proxyContextPublisher, responseAction);
    }

    void sendNok(List<String> recordNames, ITransportChannel client, ProxyContextPublisher proxyContextPublisher,
        String responseAction)
    {
        sendSubscribeResult(ProxyContext.NOK, recordNames, client, proxyContextPublisher, responseAction);
    }

    private static void sendSubscribeResult(String action, List<String> recordNames, ITransportChannel client,
        ProxyContextPublisher proxyContextPublisher, String responseAction)
    {
        final StringBuilder sb = new StringBuilder(recordNames.size() * 30);
        sb.append(action).append(responseAction).append(ProxyContext.ACK_ACTION_ARGS_START).append(
            StringUtils.join(recordNames, ProxyContext.ACK_ARGS_DELIMITER));
        final IRecordChange atomicChange =
            new AtomicChange(sb.toString(), ContextUtils.EMPTY_MAP, ContextUtils.EMPTY_MAP, ContextUtils.EMPTY_MAP);
        Log.log(Publisher.class, "(->) ", atomicChange.getName());
        client.sendAsync(proxyContextPublisher.codec.getTxMessageForAtomicChange(atomicChange));
    }

    void identify(String identityOfRemoteProxy, ITransportChannel client)
    {
        ProxyContextPublisher proxyContextPublisher = getProxyContextPublisher(client);
        proxyContextPublisher.setProxyContextIdentity(identityOfRemoteProxy);
    }

    ProxyContextPublisher getProxyContextPublisher(ITransportChannel client)
    {
        final ProxyContextPublisher proxyContextPublisher = this.proxyContextPublishers.get(client);
        if (proxyContextPublisher == null)
        {
            // ProxyContextPublisher only constructed on channel connection!
            throw new NullPointerException("No ProxyContextPublisher for " + ObjectUtils.safeToString(client)
                + ", is the channel closed?");
        }
        return proxyContextPublisher;
    }

    /**
     * @return the address used by this publisher
     */
    public EndPointAddress getEndPointAddress()
    {
        return this.server.getEndPointAddress();
    }

    public long getMessagesPublished()
    {
        return this.messagesPublished;
    }

    public long getBytesPublished()
    {
        return this.bytesPublished;
    }
}
