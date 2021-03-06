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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.ICodec.CommandEnum;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.ISessionProtocol.SyncResponse;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.CoalescingRecordListener.CachePolicyEnum;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.executors.ContextExecutorFactory;
import com.fimtra.executors.ISequentialRunnable;
import com.fimtra.util.CollectionUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SubscriptionManager;
import com.fimtra.util.SystemUtils;

/**
 * A publisher is actually a manager object for multiple {@link ProxyContextPublisher} objects. There is one
 * ProxyContextPublisher per {@link ProxyContext} that is connected. Each ProxyContextPublisher is attached
 * any number of record objects and publishes the changes to the proxy context. The proxy context requests the
 * records that should be observed by the publisher for changes.
 * <p>
 * For efficiency, each ProxyContextPublisher actually submits its record subscriptions to a {@link
 * ProxyContextMultiplexer}. The multiplexer receives the record changes, converts them into the wire-format
 * and notifies the ProxyContextPublishers with the data packet to send. The prevents redundant codec calls to
 * transform the same record update into a wire-format when the same record is published to multiple proxies.
 * <p>
 * The publisher opens up a single TCP server socket that the proxy contexts connect to.
 *
 * @author Ramon Servadei
 */
@SuppressWarnings("rawtypes")
public class Publisher {
    /**
     * Controls logging of:
     * <ul>
     * <li>Inbound requests (subscriptions, RPC calls)
     * <li>Outbound subscription responses
     * </ul>
     * This can be useful to improve performance for situations where there is high-throughput of
     * record creates
     */
    public static boolean log = SystemUtils.getProperty("log." + Publisher.class.getCanonicalName(), false);

    /**
     * Controls logging of outbound traffic. Only the first 200 bytes per message are logged.
     */
    public static boolean logTx =
            SystemUtils.getProperty("logTx." + ProxyContextPublisher.class.getCanonicalName(), false);

    /**
     * Controls logging of:
     * <ul>
     * <li>Subscribes received
     * <li>First update published
     * <ul>
     */
    public static boolean logVerboseSubscribes =
            SystemUtils.getProperty("logVerboseSubscribes." + ProxyContextPublisher.class.getCanonicalName(),
                    false);

    static final AtomicLong MESSAGES_PUBLISHED = new AtomicLong();
    static final AtomicLong BYTES_PUBLISHED = new AtomicLong();

    /**
     * @return the field name for the transmission statistics for a connection to a single {@link
     * ProxyContext}
     */
    static String getTransmissionStatisticsFieldName(ITransportChannel channel)
    {
        return channel.getDescription();
    }

    static boolean isSystemRecordUpdateCoalesced(String name)
    {
        return ContextUtils.isSystemRecordName(name) &&
                // ignore the CONTEXT_STATUS - it hardly changes and is used to detect
                // CONNECTED/DISCONNECTED
                !ISystemRecordNames.CONTEXT_STATUS.equals(name);
    }

    final Object lock;
    final AtomicLong subscribeCounter = new AtomicLong();

    /**
     * This converts each record's atomic change into the <code>byte[]</code> to transmit and notifies the
     * relevant {@link ProxyContextPublisher} objects that have subscribed for the record.
     * <p>
     * This prevents the same atomic change being converted to a <code>byte[]</code> multiple times to send to
     * multiple proxy contexts.
     *
     * @author Ramon Servadei
     */
    private final class ProxyContextMultiplexer implements IRecordListener {
        final IEndPointService service;
        final AtomicChangeTeleporter teleporter;
        final SubscriptionManager<String, ProxyContextPublisher> subscribers;
        /**
         * A {@link CoalescingRecordListener} per system record to ensure more efficient remote transmission
         * when a context has many single record creates/subscribes etc.
         */
        final Map<String, CoalescingRecordListener> systemRecordPublishers;
        /**
         * Holds the message sequences for the system records that are published
         */
        final Map<String, AtomicLong> systemRecordSequences;

        final Map<String, Long> lastSlowPublishLogTime = new ConcurrentHashMap<>();
        final Map<String, AtomicLong> slowPublishCounts = new ConcurrentHashMap<>();
        final Function<String, AtomicLong> createAtomicLongFunction = (n) -> new AtomicLong(-1);

        ProxyContextMultiplexer(IEndPointService service)
        {
            super();
            this.subscribers = new SubscriptionManager<>(ProxyContextPublisher.class);
            this.teleporter = new AtomicChangeTeleporter(
                    DataFissionProperties.Values.PUBLISHER_MAXIMUM_CHANGES_PER_MESSAGE);
            this.service = service;

            this.systemRecordPublishers = new HashMap<>(ContextUtils.SYSTEM_RECORDS.size());
            this.systemRecordSequences = new HashMap<>(ContextUtils.SYSTEM_RECORDS.size());

            for (String systemRecord : ContextUtils.SYSTEM_RECORDS)
            {
                this.systemRecordSequences.put(systemRecord, new AtomicLong());
                this.systemRecordPublishers.put(systemRecord, new CoalescingRecordListener(
                        DataFissionProperties.Values.SYSTEM_RECORD_COALESCE_WINDOW_MILLIS,
                        (image, atomicChange) -> handleTimedSystemRecordChange(atomicChange),
                        ContextUtils.CORE_EXECUTOR, getCoalescingContext(systemRecord),
                        CachePolicyEnum.NO_IMAGE_NEEDED));
            }
        }

        void handleTimedSystemRecordChange(IRecordChange atomicChange)
        {
            final AtomicLong currentSequence =
                    ProxyContextMultiplexer.this.systemRecordSequences.get(atomicChange.getName());
            atomicChange.setSequence(currentSequence.getAndIncrement());
            handleRecordChange(atomicChange);
        }

        @Override
        public void onChange(IRecord imageCopy, final IRecordChange atomicChange)
        {
            if (isSystemRecordUpdateCoalesced(imageCopy.getName()))
            {
                this.systemRecordPublishers.get(imageCopy.getName()).onChange(imageCopy, atomicChange);
            }
            else
            {
                handleRecordChange(atomicChange);
            }
        }

        void handleRecordChange(IRecordChange atomicChange)
        {
            long publishTime = System.nanoTime();

            final AtomicChange[] parts = this.teleporter.split((AtomicChange) atomicChange);
            byte[] txMessage;
            final String name = atomicChange.getName();
            final ProxyContextPublisher[] clients = this.subscribers.getSubscribersFor(name);
            int j;
            int broadcastCount = 0;

            long splitTime = System.nanoTime();

            long encodeTime = 0, sendTime = 0;

            int bytesPublished = 0;
            if (parts != null)
            {
                long t1, t2, t3;
                int loopBroadcastCount;
                t3 = System.nanoTime();
                for (AtomicChange part : parts)
                {
                    t1 = t3;
                    txMessage = Publisher.this.mainCodec.getTxMessageForAtomicChange(part);
                    t2 = System.nanoTime();

                    loopBroadcastCount = this.service.broadcast(name, txMessage, clients);
                    bytesPublished += loopBroadcastCount * txMessage.length;
                    broadcastCount += loopBroadcastCount;

                    // even if the service is broadcast capable, perform this loop to capture stats
                    for (j = 0; j < clients.length; j++)
                    {
                        clients[j].publish(txMessage, false, name);
                    }
                    t3 = System.nanoTime();

                    encodeTime += t2 - t1;
                    sendTime = t3 - t2;
                }

                Publisher.this.messagesPublished += broadcastCount;
                MESSAGES_PUBLISHED.addAndGet(broadcastCount);
                Publisher.this.bytesPublished += bytesPublished;
                BYTES_PUBLISHED.addAndGet(bytesPublished);
            }
            else
            {
                txMessage = Publisher.this.mainCodec.getTxMessageForAtomicChange(atomicChange);
                encodeTime = System.nanoTime();

                broadcastCount = this.service.broadcast(name, txMessage, clients);

                Publisher.this.messagesPublished += broadcastCount;
                MESSAGES_PUBLISHED.addAndGet(broadcastCount);
                bytesPublished = broadcastCount * txMessage.length;
                Publisher.this.bytesPublished += bytesPublished;
                BYTES_PUBLISHED.addAndGet(bytesPublished);

                // even if the service is broadcast capable, perform this loop to capture stats
                for (j = 0; j < clients.length; j++)
                {
                    clients[j].publish(txMessage, false, name);
                }

                sendTime = System.nanoTime() - encodeTime;
                encodeTime = encodeTime - splitTime;
            }

            splitTime = splitTime - publishTime;
            publishTime = splitTime + encodeTime + sendTime;
            if (publishTime > DataFissionProperties.Values.SLOW_PUBLISH_THRESHOLD_NANOS)
            {
                final long now = System.nanoTime();
                final Long lastSlowTime = this.lastSlowPublishLogTime.get(name);
                this.slowPublishCounts.computeIfAbsent(name,
                        this.createAtomicLongFunction).incrementAndGet();
                if ((lastSlowTime == null || (now - lastSlowTime >= 60_000_000_000L)))
                {
                    this.lastSlowPublishLogTime.put(name, now);
                    Log.log(this, "SLOW publish for [", atomicChange.getName(), "] (",
                            Long.toString(this.slowPublishCounts.get(name).getAndSet(-1)),
                            " since last log): ",
                            Long.toString((long) (publishTime * ContextUtils.INVERSE_1000000)), "ms {split=",
                            Long.toString((long) (splitTime * ContextUtils.INVERSE_1000000)), "ms, encode=",
                            Long.toString((long) (encodeTime * ContextUtils.INVERSE_1000000)), "ms, send=",
                            Long.toString((long) (sendTime * ContextUtils.INVERSE_1000000)), "ms, parts=",
                            Long.toString(parts == null ? 1 : parts.length), "}");
                }
            }
        }

        void addSubscriberFor(final Collection<String> names, final ProxyContextPublisher publisher,
                final String permissionToken, final List<String> ackSubscribes,
                final List<String> nokSubscribes, final Runnable task)
        {
            try
            {
                synchronized (Publisher.this.lock)
                {
                    final List<String> increment = new LinkedList<>();
                    for (final String name : names)
                    {
                        try
                        {
                            if (Publisher.this.context.permissionTokenValidForRecord(permissionToken, name))
                            {
                                if (ProxyContextMultiplexer.this.subscribers.addSubscriberFor(name,
                                        publisher))
                                {
                                    if (ProxyContextMultiplexer.this.subscribers.getSubscribersFor(
                                            name).length == 1)
                                    {
                                        // NOTE: adding the observer ends up calling
                                        // addDeltaToSubscriptionCount which publishes the image
                                        if (!Publisher.this.context.addObserver(permissionToken,
                                                ProxyContextMultiplexer.this, name).get().get(
                                                name).booleanValue())
                                        {
                                            throw new IllegalStateException(
                                                    "Could not add ProxyContextMultiplexer as a listener for recordName="
                                                            + name + " using permissionToken="
                                                            + permissionToken);
                                        }
                                    }
                                    else
                                    {
                                        increment.add(name);
                                    }

                                    ackSubscribes.add(name);
                                }
                                else
                                {
                                    Log.log(ProxyContextMultiplexer.this,
                                            "Ignoring duplicate subscribe for recordName=", name);

                                    nokSubscribes.add(name);
                                }
                            }
                            else
                            {
                                Log.log(ProxyContextMultiplexer.this, "Invalid permission token=",
                                        permissionToken, " for recordName=", name);

                                nokSubscribes.add(name);
                            }
                        }
                        catch (Exception e)
                        {
                            Log.log(ProxyContextMultiplexer.this,
                                    "Could not add subscriber using permissionToken=" + permissionToken
                                            + " for recordName=" + name, e);

                            nokSubscribes.add(name);

                            // the subscribe was not completed, so remove the subscriber
                            // registration
                            ProxyContextMultiplexer.this.subscribers.removeSubscriberFor(name, publisher);
                        }
                    }

                    Publisher.this.context.addDeltaToSubscriptionCount(1, increment);

                    for (final String name : increment)
                    {
                        try
                        {
                            // we must send an initial image to the new client if it is
                            // not the first one to register
                            Publisher.this.context.executeSequentialCoreTask(new ISequentialRunnable() {
                                @Override
                                public void run()
                                {
                                    Publisher.this.multiplexer.republishImage(name, publisher);
                                }

                                @Override
                                public Object context()
                                {
                                    return name;
                                }
                            });
                        }
                        catch (Exception e)
                        {
                            Log.log(ProxyContextMultiplexer.this,
                                    "Could not setup initial image publish for recordName=" + name, e);
                        }
                    }
                }
            }
            finally
            {
                task.run();
            }
        }

        void removeSubscriberFor(final Collection<String> names, final ProxyContextPublisher publisher)
        {
            synchronized (Publisher.this.lock)
            {
                final List<String> decrement = new LinkedList<>();
                final List<String> remove = new LinkedList<>();
                for (String name : names)
                {
                    if (ProxyContextMultiplexer.this.subscribers.removeSubscriberFor(name, publisher))
                    {
                        if (ProxyContextMultiplexer.this.subscribers.getSubscribersFor(name).length == 0)
                        {
                            remove.add(name);
                        }
                        else
                        {
                            decrement.add(name);
                        }
                    }
                }
                Publisher.this.context.addDeltaToSubscriptionCount(-1, decrement);
                Publisher.this.context.removeObserver(ProxyContextMultiplexer.this,
                        remove.toArray(new String[0]));

                for (String name : remove)
                {
                    ProxyContextMultiplexer.this.service.endBroadcast(name);
                    this.slowPublishCounts.remove(name);
                    this.lastSlowPublishLogTime.remove(name);
                }
            }
        }

        /**
         * <b>ONLY CALL THIS IN AN {@link ISequentialRunnable} RUNNING IN THE SAME CONTEXT AS THE
         * RECORD NAME! OTHERWISE YOU ARE NOT GUARANTEED TO GET THE LAST PUBLISHED IMAGE.</b>
         * <p>
         *
         * @see Context#getLastPublishedImage_callInRecordContext(String)
         */
        void republishImage(final String recordNameToRepublish, final ProxyContextPublisher publisher)
        {
            final IRecord record =
                    Publisher.this.context.getLastPublishedImage_callInRecordContext(recordNameToRepublish);
            if (record != null)
            {
                final AtomicChange change = new AtomicChange(record);
                if (isSystemRecordUpdateCoalesced(record.getName()))
                {
                    ContextUtils.CORE_EXECUTOR.execute(new ISequentialRunnable() {
                        @Override
                        public Object context()
                        {
                            return getCoalescingContext(record.getName());
                        }

                        @Override
                        public void run()
                        {
                            // NOTE: sequences increment-then-get, hence to send the previous image, we need to subtract 1
                            change.setSequence(ProxyContextMultiplexer.this.systemRecordSequences.get(
                                    recordNameToRepublish).get() - 1);
                            publishImageOnSubscribe(publisher, change);
                        }
                    });
                }
                else
                {
                    publishImageOnSubscribe(publisher, change);
                }
            }
        }

        void publishImageOnSubscribe(final ProxyContextPublisher publisher, final AtomicChange change)
        {
            final String name = change.getName();
            final AtomicChange[] parts = this.teleporter.split(change);
            if (parts != null)
            {
                for (AtomicChange part : parts)
                {
                    publisher.publish(publisher.codec.getTxMessageForAtomicChange(part), true, name);
                }
            }
            else
            {
                publisher.publish(publisher.codec.getTxMessageForAtomicChange(change), true, name);
            }
        }
    }

    String getCoalescingContext(String systemRecord)
    {
        return Publisher.this + "-" + systemRecord;
    }

    /**
     * This is the actual publisher object that publishes record changes to a single {@link ProxyContext}.
     * <p>
     * A scheduled task runs periodically to update the publishing statistics of this.
     *
     * @author Ramon Servadei
     */
    private final class ProxyContextPublisher implements ITransportChannel {
        final ITransportChannel channel;
        final Set<String> subscriptions = Collections.synchronizedSet(new HashSet<>());
        // note: unsynchronized
        final Set<String> firstPublishPending = new HashSet<>();
        final Set<String> firstPublishDone = Collections.synchronizedSet(new HashSet<>());
        final long start;
        /**
         * NOTE: this is only used for handling subscribe and RPC commands. The {@link
         * ProxyContextMultiplexer}'s codec performs the wire-formatting for atomic changes that are sent to
         * this publisher's {@link #publish(byte[], boolean, String)} method.
         */
        final ICodec codec;
        ScheduledFuture statsUpdateTask;
        final AtomicLong messagesPublished = new AtomicLong();
        final AtomicLong bytesPublished = new AtomicLong();
        String identity;
        volatile boolean active;
        boolean codecSyncExpected;

        ProxyContextPublisher(ITransportChannel channel, ICodec codec)
        {
            this.active = true;
            this.codecSyncExpected = true;
            this.codec = codec;
            this.start = System.currentTimeMillis();
            this.channel = channel;

            // add the connection record static parts
            synchronized (Publisher.this.connectionsRecord.getWriteLock())
            {
                final Map<String, IValue> submapConnections =
                        Publisher.this.connectionsRecord.getOrCreateSubMap(
                                getTransmissionStatisticsFieldName(channel));
                final EndPointAddress endPointAddress = Publisher.this.server.getEndPointAddress();
                final String clientSocket = channel.getEndPointDescription();
                submapConnections.put(IContextConnectionsRecordFields.PUBLISHER_ID,
                        TextValue.valueOf(Publisher.this.context.getName()));
                submapConnections.put(IContextConnectionsRecordFields.PUBLISHER_NODE,
                        TextValue.valueOf(endPointAddress.getNode()));
                submapConnections.put(IContextConnectionsRecordFields.PUBLISHER_PORT,
                        LongValue.valueOf(endPointAddress.getPort()));
                submapConnections.put(IContextConnectionsRecordFields.PROXY_ENDPOINT,
                        TextValue.valueOf(clientSocket));
                submapConnections.put(IContextConnectionsRecordFields.PROTOCOL,
                        TextValue.valueOf(this.codec.getClass().getSimpleName()));
                submapConnections.put(IContextConnectionsRecordFields.TRANSPORT,
                        TextValue.valueOf(Publisher.this.getTransportTechnology().toString()));
            }

            Publisher.this.context.publishAtomicChange(ISystemRecordNames.CONTEXT_CONNECTIONS);

            scheduleStatsUpdateTask();

            Log.log(this, "Constructed for ", ObjectUtils.safeToString(channel));
        }

        void scheduleStatsUpdateTask()
        {
            if (this.statsUpdateTask != null)
            {
                this.statsUpdateTask.cancel(false);
            }
            this.statsUpdateTask = ContextExecutorFactory.get(Publisher.this).schedule(new Runnable() {
                long lastMessagesPublished = 0;
                long lastBytesPublished = 0;
                long lastTimeNanos;

                @Override
                public void run()
                {
                    // log any records that have had their first publish done
                    if (logVerboseSubscribes)
                    {
                        synchronized (ProxyContextPublisher.this.firstPublishPending)
                        {
                            logFirstPublishDone();
                        }
                    }

                    final String transmissionStatisticsFieldName =
                            getTransmissionStatisticsFieldName(ProxyContextPublisher.this.channel);
                    if (!Publisher.this.connectionsRecord.getSubMapKeys().contains(
                            transmissionStatisticsFieldName))
                    {
                        ProxyContextPublisher.this.statsUpdateTask.cancel(false);
                        return;
                    }

                    final long nanoTime = System.nanoTime();
                    final long l_messagesPublished = ProxyContextPublisher.this.messagesPublished.get();
                    final long l_bytesPublished = ProxyContextPublisher.this.bytesPublished.get();

                    final long intervalMessagesPublished = l_messagesPublished - this.lastMessagesPublished;
                    final long intervalBytesPublished = l_bytesPublished - this.lastBytesPublished;

                    this.lastMessagesPublished = l_messagesPublished;
                    this.lastBytesPublished = l_bytesPublished;

                    final double perSec = 1000000000d / (nanoTime - this.lastTimeNanos);
                    this.lastTimeNanos = nanoTime;
                    final double inverse_1K = 1 / 1024d;

                    synchronized (Publisher.this.connectionsRecord.getWriteLock())
                    {
                        final Map<String, IValue> submapConnections =
                                Publisher.this.connectionsRecord.getOrCreateSubMap(
                                        transmissionStatisticsFieldName);

                        submapConnections.put(IContextConnectionsRecordFields.MSGS_PER_SEC,
                                LongValue.valueOf((long) (intervalMessagesPublished * perSec)));
                        submapConnections.put(IContextConnectionsRecordFields.KB_PER_SEC, DoubleValue.valueOf(
                                (((long) ((intervalBytesPublished * inverse_1K * perSec) * 10)) / 10d)));
                        submapConnections.put(IContextConnectionsRecordFields.AVG_MSG_SIZE, LongValue.valueOf(
                                l_messagesPublished == 0 ? 0 : l_bytesPublished / l_messagesPublished));
                        submapConnections.put(IContextConnectionsRecordFields.MESSAGE_COUNT,
                                LongValue.valueOf(l_messagesPublished));
                        submapConnections.put(IContextConnectionsRecordFields.KB_COUNT,
                                LongValue.valueOf((long) (l_bytesPublished * inverse_1K)));
                        submapConnections.put(IContextConnectionsRecordFields.SUBSCRIPTION_COUNT,
                                LongValue.valueOf(ProxyContextPublisher.this.subscriptions.size()));
                        submapConnections.put(IContextConnectionsRecordFields.UPTIME, LongValue.valueOf(
                                (long) ((System.currentTimeMillis() - ProxyContextPublisher.this.start)
                                        * 0.001d)));
                        submapConnections.put(IContextConnectionsRecordFields.TX_QUEUE_SIZE,
                                LongValue.valueOf(ProxyContextPublisher.this.channel.getTxQueueSize()));
                        submapConnections.put(IContextConnectionsRecordFields.LAST_INTERVAL_MSG_SIZE,
                                LongValue.valueOf(intervalMessagesPublished == 0 ? 0 :
                                        intervalBytesPublished / intervalMessagesPublished));
                    }

                    if (ProxyContextPublisher.this.active)
                    {
                        ProxyContextPublisher.this.statsUpdateTask =
                                ContextExecutorFactory.get(Publisher.this).schedule(this,
                                        Publisher.this.contextConnectionsRecordPublishPeriodMillis / 2,
                                        TimeUnit.MILLISECONDS);
                    }
                }
            }, Publisher.this.contextConnectionsRecordPublishPeriodMillis / 2, TimeUnit.MILLISECONDS);
        }

        void publish(byte[] txMessage, boolean pointToPoint, String recordName)
        {
            if (pointToPoint)
            {
                send(txMessage);
            }
            this.bytesPublished.addAndGet(txMessage.length);
            this.messagesPublished.incrementAndGet();

            // peek at the size before attempting the synchronize block
            if (logVerboseSubscribes && this.firstPublishPending.size() > 0)
            {
                synchronized (this.firstPublishPending)
                {
                    final int size = this.firstPublishPending.size();
                    if (size > 0)
                    {
                        if (this.firstPublishPending.remove(recordName))
                        {
                            this.firstPublishDone.add(recordName);
                            // size is 1 BEFORE the call to remove, which returned true so now
                            // the size is 0 so no need to test for firstPublishPending.size()==0
                            if (size == 1)
                            {
                                logFirstPublishDone();
                            }
                        }
                    }
                }
            }
        }

        void logFirstPublishDone()
        {
            if (this.firstPublishDone.size() > 0)
            {
                Log.log(ProxyContextPublisher.this, "(->) First publish to [",
                        this.channel.getEndPointDescription(), "] done for ",
                        this.firstPublishDone.toString());
                this.firstPublishDone.clear();
            }
        }

        void subscribe(Collection<String> names, String permissionToken, List<String> ackSubscribes,
                List<String> nokSubscribes, Runnable task)
        {
            try
            {
                this.subscriptions.addAll(names);
                if (logVerboseSubscribes)
                {
                    synchronized (this.firstPublishPending)
                    {
                        this.firstPublishPending.addAll(names);
                    }
                }
                Publisher.this.multiplexer.addSubscriberFor(names, this, permissionToken, ackSubscribes,
                        nokSubscribes, task);
            }
            catch (Exception e)
            {
                Log.log(ProxyContextPublisher.this, "Could not subscribe " + names, e);
            }
        }

        void unsubscribe(Collection<String> names)
        {
            try
            {
                this.subscriptions.removeAll(names);
                if (logVerboseSubscribes)
                {
                    synchronized (this.firstPublishPending)
                    {
                        this.firstPublishPending.removeAll(names);
                    }
                }
                Publisher.this.multiplexer.removeSubscriberFor(names, this);
            }
            catch (Exception e)
            {
                Log.log(ProxyContextPublisher.this, "Could not unsubscribe " + names, e);
            }
        }

        void resync(final String name)
        {
            try
            {
                Publisher.this.context.executeSequentialCoreTask(new ISequentialRunnable() {
                    @Override
                    public void run()
                    {
                        Publisher.this.multiplexer.republishImage(name, ProxyContextPublisher.this);
                    }

                    @Override
                    public Object context()
                    {
                        return name;
                    }
                });
            }
            catch (Exception e)
            {
                Log.log(ProxyContextPublisher.this, "Could not resync " + name, e);
            }
        }

        void destroy()
        {

            long time = System.currentTimeMillis();
            this.active = false;
            this.codec.getSessionProtocol().destroy();
            this.statsUpdateTask.cancel(false);
            Set<String> copy;
            synchronized (this.subscriptions)
            {
                copy = CollectionUtils.newHashSet(this.subscriptions);
            }
            unsubscribe(copy);

            if (Publisher.this.connectionsRecord.removeSubMap(
                    getTransmissionStatisticsFieldName(ProxyContextPublisher.this.channel)) != null)
            {
                Publisher.this.context.publishAtomicChange(ISystemRecordNames.CONTEXT_CONNECTIONS);
            }

            Log.log(this, "Destroyed ", this.identity, ", removed ", Integer.toString(copy.size()),
                    " subscriptions (", Long.toString(System.currentTimeMillis() - time), "ms)");
        }

        @Override
        public void destroy(String reason, Exception... e)
        {
            this.channel.destroy(reason, e);
        }

        void setProxyContextIdentity(String identity)
        {
            this.identity = identity;
            Publisher.this.connectionsRecord.getOrCreateSubMap(
                    getTransmissionStatisticsFieldName(this.channel)).put(
                    IContextConnectionsRecordFields.PROXY_ID, TextValue.valueOf(this.identity));
            Publisher.this.context.publishAtomicChange(ISystemRecordNames.CONTEXT_CONNECTIONS);
        }

        @Override
        public boolean sendAsync(byte[] toSend)
        {
            return send(toSend);
        }

        @Override
        public boolean send(byte[] toSend)
        {
            if (logTx)
            {
                // log first 200 bytes that are sent
                Log.log(ProxyContextPublisher.this, "(->) ",
                        new String(toSend, 0, (Math.min(toSend.length, 200))),
                        (toSend.length < 200 ? "" : "...(truncated)"));
            }
            return this.channel.send(this.codec.finalEncode(toSend));
        }

        @Override
        public boolean isConnected()
        {
            return this.channel.isConnected();
        }

        @Override
        public String getEndPointDescription()
        {
            return this.channel.getEndPointDescription();
        }

        @Override
        public String getDescription()
        {
            return this.channel.getDescription();
        }

        @Override
        public boolean hasRxData()
        {
            return this.channel.hasRxData();
        }

        @Override
        public int getTxQueueSize()
        {
            return this.channel.getTxQueueSize();
        }
    }

    final Map<ITransportChannel, ProxyContextPublisher> proxyContextPublishers;
    final Context context;
    final ICodec mainCodec;
    final IEndPointService server;
    final IRecord connectionsRecord;
    final ProxyContextMultiplexer multiplexer;
    final TransportTechnologyEnum transportTechnology;
    volatile long contextConnectionsRecordPublishPeriodMillis =
            DataFissionProperties.Values.CONNECTIONS_RECORD_PUBLISH_PERIOD_MILLIS;
    ScheduledFuture contextConnectionsRecordPublishTask;
    long messagesPublished;
    long bytesPublished;

    /**
     * Constructs the publisher and creates an {@link IEndPointService} to accept connections from {@link
     * ProxyContext} objects.
     * <p>
     * This uses the transport technology defined by the system property <code>-Dtransport</code>
     *
     * @param context the context the publisher is for
     * @param codec   the codec to use for sending/receiving messages from the {@link ProxyContext}
     * @param node    the node for the {@link EndPointAddress} of this publisher
     * @param port    the port for the {@link EndPointAddress} of this publisher
     * @see TransportTechnologyEnum
     */
    public Publisher(Context context, ICodec codec, String node, int port)
    {
        this(context, codec, node, port, TransportTechnologyEnum.getDefaultFromSystemProperty());
    }

    /**
     * Constructs the publisher and creates an {@link IEndPointService} to accept connections from {@link
     * ProxyContext} objects. This constructor provides the {@link TransportTechnologyEnum} to use.
     *
     * @param context             the context the publisher is for
     * @param codec               the codec to use for sending/receiving messages from the {@link
     *                            ProxyContext}
     * @param node                the node for the {@link EndPointAddress} of this publisher
     * @param port                the port for the {@link EndPointAddress} of this publisher
     * @param transportTechnology the enum expressing the transport technology to use
     */
    public Publisher(Context context, ICodec codec, String node, int port,
            TransportTechnologyEnum transportTechnology)
    {
        super();
        this.context = context;
        this.transportTechnology = transportTechnology;
        this.lock = new Object();
        this.proxyContextPublishers = new ConcurrentHashMap<>();
        this.connectionsRecord =
                Context.getRecordInternal(this.context, ISystemRecordNames.CONTEXT_CONNECTIONS);

        this.mainCodec = codec;
        this.server =
                transportTechnology.constructEndPointServiceBuilder(this.mainCodec.getFrameEncodingFormat(),
                        new EndPointAddress(node, port)).buildService(new IReceiver() {
                    @Override
                    public void onChannelConnected(final ITransportChannel channel)
                    {
                        Publisher.this.context.executeSequentialCoreTask(new ISequentialRunnable() {
                            @Override
                            public void run()
                            {
                                // synchronize to avoid race conditions that can remove the static
                                // portions of the connections of a proxyContextPublisher for one that
                                // is in the process of being constructed
                                synchronized (Publisher.this.proxyContextPublishers)
                                {
                                    Publisher.this.proxyContextPublishers.put(channel,
                                            new ProxyContextPublisher(channel,
                                                    Publisher.this.mainCodec.newInstance()));
                                }
                            }

                            @Override
                            public Object context()
                            {
                                return channel;
                            }
                        });
                    }

                    @Override
                    public void onDataReceived(final ByteBuffer data, final ITransportChannel source)
                    {
                        Publisher.this.context.executeSequentialCoreTask(new ISequentialRunnable() {
                            @SuppressWarnings("unchecked")
                            @Override
                            public void run()
                            {
                                final ProxyContextPublisher proxyContextPublisher =
                                        getProxyContextPublisher(source);
                                final ICodec channelsCodec = proxyContextPublisher.codec;
                                if (proxyContextPublisher.codecSyncExpected)
                                {
                                    final SyncResponse response =
                                            channelsCodec.getSessionProtocol().handleSessionSyncData(data);
                                    Log.log(Publisher.this, "(<-) SYNC RESP ",
                                            ObjectUtils.safeToString(source));
                                    if (response.syncDataResponse != null)
                                    {
                                        proxyContextPublisher.channel.send(response.syncDataResponse);
                                        Log.log(Publisher.class, "(->) SYNC RESP ",
                                                ObjectUtils.safeToString(source));
                                    }
                                    if (!response.syncFailed)
                                    {
                                        if (response.syncComplete)
                                        {
                                            proxyContextPublisher.codecSyncExpected = false;
                                            Log.log(Publisher.class, "SESSION SYNCED ",
                                                    ObjectUtils.safeToString(source));
                                        }
                                    }
                                    else
                                    {
                                        proxyContextPublisher.channel.destroy("SYNC FAILED");
                                    }
                                    return;
                                }
                                final Object decodedMessage = channelsCodec.decode(data);
                                final CommandEnum command = channelsCodec.getCommand(decodedMessage);
                                if (log)
                                {
                                    final int maxLogLength = 128;
                                    if (decodedMessage instanceof char[])
                                    {
                                        if (((char[]) decodedMessage).length < maxLogLength)
                                        {
                                            Log.log(Publisher.this, "(<-) '",
                                                    new String((char[]) decodedMessage), "' from ",
                                                    ObjectUtils.safeToString(source));
                                        }
                                        else
                                        {
                                            Log.log(Publisher.this, "(<-) '",
                                                    new String((char[]) decodedMessage, 0, maxLogLength),
                                                    "...(too long)' from ", ObjectUtils.safeToString(source));
                                        }
                                    }
                                    else
                                    {
                                        Log.log(Publisher.this, "(<-) ", command.toString(), " from ",
                                                ObjectUtils.safeToString(source));
                                    }
                                }
                                switch(command)
                                {
                                    case RPC:
                                        rpc(decodedMessage, source);
                                        break;
                                    case IDENTIFY:
                                        identify(channelsCodec.getIdentityArgumentFromDecodedMessage(
                                                decodedMessage), source);
                                        break;
                                    case SUBSCRIBE:
                                        subscribe(channelsCodec.getSubscribeArgumentsFromDecodedMessage(
                                                decodedMessage), source);
                                        break;
                                    case UNSUBSCRIBE:
                                        unsubscribe(channelsCodec.getUnsubscribeArgumentsFromDecodedMessage(
                                                decodedMessage), source);
                                        break;
                                    case RESYNC:
                                        resync(channelsCodec.getResyncArgumentsFromDecodedMessage(
                                                decodedMessage), source);
                                        break;
                                    case NOOP:
                                        break;
                                }
                            }

                            @Override
                            public Object context()
                            {
                                return source;
                            }
                        });
                    }

                    @Override
                    public void onChannelClosed(final ITransportChannel channel)
                    {
                        Publisher.this.context.executeSequentialCoreTask(new ISequentialRunnable() {
                            @Override
                            public void run()
                            {
                                ProxyContextPublisher clientPublisher;
                                synchronized (Publisher.this.proxyContextPublishers)
                                {
                                    clientPublisher = Publisher.this.proxyContextPublishers.remove(channel);
                                }
                                if (clientPublisher != null)
                                {
                                    clientPublisher.destroy();
                                }
                            }

                            @Override
                            public Object context()
                            {
                                return channel;
                            }
                        });
                    }
                });

        this.multiplexer = new ProxyContextMultiplexer(this.server);

        // prepare to periodically publish status changes
        this.publishContextConnectionsRecordAtPeriod(this.contextConnectionsRecordPublishPeriodMillis);
    }

    public long getContextConnectionsRecordPublishPeriodMillis()
    {
        return this.contextConnectionsRecordPublishPeriodMillis;
    }

    /**
     * Publish the {@link ISystemRecordNames#CONTEXT_CONNECTIONS} record at the given period in milliseconds
     */
    public synchronized void publishContextConnectionsRecordAtPeriod(
            long contextConnectionsRecordPublishPeriodMillis)
    {
        this.contextConnectionsRecordPublishPeriodMillis = contextConnectionsRecordPublishPeriodMillis;
        if (this.contextConnectionsRecordPublishTask != null)
        {
            this.contextConnectionsRecordPublishTask.cancel(false);
        }
        final Runnable contextConnectionsPublishTask = new Runnable() {
            CountDownLatch publishAtomicChange = new CountDownLatch(0);

            @Override
            public void run()
            {
                if (this.publishAtomicChange.getCount() == 0)
                {
                    synchronized (Publisher.this.proxyContextPublishers)
                    {
                        // check each connection is still active - remove if not
                        final Set<String> connectionIds =
                                new HashSet<>(Publisher.this.connectionsRecord.getSubMapKeys());
                        final Set<ITransportChannel> channels =
                                Publisher.this.proxyContextPublishers.keySet();
                        for (ITransportChannel channel : channels)
                        {
                            connectionIds.remove(getTransmissionStatisticsFieldName(channel));
                        }
                        for (String connectionId : connectionIds)
                        {
                            Publisher.this.connectionsRecord.removeSubMap(connectionId);
                        }
                    }

                    this.publishAtomicChange = Publisher.this.context.publishAtomicChange(
                            ISystemRecordNames.CONTEXT_CONNECTIONS);
                }
            }
        };
        this.contextConnectionsRecordPublishTask =
                ContextExecutorFactory.get(Publisher.this).scheduleWithFixedDelay(contextConnectionsPublishTask,
                        this.contextConnectionsRecordPublishPeriodMillis,
                        this.contextConnectionsRecordPublishPeriodMillis, TimeUnit.MILLISECONDS);

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
                + this.proxyContextPublishers.keySet().size() + ", messages published="
                + this.messagesPublished + ", bytes published=" + this.bytesPublished + "]";
    }

    public void destroy()
    {
        ContextExecutorFactory.remove(Publisher.this);
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
        this.context.executeRpcTask(new ISequentialRunnable() {
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

    void unsubscribe(List<String> recordNames, ITransportChannel client)
    {
        Log.log(this, "(<-) unsubscribe from [", client.getEndPointDescription(), "] for ",
                (logVerboseSubscribes || recordNames.size() == 1 ? ObjectUtils.safeToString(recordNames) :
                        Integer.toString(recordNames.size())));
        final ProxyContextPublisher proxyContextPublisher = getProxyContextPublisher(client);
        proxyContextPublisher.unsubscribe(recordNames);
        sendAck(recordNames, client, proxyContextPublisher, ProxyContext.UNSUBSCRIBE);
    }

    void resync(List<String> recordNames, ITransportChannel client)
    {
        Log.log(this, "(<-) re-sync from [", client.getEndPointDescription(), "] for ",
                (logVerboseSubscribes || recordNames.size() == 1 ? ObjectUtils.safeToString(recordNames) :
                        Integer.toString(recordNames.size())));
        final ProxyContextPublisher proxyContextPublisher = getProxyContextPublisher(client);

        this.context.executeSequentialCoreTask(new ISequentialRunnable() {
            @Override
            public Object context()
            {
                return Publisher.this;
            }

            @Override
            public void run()
            {
                recordNames.forEach(n -> {
                    proxyContextPublisher.resync(n);
                    if (DataFissionProperties.Values.SUBSCRIBE_DELAY_MICROS > 0)
                    {
                        LockSupport.parkNanos(DataFissionProperties.Values.SUBSCRIBE_DELAY_MICROS * 1000);
                    }
                });
            }
        });
    }

    void subscribe(final List<String> recordNames, final ITransportChannel client)
    {
        // the first item is always the permission token
        final String permissionToken = recordNames.remove(0);

        // break up into batches
        int batchSize = DataFissionProperties.Values.SUBSCRIBE_BATCH_SIZE;
        int batchCounter = 0;
        List<String> batchSubscribeRecordNames = new ArrayList<>(batchSize);
        final int size = recordNames.size();
        int i;
        for (i = 0; i < size; i++)
        {
            batchSubscribeRecordNames.add(recordNames.get(i));
            if (++batchCounter == batchSize)
            {
                subscribeBatch(batchSubscribeRecordNames, client, permissionToken, i + 1, size);
                batchSubscribeRecordNames = new ArrayList<>(batchSize);
                batchCounter = 0;
            }
        }
        if (batchSubscribeRecordNames.size() > 0)
        {
            subscribeBatch(batchSubscribeRecordNames, client, permissionToken, i, size);
        }
    }

    private void subscribeBatch(final List<String> recordNames, final ITransportChannel client,
            final String permissionToken, int current, int total)
    {
        final String subscribeKey = Long.toString(this.subscribeCounter.incrementAndGet());
        Log.log(this, "(<-) subscribe #", subscribeKey, " (", Integer.toString(current), "/",
                Integer.toString(total), ") from [", client.getEndPointDescription(), "]",
                (logVerboseSubscribes || recordNames.size() == 1 ? ObjectUtils.safeToString(recordNames) :
                        ""));

        final ProxyContextPublisher proxyContextPublisher = getProxyContextPublisher(client);
        final List<String> ackSubscribes = new LinkedList<>();
        final List<String> nokSubscribes = new LinkedList<>();
        final Runnable finallyTask = () -> {
            if (ackSubscribes.size() + nokSubscribes.size() == recordNames.size())
            {
                Log.log(Publisher.this, "(->) subscribe #", subscribeKey, " complete ",
                        Integer.toString(ackSubscribes.size()), ":", Integer.toString(nokSubscribes.size()));
                sendAck(ackSubscribes, client, proxyContextPublisher, ProxyContext.SUBSCRIBE);
                sendNok(nokSubscribes, client, proxyContextPublisher);
            }
        };

        proxyContextPublisher.subscribe(recordNames, permissionToken, ackSubscribes, nokSubscribes,
                finallyTask);
    }

    void sendAck(List<String> recordNames, ITransportChannel client,
            ProxyContextPublisher proxyContextPublisher, String responseAction)
    {
        sendSubscribeResult(ProxyContext.ACK, recordNames, client, proxyContextPublisher, responseAction);
    }

    void sendNok(List<String> recordNames, ITransportChannel client,
            ProxyContextPublisher proxyContextPublisher)
    {
        sendSubscribeResult(ProxyContext.NOK, recordNames, client, proxyContextPublisher,
                ProxyContext.SUBSCRIBE);
    }

    private void sendSubscribeResult(String action, List<String> recordNames, ITransportChannel client,
            ProxyContextPublisher proxyContextPublisher, String responseAction)
    {
        if (recordNames.size() == 0)
        {
            return;
        }

        final AtomicChange atomicChange = new AtomicChange(action + responseAction);
        final Map<String, IValue> puts = atomicChange.internalGetPutEntries();
        final LongValue dummy = LongValue.valueOf(1);
        for (String recordName : recordNames)
        {
            puts.put(recordName, dummy);
        }

        if (log)
        {
            Log.log(Publisher.this, "(->) ", ObjectUtils.safeToString(atomicChange), " to [",
                    client.getEndPointDescription(), "]");
        }
        client.send(proxyContextPublisher.codec.finalEncode(
                proxyContextPublisher.codec.getTxMessageForAtomicChange(atomicChange)));
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

    public TransportTechnologyEnum getTransportTechnology()
    {
        return this.transportTechnology;
    }

    public void disconnectClients(String reason, Function<String, Boolean> connectionIdentityCheck)
    {
        for (Entry<ITransportChannel, ProxyContextPublisher> entry : this.proxyContextPublishers.entrySet())
        {
            if (Boolean.TRUE == connectionIdentityCheck.apply(entry.getValue().identity))
            {
                entry.getKey().destroy(reason);
            }
        }
    }
}
