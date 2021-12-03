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
package com.fimtra.channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.Pair;
import com.fimtra.util.SystemUtils;

/**
 * This class checks that the {@link ITransportChannel} objects it knows about are still alive. This
 * is done by periodically sending a heartbeat message to each channel it knows about and listening
 * for heartbeats received on each channel.
 * <p>
 * The watchdog can be configured to allow a specified number of missed heartbeats before closing a
 * channel.
 * <p>
 * Can be configured with the following system properties:
 * 
 * <pre>
 * -DChannelWatchdog.periodMillis={period in milliseconds for heartbeats}
 * -DChannelWatchdog.missedHbCount={missed heartbeats}
 * </pre>
 * 
 * @author Ramon Servadei
 */
public final class ChannelWatchdog implements Runnable
{
    /** The tolerance to allow before logging if a received heartbeat is too early or too late. */
    static final long HB_TOLERANCE_MILLIS =
            SystemUtils.getPropertyAsLong("channelWatchdog.hbToleranceMillis", 1000);

    int heartbeatPeriodMillis;
    long lateHeartbeatLimit;
    int missedHeartbeatCount;
    volatile Set<ITransportChannel> channels;
    /** Tracks channels that receive a HB */
    final Set<ITransportChannel> channelsReceivingHeartbeat;
    final Map<ITransportChannel, Integer> channelsMissingHeartbeat;
    final Map<ITransportChannel, Long> channelsHeartbeatArrivalTime;
    final Object lock;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory()
    {
        private final AtomicInteger threadNumber = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r)
        {
            Thread t = new Thread(r, "channel-watchdog-" + this.threadNumber.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    });

    private ScheduledFuture<?> current;

    public ChannelWatchdog()
    {
        super();
        this.lock = new Object();
        this.channels = new HashSet<>();
        this.channelsReceivingHeartbeat = new HashSet<>();
        this.channelsMissingHeartbeat = new HashMap<>();
        this.channelsHeartbeatArrivalTime = new ConcurrentHashMap<>();
        configure(SystemUtils.getPropertyAsInt("ChannelWatchdog.periodMillis", 30_000),
                SystemUtils.getPropertyAsInt("ChannelWatchdog.missedHbCount", 3));
    }

    public int getHeartbeatPeriodMillis()
    {
        return this.heartbeatPeriodMillis;
    }

    public int getMissedHeartbeatCount()
    {
        return this.missedHeartbeatCount;
    }

    /**
     * Configure the watchdog period. This also defines 3 allowed missed heartbeats.
     * 
     * @param periodMillis
     *            the period to scan for heartbeats and to send heartbeats down each channel
     * @see #configure(int, int)
     */
    public void configure(int periodMillis)
    {
        configure(periodMillis, SystemUtils.getPropertyAsInt("ChannelWatchdog.missedHbCount", 3));
    }

    /**
     * Configure the watchdog period and heartbeat
     * 
     * @param periodMillis
     *            the period to scan for heartbeats and to send heartbeats down each channel
     * @param missedHeartbeats
     *            the number of allowed missed heartbeats for a channel
     */
    public void configure(int periodMillis, int missedHeartbeats)
    {
        synchronized (this.lock)
        {
            if (this.heartbeatPeriodMillis == periodMillis && this.missedHeartbeatCount == missedHeartbeats)
            {
                return;
            }

            if (this.current != null)
            {
                this.current.cancel(false);
            }
            this.heartbeatPeriodMillis = periodMillis;
            this.lateHeartbeatLimit = this.heartbeatPeriodMillis + HB_TOLERANCE_MILLIS;
            this.missedHeartbeatCount = missedHeartbeats;
            this.current = this.executor.scheduleWithFixedDelay(this, this.heartbeatPeriodMillis,
                this.heartbeatPeriodMillis, TimeUnit.MILLISECONDS);
            Log.log(this, "Heartbeat period is ", Integer.toString(this.heartbeatPeriodMillis),
                "ms, missed heartbeat count is ", Integer.toString(this.missedHeartbeatCount));
        }
    }

    /**
     * Add the channel to be monitored by this watchdog. This immediately sends a heartbeat down
     * this channel.
     */
    public void addChannel(final ITransportChannel channel)
    {
        synchronized (this.lock)
        {
            final Set<ITransportChannel> copy = new HashSet<>(this.channels);
            copy.add(channel);
            this.channels = copy;
        }
        this.executor.execute(() -> channel.send(ChannelUtils.HEARTBEAT_SIGNAL));
    }

    @Override
    public void run()
    {
        final ScheduledFuture<?> ref;
        synchronized (this.lock)
        {
            ref = this.current;
        }
        for (ITransportChannel channel : this.channels)
        {
            if (ref.isCancelled())
            {
                this.channelsMissingHeartbeat.clear();
                return;
            }

            try
            {
                // send HB
                if (!channel.send(ChannelUtils.HEARTBEAT_SIGNAL))
                {
                    channel.destroy("Could not send heartbeat");
                    stopMonitoring(channel);
                }
                else
                {
                    // if the channel has received data, then its still alive...
                    if (channel.hasRxData())
                    {
                        checkHeartbeatRecovered(channel);
                    }
                    else
                    {
                        // now check for missed heartbeat
                        Integer missedCount = this.channelsMissingHeartbeat.get(channel);
                        if (missedCount != null && missedCount.intValue() >= this.missedHeartbeatCount)
                        {
                            channel.destroy(
                                "Missed " + missedCount.intValue() + "/" + this.missedHeartbeatCount + " heartbeats");
                            stopMonitoring(channel);
                        }

                        // prepare for missed heartbeat on the next cycle
                        if (!this.channelsReceivingHeartbeat.contains(channel))
                        {
                            Integer count = missedCount;
                            if (count == null)
                            {
                                count = Integer.valueOf(1);
                            }
                            else
                            {
                                count = Integer.valueOf(count.intValue() + 1);
                            }
                            if (count.intValue() > 1)
                            {
                                Log.log(this, "Missed heartbeat ", count.toString(), "/",
                                    Integer.toString(this.missedHeartbeatCount), " from ",
                                    ObjectUtils.safeToString(channel));
                            }
                            this.channelsMissingHeartbeat.put(channel, count);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                channel.destroy("Could not verify channel status", e);
                stopMonitoring(channel);
            }
        }
        this.channelsReceivingHeartbeat.clear();
    }

    /**
     * @param channel
     *            the channel to stop monitoring
     */
    private void stopMonitoring(ITransportChannel channel)
    {
        final boolean remove;
        synchronized (this.lock)
        {
            final Set<ITransportChannel> copy = new HashSet<>(this.channels);
            remove = copy.remove(channel);
            this.channels = copy;
        }
        if (remove)
        {
            this.channelsReceivingHeartbeat.remove(channel);
            this.channelsMissingHeartbeat.remove(channel);
            this.channelsHeartbeatArrivalTime.remove(channel);
        }
    }

    public void onHeartbeat(final ITransportChannel channel)
    {
        final long timeIn = System.nanoTime();
        // grab the previous time now, excludes latency in the executor 
        final Long previous = this.channelsHeartbeatArrivalTime.put(channel, Long.valueOf(timeIn));

        this.executor.execute(() -> {
            if (!this.channels.contains(channel))
            {
                this.channelsHeartbeatArrivalTime.remove(channel);
            }
            else if (previous != null)
            {
                final long hbDelta = (long) ((timeIn - previous.longValue()) * 0.000001d);
                if (hbDelta > this.lateHeartbeatLimit)
                {
                    Log.log(ChannelWatchdog.this, "LATE heartbeat ", Long.toString(hbDelta), "ms from ",
                        ObjectUtils.safeToString(channel));
                }

                this.channelsReceivingHeartbeat.add(channel);
                checkHeartbeatRecovered(channel);
            }
        });
    }

    void checkHeartbeatRecovered(ITransportChannel channel)
    {
        final Integer removed = ChannelWatchdog.this.channelsMissingHeartbeat.remove(channel);
        if (removed != null)
        {
            if (removed.intValue() > 1)
            {
                Log.log(this, "Heartbeat recovered for ", ObjectUtils.safeToString(channel));
            }
        }
    }

    /**
     * @return a {@link List} of {@link Pair} objects of
     *         {@link Integer}={@link ITransportChannel#getTxQueueSize()} and
     *         {@link String}={@link ITransportChannel#getDescription()}
     */
    public List<Pair<Integer, String>> getChannelStats()
    {
        final Set<ITransportChannel> localChannelsRef = this.channels;
        final List<Pair<Integer, String>> stats = new ArrayList<>(localChannelsRef.size());
        for (ITransportChannel channel : localChannelsRef)
        {
            stats.add(new Pair<>(Integer.valueOf(channel.getTxQueueSize()), channel.getDescription()));
        }
        return stats;
    }
}
