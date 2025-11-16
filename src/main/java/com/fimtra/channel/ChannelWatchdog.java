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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.Pair;
import com.fimtra.util.SystemUtils;
import com.fimtra.util.ThreadUtils;

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
    private static int getMissedHeartbeats()
    {
        return SystemUtils.getPropertyAsInt("ChannelWatchdog.missedHbCount", 3);
    }

    int heartbeatPeriodMillis;
    int missedHeartbeatCount;
    final Set<ITransportChannel> channels;
    /** Tracks channels that receive a HB */
    final Set<ITransportChannel> channelsReceivingHeartbeat;
    final Map<ITransportChannel, Integer> channelsMissingHeartbeat;

    private final ScheduledExecutorService executor =
            ThreadUtils.newScheduledExecutorService("channel-watchdog", 1);

    private volatile ScheduledFuture<?> current;

    public ChannelWatchdog()
    {
        super();
        this.channels = Collections.synchronizedSet(new HashSet<>());
        this.channelsReceivingHeartbeat = Collections.synchronizedSet(new HashSet<>());
        this.channelsMissingHeartbeat = new HashMap<>();
        configure(SystemUtils.getPropertyAsInt("ChannelWatchdog.periodMillis", 30_000), getMissedHeartbeats());
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
        configure(periodMillis, getMissedHeartbeats());
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
        synchronized (this.channels)
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
        if (channels.add(channel))
        {
            this.executor.execute(() -> channel.send(ChannelUtils.HEARTBEAT_SIGNAL));
        }
    }

    @Override
    public void run()
    {
        final Collection<ITransportChannel> channelsCopy;
        synchronized (this.channels)
        {
            channelsCopy = new ArrayList<>(this.channels);
        }
        Integer count;
        for (ITransportChannel channel : channelsCopy)
        {
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
                    if (channel.hasRxData() || this.channelsReceivingHeartbeat.contains(channel))
                    {
                        checkHeartbeatRecovered(channel);
                    }
                    else
                    {
                        count = this.channelsMissingHeartbeat.get(channel);
                        if (count == null)
                        {
                            count = 1;
                        }
                        else
                        {
                            count = count + 1;
                            if (count >= this.missedHeartbeatCount)
                            {
                                channel.destroy(
                                        "Missed " + count + "/" + this.missedHeartbeatCount + " heartbeats");
                                stopMonitoring(channel);
                            }
                            else
                            {
                                Log.log(this, "Missed heartbeat ", count.toString(), "/",
                                        Integer.toString(this.missedHeartbeatCount), " from ",
                                        ObjectUtils.safeToString(channel));
                            }
                        }
                        this.channelsMissingHeartbeat.put(channel, count);
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
        if (channels.remove(channel))
        {
            this.channelsReceivingHeartbeat.remove(channel);
            this.channelsMissingHeartbeat.remove(channel);
        }
    }

    public void onHeartbeat(final ITransportChannel channel)
    {
        this.channelsReceivingHeartbeat.add(channel);
    }

    void checkHeartbeatRecovered(ITransportChannel channel)
    {
        final Integer removed = ChannelWatchdog.this.channelsMissingHeartbeat.remove(channel);
        if (removed != null && removed > 1)
        {
            Log.log(this, "Heartbeat recovered for ", ObjectUtils.safeToString(channel));
        }
    }

    /**
     * @return a {@link List} of {@link Pair} objects of
     *         {@link Integer}={@link ITransportChannel#getTxQueueSize()} and
     *         {@link String}={@link ITransportChannel#getDescription()}
     */
    public List<Pair<Integer, String>> getChannelStats()
    {
        final Collection<ITransportChannel> localChannelsRef;
        synchronized (this.channels)
        {
            localChannelsRef = new ArrayList<>(this.channels);
        }
        final List<Pair<Integer, String>> stats = new ArrayList<>(localChannelsRef.size());
        for (ITransportChannel channel : localChannelsRef)
        {
            stats.add(new Pair<>(channel.getTxQueueSize(), channel.getDescription()));
        }
        return stats;
    }
}
