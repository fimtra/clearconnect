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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fimtra.util.Log;

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
    int heartbeatPeriodMillis;
    int missedHeartbeatCount;
    final Set<ITransportChannel> channels;
    /** Tracks channels that receive a HB */
    final Set<ITransportChannel> channelsReceivingHeartbeat;
    final Map<ITransportChannel, Integer> channelsMissingHeartbeat;

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
        this.channels = new CopyOnWriteArraySet<ITransportChannel>();
        this.channelsReceivingHeartbeat = new HashSet<ITransportChannel>();
        this.channelsMissingHeartbeat = new HashMap<ITransportChannel, Integer>();
        configure(Integer.parseInt(System.getProperty("ChannelWatchdog.periodMillis", "5000")),
            Integer.parseInt(System.getProperty("ChannelWatchdog.missedHbCount", "3")));
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
        configure(periodMillis, Integer.parseInt(System.getProperty("ChannelWatchdog.missedHbCount", "3")));
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
        if (this.current != null)
        {
            this.current.cancel(false);
        }
        this.heartbeatPeriodMillis = periodMillis;
        this.missedHeartbeatCount = missedHeartbeats;
        this.current =
            this.executor.scheduleWithFixedDelay(this, this.heartbeatPeriodMillis, this.heartbeatPeriodMillis,
                TimeUnit.MILLISECONDS);
        Log.log(this, "Heartbeat period is ", Integer.toString(this.heartbeatPeriodMillis),
            "ms, missed heartbeat count is ", Integer.toString(this.missedHeartbeatCount));
    }

    /**
     * Add the channel to be monitored by this watchdog. This immediately sends a heartbeat down
     * this channel.
     */
    public void addChannel(final ITransportChannel channel)
    {
        // Log.log(this, "Monitoring " + channel);
        this.channels.add(channel);
        this.executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                channel.sendAsync(ChannelUtils.HEARTBEAT_SIGNAL);
            }
        });
    }

    @Override
    public void run()
    {
        for (ITransportChannel channel : this.channels)
        {
            try
            {
                // send HB
                if (!channel.sendAsync(ChannelUtils.HEARTBEAT_SIGNAL))
                {
                    channel.destroy("Could not send heartbeat");
                    stopMonitoring(channel);
                }
                else
                {
                    // if the channel has received data, then its still alive...
                    if (channel.hasRxData())
                    {
                        this.channelsMissingHeartbeat.remove(channel);
                    }
                    else
                    {
                        // now check for missed heartbeat
                        Integer missedCount = this.channelsMissingHeartbeat.get(channel);
                        if (missedCount != null && missedCount.intValue() > this.missedHeartbeatCount)
                        {
                            channel.destroy("Heartbeat not received");
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
        if (this.channels.remove(channel))
        {
            ChannelWatchdog.this.channelsReceivingHeartbeat.remove(channel);
            ChannelWatchdog.this.channelsMissingHeartbeat.remove(channel);
            // Log.log(this, "Not monitoring " + channel);
        }
    }

    public void onHeartbeat(final ITransportChannel channel)
    {
        this.executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                if (ChannelWatchdog.this.channels.contains(channel))
                {
                    ChannelWatchdog.this.channelsReceivingHeartbeat.add(channel);
                    ChannelWatchdog.this.channelsMissingHeartbeat.remove(channel);
                }
            }
        });
    }
}
