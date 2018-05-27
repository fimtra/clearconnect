/*
 * Copyright (c) 2018 Ramon Servadei
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
package com.fimtra.tcpchannel;

import static com.fimtra.tcpchannel.TcpChannelProperties.Values.SEND_QUEUE_THRESHOLD;
import static com.fimtra.tcpchannel.TcpChannelProperties.Values.SEND_QUEUE_THRESHOLD_BREACH_MILLIS;

import java.util.Deque;

import com.fimtra.util.Log;

/**
 * A simple utility class that monitors queue levels and upgrades/downgrades warning levels as the
 * queue size alters.
 * 
 * @author Ramon Servadei
 */
final class QueueThresholdMonitor
{
    private static final double _40_WATERMARK = SEND_QUEUE_THRESHOLD * 0.4;
    private static final double _50_WATERMARK = SEND_QUEUE_THRESHOLD * 0.5;
    private static final double _60_WATERMARK = SEND_QUEUE_THRESHOLD * 0.6;
    private static final double _75_WATERMARK = SEND_QUEUE_THRESHOLD * 0.75;
    private static final double _80_WATERMARK = SEND_QUEUE_THRESHOLD * 0.8;
    private static final double _90_WATERMARK = SEND_QUEUE_THRESHOLD * 0.9;

    final Object target;
    final long sendQThresholdBreachNanos;
    int thresholdWarningLevel;
    int queueSizeAtLastCheckPoint;
    long thresholdMaxBreachStartTimeNanos;

    QueueThresholdMonitor(Object target, long sendQueueThresholdBreachMillis)
    {
        this.target = target;
        this.sendQThresholdBreachNanos = sendQueueThresholdBreachMillis * 1000000;
    }

    /**
     * @return <code>true</code> if the size of the combined queues has breached a maximum level for
     *         too long
     */
    final boolean checkQueueSize(final Deque<TxByteArrayFragment> pendingTxFrames,
        final Deque<TxByteArrayFragment> sendingTxFrames)
    {
        return checkQSize(pendingTxFrames.size() + sendingTxFrames.size());
    }

    final boolean checkQSize(final int size)
    {
        switch(this.thresholdWarningLevel)
        {
            case 0:
                if (size > _90_WATERMARK)
                {
                    upgrade(3, size);
                }
                else if (size > _75_WATERMARK)
                {
                    upgrade(2, size);
                }
                else if (size > _50_WATERMARK)
                {
                    upgrade(1, size);
                }
                break;
            case 1:
                if (size > _90_WATERMARK)
                {
                    upgrade(3, size);
                }
                else if (size > _75_WATERMARK)
                {
                    upgrade(2, size);
                }
                else if (size < _40_WATERMARK)
                {
                    downgrade(0, size);
                }
                break;
            case 2:
                if (size > _90_WATERMARK)
                {
                    upgrade(3, size);
                }
                else if (size < _40_WATERMARK)
                {
                    downgrade(0, size);
                }
                else if (size < _60_WATERMARK)
                {
                    downgrade(1, size);
                }
                break;
            case 3:
                if (size < _40_WATERMARK)
                {
                    this.thresholdMaxBreachStartTimeNanos = 0;
                    downgrade(0, size);
                }
                else if (size < _60_WATERMARK)
                {
                    this.thresholdMaxBreachStartTimeNanos = 0;
                    downgrade(1, size);
                }
                else if (size < _80_WATERMARK)
                {
                    this.thresholdMaxBreachStartTimeNanos = 0;
                    downgrade(2, size);
                }
                break;
        }

        if (size > SEND_QUEUE_THRESHOLD)
        {
            if (this.thresholdMaxBreachStartTimeNanos == 0)
            {
                this.thresholdMaxBreachStartTimeNanos = System.nanoTime();
                this.queueSizeAtLastCheckPoint = size;
            }
            else if ((System.nanoTime() - this.thresholdMaxBreachStartTimeNanos) > this.sendQThresholdBreachNanos)
            {
                if (size < this.queueSizeAtLastCheckPoint)
                {
                    Log.log(this.target, "Queue draining, was ", Integer.toString(this.queueSizeAtLastCheckPoint),
                        " now ", Integer.toString(size), " for ", this.target.toString());
                    this.queueSizeAtLastCheckPoint = size;
                }
                else
                {
                    Log.log(this.target,
                        "Queue too large: " + size + " after waiting " + SEND_QUEUE_THRESHOLD_BREACH_MILLIS + "ms, was "
                            + Integer.toString(this.queueSizeAtLastCheckPoint));
                    return true;
                }
            }
        }

        return false;
    }

    private final void downgrade(final int level, final int size)
    {
        Log.log(this.target, "[-] Q level ", Integer.toString(this.thresholdWarningLevel), "->",
            Integer.toString(level), " size=", Integer.toString(size), " ", this.target.toString());
        this.thresholdWarningLevel = level;
    }

    private final void upgrade(final int level, final int size)
    {
        Log.log(this.target, "[+] Q level ", Integer.toString(this.thresholdWarningLevel), "->",
            Integer.toString(level), " size=", Integer.toString(size), " ", this.target.toString());
        this.thresholdWarningLevel = level;
    }

}
