/*
 * Copyright (c) 2017 Ramon Servadei
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import com.fimtra.util.Log;

/**
 * Provides throttling logic for event publishing in a {@link Context}.
 * 
 * @author Ramon Servadei
 */
class ContextThrottle
{
    private static final int ONE_SECOND_NANOS = 1000000000;

    /** The threshold for throttling to kick in */
    final int threshold;
    final AtomicInteger eventCount;
    /**
     * Threads that are exempt from throttling for a period of time. <br>
     * Key=thread ID, Value=time in nanos when exemption started
     */
    final Map<Long, Long> exemptThreads;

    ContextThrottle(int threshold, AtomicInteger eventCount)
    {
        this.threshold = threshold;
        this.eventCount = eventCount;
        this.exemptThreads = Collections.synchronizedMap(new HashMap<Long, Long>());
    }

    /**
     * Tracks the start of an event. If the event count is below the {@link #threshold} then this
     * method adds to the event count and returns. If the count is above the threshold, the method
     * blocks until either:
     * <ul>
     * <li>A time period has elapsed (default is 1 second) <b>OR</b>
     * <li>The event count goes down by at least 1
     * </ul>
     * If the full time period was taken and the event count did not go down, then the thread is
     * granted an "exemption" from throttling for 1 second. This is to protect from deadlocks where
     * the thread generating the events is holding a resource that the queue draining threads are
     * waiting to obtain before being able to execute the next tasks in the queue.
     * 
     * @param recordName
     * @param forcePublish
     */
    void eventStart(String recordName, boolean forcePublish)
    {
        if (!forcePublish && this.eventCount.get() > this.threshold
        // throttling only activated for application threads
            && !ContextUtils.isFrameworkThread()
            // throttling only activated for non-system records
            && !ContextUtils.isSystemRecordName(recordName))
        {
            final Long threadId = Long.valueOf(Thread.currentThread().getId());
            final Long exemptionStartNanos = this.exemptThreads.get(threadId);
            if (exemptionStartNanos == null ||
            // has the exemption expired?
                System.nanoTime() - exemptionStartNanos.longValue() > ONE_SECOND_NANOS)
            {
                final int eventCountAtStart = this.eventCount.get();
                final long startTimeNanos = System.nanoTime();
                long loopTimeNanos = 0;
                do
                {
                    LockSupport.parkNanos(eventCountAtStart);
                }
                while (
                // the event count has not gone down since the loop started
                this.eventCount.get() >= eventCountAtStart &&
                // the loop has been going for less than 1 second
                    ((loopTimeNanos = (System.nanoTime() - startTimeNanos)) < ONE_SECOND_NANOS));

                if (loopTimeNanos >= ONE_SECOND_NANOS && this.eventCount.get() >= eventCountAtStart)
                {
                    // mark the thread exempt to prevent event processing deadlock
                    Log.log(this, "Adding thread to throttle exemptions, eventCount=",
                        Integer.toString(this.eventCount.get()));
                    this.exemptThreads.put(threadId, Long.valueOf(System.nanoTime()));
                }
                else
                {
                    if (exemptionStartNanos != null)
                    {
                        Log.log(this, "Removing thread from throttle exemptions");
                        this.exemptThreads.remove(threadId);
                    }
                }
            }
        }

        this.eventCount.getAndIncrement();
    }

    /**
     * Tracks when an event finishes.
     */
    void eventFinish()
    {
        this.eventCount.decrementAndGet();
    }
}