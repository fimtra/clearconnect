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
package com.fimtra.thimble;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tracks statistics for tasks . The statistics are gathered in intervals. An interval is the time
 * between successive calls to {@link #intervalFinished()}.
 * 
 * The statistics are:
 * <ul>
 * <li>intervalSubmitted - the number of tasks that were submitted for the context during the
 * interval
 * <li>intervalExecuted - the number of tasks that were executed for the context during the interval
 * <li>totalSubmitted - the cumulative total number of tasks that were submitted for the context
 * <li>totalExecuted - the cumulative total number of tasks that have been executed for the context
 * </ul>
 * 
 * @author Ramon Servadei
 * @deprecated See {@link com.fimtra.executors.ContextExecutorFactory}
 */
@Deprecated
public final class TaskStatistics
{
    private final Object context;
    private long currentSubmitted, currentExecuted;
    private long intervalSubmitted, intervalExecuted;
    private long totalSubmitted, totalExecuted;
    private final Lock lock = new ReentrantLock();

    TaskStatistics(Object context)
    {
        super();
        this.context = context;
    }

    public Object getContext()
    {
        return this.context;
    }

    public long getIntervalSubmitted()
    {
        return this.intervalSubmitted;
    }

    public long getIntervalExecuted()
    {
        return this.intervalExecuted;
    }

    public long getTotalSubmitted()
    {
        return this.totalSubmitted;
    }

    public long getTotalExecuted()
    {
        return this.totalExecuted;
    }

    @Override
    public String toString()
    {
        return "TaskStatistics [" + this.context + ", intervalSubmitted=" + this.intervalSubmitted
            + ", intervalExecuted=" + this.intervalExecuted + ", totalSubmitted=" + this.totalSubmitted
            + ", totalExecuted=" + this.totalExecuted + "]";
    }

    void itemSubmitted()
    {
        this.lock.lock();
        try
        {
            this.currentSubmitted++;
        }
        finally
        {
            this.lock.unlock();
        }
    }

    void itemExecuted()
    {
        this.lock.lock();
        try
        {
            this.currentExecuted++;
        }
        finally
        {
            this.lock.unlock();
        }
    }

    /**
     * Update the interval and total statistics values and return a snapshot
     * 
     * @return a snapshot of the current statistics
     */
    TaskStatistics intervalFinished()
    {
        this.lock.lock();
        try
        {
            this.intervalSubmitted = this.currentSubmitted;
            this.currentSubmitted = 0;
            this.intervalExecuted = this.currentExecuted;
            this.currentExecuted = 0;
            this.totalSubmitted += this.intervalSubmitted;
            this.totalExecuted += this.intervalExecuted;

            final TaskStatistics snapshot = new TaskStatistics(this.context);
            snapshot.intervalExecuted = this.intervalExecuted;
            snapshot.intervalSubmitted = this.intervalSubmitted;
            snapshot.totalSubmitted = this.totalSubmitted;
            snapshot.totalExecuted = this.totalExecuted;
            return snapshot;
        }
        finally
        {
            this.lock.unlock();
        }
    }

}
