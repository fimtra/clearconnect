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
 * Not thread safe.
 *
 * @author Ramon Servadei
 */
public final class TaskStatistics
{
    private final Object context;
    private long intervalSubmitted, intervalExecuted;
    private long totalSubmitted, totalExecuted;
    private long submittedMark, executedMark;

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
        this.totalSubmitted++;
    }

    void itemExecuted()
    {
        this.totalExecuted++;
    }

    /**
     * Update the interval and total statistics values and return a snapshot
     * 
     * @return a snapshot of the current statistics
     */
    TaskStatistics intervalFinished()
    {
        final TaskStatistics snapshot = new TaskStatistics(this.context);

        // just to ensure we read / write
        synchronized (this)
        {
            long lastSubmittedMark = this.submittedMark;
            long lastExecutedMark = this.executedMark;

            this.submittedMark = this.totalSubmitted;
            this.executedMark = this.totalExecuted;

            this.intervalSubmitted = this.submittedMark - lastSubmittedMark;
            this.intervalExecuted = this.executedMark - lastExecutedMark;

            snapshot.intervalSubmitted = this.intervalSubmitted;
            snapshot.intervalExecuted = this.intervalExecuted;
            snapshot.totalSubmitted = this.totalSubmitted;
            snapshot.totalExecuted = this.totalExecuted;
        }

        return snapshot;
    }

}
