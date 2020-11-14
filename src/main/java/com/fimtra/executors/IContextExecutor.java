/*
 * Copyright 2020 Ramon Servadei
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.executors;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import com.sun.xml.internal.ws.client.sei.SEIStub;

/**
 * An IContextExecutor is a multi-thread {@link Executor} implementation that supports
 * sequential and coalescing tasks. The tasks submitted are bound to a context, which ensures
 * in-order execution across multiple threads. There are 2 types of tasks: sequential and
 * coalescing:
 * <ul>
 * <li>Sequential tasks are guaranteed to run in order, sequentially (but may run in different
 * threads).
 * <li>Coalescing tasks are tasks where only the latest submitted task needs to be executed
 * (effectively overwriting previously submitted tasks).
 * </ul>
 * <p>
 * <b>Sequential and coalesing tasks are mutually exclusive.</b>
 * <p>
 * As an example, if 50,000 sequential tasks for the same context are submitted, then we can expect
 * that the IContextExecutor will run all 50,000 tasks in submission order and sequentially. If
 * 50,000 coalescing tasks for the same context are submitted then, depending on performance, we can
 * expect that the IContextExecutor will run perhaps only 20,000.
 * <p>
 * Statistics for the number of submitted and executed sequential and coalescing tasks can be
 * obtained via the {@link #getSequentialTaskStatistics()} and
 * {@link #getCoalescingTaskStatistics()} methods. The statistics are recomputed on each successive
 * call to these methods. It is up to user code to call these methods periodically. Statistics for
 * the number of submitted and executed tasks (regardless of type) can be obtained via the
 * {@link #getExecutorStatistics()} method.
 * 
 * @see ISequentialRunnable
 * @see ICoalescingRunnable
 * @author Ramon Servadei
 */
public interface IContextExecutor extends ScheduledExecutorService
{
    String QUEUE_LEVEL_STATS = "QueueLevelStats";
    
    boolean isExecutorThread(long id);

    /**
     * @return the name of the {@link IContextExecutor}
     */
    String getName();

    /**
     * Get the statistics for the sequential tasks submitted to this {@link IContextExecutor}. The
     * statistics are updated on each successive call to this method (this defines the time interval
     * for the statistics).
     * 
     * @return a Map holding all sequential task context statistics. The statistics objects will be
     *         updated on each successive call to this method.
     */
    Map<Object, ITaskStatistics> getSequentialTaskStatistics();

    /**
     * Get the statistics for the coalescing tasks submitted to this {@link IContextExecutor}. The
     * statistics are updated on each successive call to this method (this defines the time interval
     * for the statistics).
     * 
     * @return a Map holding all coalescing task context statistics. The statistics objects will be
     *         updated on each successive call to this method.
     */
    Map<Object, ITaskStatistics> getCoalescingTaskStatistics();

    /**
     * @return the statistics for all tasks submitted to this {@link IContextExecutor}.
     */
    ITaskStatistics getExecutorStatistics();

    void destroy();
}
