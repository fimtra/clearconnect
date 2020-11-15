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
import java.util.concurrent.ScheduledExecutorService;

/**
 * An {@link ScheduledExecutorService} implementation that supports 3 types of tasks:
 * <p>
 * - {@link Runnable} - these will be executed with no guarantee of ordering
 * <p>
 * - {@link ICoalescingRunnable} - this is a context task which can replace any pending task with the same
 * context if it has not executed yet. The coalescing-runnable feature allows for high through-put of task
 * processing in situations where only the latest task for a context requires processing.
 * <p>
 * - {@link ISequentialRunnable} - this is a context task that will be executed sequentially and inorder of
 * submission to the executor with respect to other tasks of the same context. The sequential-runnable feature
 * allows the executor to behave as multiple single-threaded executors which traditionally would be needed to
 * ensure ordered execution of tasks in a multi-thread system. This can reduce thread context switching
 * compared to an executor-per-context approach which also translates to higher task throughput.
 * <p>
 * <b>Sequential and coalescing tasks are mutually exclusive.</b>
 * <p>
 * As an example, if 50,000 sequential tasks for the same context are submitted, then we can expect that the
 * context-executor will run all 50,000 tasks in submission order and sequentially. If 50,000 coalescing tasks
 * for the same context are submitted then, depending on performance, we can expect that the GatlingExecutor
 * will run perhaps only 50 (depends on how fast the processing of 1 occurrence of the task is, the faster its
 * processed, the more occurrences will be processed, but its hard to really beat coalescing like this!).
 * <p>
 * Statistics for the number of submitted and executed sequential and coalescing tasks can be obtained via the
 * {@link #getSequentialTaskStatistics()} and {@link #getCoalescingTaskStatistics()} methods. The statistics
 * are recomputed on each successive call to these methods. It is up to user code to call these methods
 * periodically.
 *
 * @author Ramon Servadei
 * @see ISequentialRunnable
 * @see ICoalescingRunnable
 */
public interface IContextExecutor extends ScheduledExecutorService {

    String QUEUE_LEVEL_STATS = "QueueLevelStats";

    boolean isExecutorThread(long id);

    /**
     * @return the name of the {@link IContextExecutor}
     */
    String getName();

    /**
     * Get the statistics for the sequential tasks submitted to this {@link IContextExecutor}. The statistics
     * are updated on each successive call to this method (this defines the time interval for the
     * statistics).
     *
     * @return a Map holding all sequential task context statistics. The statistics objects will be updated on
     * each successive call to this method.
     */
    Map<Object, ITaskStatistics> getSequentialTaskStatistics();

    /**
     * Get the statistics for the coalescing tasks submitted to this {@link IContextExecutor}. The statistics
     * are updated on each successive call to this method (this defines the time interval for the
     * statistics).
     *
     * @return a Map holding all coalescing task context statistics. The statistics objects will be updated on
     * each successive call to this method.
     */
    Map<Object, ITaskStatistics> getCoalescingTaskStatistics();

    void destroy();
}
