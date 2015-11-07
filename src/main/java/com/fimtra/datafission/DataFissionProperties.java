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
package com.fimtra.datafission;

import java.util.Set;

import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.core.Publisher;
import com.fimtra.thimble.ThimbleExecutor;
import com.fimtra.util.CollectionUtils;

/**
 * Defines the properties and property keys used by DataFission
 * 
 * @author Ramon Servadei
 */
public abstract class DataFissionProperties
{
    /**
     * The names of the properties
     * 
     * @author Ramon Servadei
     */
    public static interface Names
    {
        String BASE = "dataFission.";

        /**
         * The system property name to define the number of threads used in the core
         * {@link ThimbleExecutor} used by all DataFission {@link Context} instances in the runtime.<br>
         * E.g. <code>-DdataFission.coreThreadCount=8</code>
         */
        String CORE_THREAD_COUNT = BASE + "coreThreadCount";

        /**
         * The system property name to define the number of threads used in the
         * {@link ThimbleExecutor} for RPCs used by all DataFission {@link Context} instances in the
         * runtime.<br>
         * E.g. <code>-DdataFission.rpcThreadCount=4</code>
         */
        String RPC_THREAD_COUNT = BASE + "rpcThreadCount";

        /**
         * The system property name to define the timeout in milliseconds to wait for the execution
         * start of an RPC.<br>
         * E.g. <code>-DdataFission.rpcExecutionStartTimeoutMillis=5000</code>
         */
        String RPC_EXECUTION_START_TIMEOUT_MILLIS = BASE + "rpcExecutionStartTimeoutMillis";

        /**
         * The system property name to define the timeout in milliseconds to wait for the execution
         * duration of an RPC. The duration timeout begins <b>after</b> the RPC execution starts.<br>
         * E.g. <code>-DdataFission.rpcExecutionDurationTimeoutMillis=5000</code>
         */
        String RPC_EXECUTION_DURATION_TIMEOUT_MILLIS = BASE + "rpcExecutionDurationTimeoutMillis";

        /**
         * The system property name to define the period in milliseconds to wait before a
         * {@link ProxyContext} will attempt re-connecting to a {@link Context} after losing the
         * connection.<br>
         * E.g. <code>-DdataFission.proxyReconnectPeriodMillis=5000</code>
         */
        String PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS = BASE + "proxyReconnectPeriodMillis";

        /**
         * The system property name to define the maximum number of atomic changes per message sent
         * from a {@link Publisher}.<br>
         * E.g. <code>-DdataFission.maxChangesPerMessage=20</code>
         */
        String PUBLISHER_MAXIMUM_CHANGES_PER_MESSAGE = BASE + "maxChangesPerMessage";

        /**
         * The system property name to define the period, in seconds, for logging the context core
         * executors statistics.
         * <p>
         * <b>This setting also defines how often the QStats file is updated with the queue
         * statistics.</b>
         * <p>
         * E.g. <code>-DdataFission.statsLoggingPeriodSecs=30</code>
         */
        String STATS_LOGGING_PERIOD_SECS = BASE + "statsLoggingPeriodSecs";

        /**
         * The system property name to define the maximum number of fields in a map to print.<br>
         * E.g. <code>-DdataFission.maxMapFieldsToPrint=30</code>
         */
        String MAX_MAP_FIELDS_TO_PRINT = BASE + "maxMapFieldsToPrint";

        /**
         * The system property name to define the threshold, in nanos, for defining a slow task (and
         * thus logging a message indicating the task was slow).<br>
         * E.g. <code>-DdataFission.slowTaskThresholdNanos=50000000</code>
         */
        String SLOW_TASK_THRESHOLD_NANOS = BASE + "slowTaskThresholdNanos";

        /**
         * The system property name to define the list of comma-separated string prefixes that
         * should be ignored for logging of received commands. <b>These are NOT regular
         * expressions.</b><br>
         * E.g. <code>-DdataFission.ignoreLoggingRxCommandsWithPrefix=rpc|xyz,</code>
         */
        String IGNORE_LOGGING_RX_COMMANDS_WITH_PREFIX = BASE + "ignoreLoggingRxCommandsWithPrefix";

        /**
         * The system property name to define the number of threads assigned to the runtime-wide
         * reconnect task scheduler used by all {@link ProxyContext} instances.<br>
         * E.g. <code>-DdataFission.reconnectThreadCount=2</code>
         */
        String RECONNECT_THREAD_COUNT = BASE + "reconnectThreadCount";
    }

    /**
     * The values of the properties described in {@link Names}
     * 
     * @author Ramon Servadei
     */
    public static interface Values
    {
        /**
         * The number of threads used in the core {@link ThimbleExecutor} used by all DataFission
         * {@link Context} instances in the runtime.
         * <p>
         * Default is 1x available CPU cores (+1) in the host runtime.
         * 
         * @see Names#CORE_THREAD_COUNT
         */
        int CORE_THREAD_COUNT = Integer.parseInt(System.getProperty(Names.CORE_THREAD_COUNT, ""
            + (Runtime.getRuntime().availableProcessors() + 1)));

        /**
         * The number of threads used in the {@link ThimbleExecutor} for RPCs used by all
         * DataFission {@link Context} instances in the runtime.
         * <p>
         * Default is 1x available CPU cores in the host runtime.
         * 
         * @see Names#RPC_THREAD_COUNT
         */
        int RPC_THREAD_COUNT = Integer.parseInt(System.getProperty(Names.RPC_THREAD_COUNT,
            "" + (Runtime.getRuntime().availableProcessors())));

        /**
         * The timeout to wait for an RPC to start.
         * <p>
         * Default is: 5000
         * 
         * @See Names#RPC_EXECUTION_START_TIMEOUT_MILLIS
         */
        Long RPC_EXECUTION_START_TIMEOUT_MILLIS = Long.valueOf(System.getProperty(
            Names.RPC_EXECUTION_START_TIMEOUT_MILLIS, "5000"));

        /**
         * The duration to wait for an RPC to complete after it has started.
         * <p>
         * Default is: 5000
         * 
         * @See Names#RPC_EXECUTION_DURATION_TIMEOUT_MILLIS
         */
        Long RPC_EXECUTION_DURATION_TIMEOUT_MILLIS = Long.valueOf(System.getProperty(
            Names.RPC_EXECUTION_DURATION_TIMEOUT_MILLIS, "5000"));

        /**
         * The default period in milliseconds that a {@link ProxyContext} waits before reconnecting
         * to a {@link Context} after losing the connection.
         * <p>
         * Default is: 5000
         * 
         * @see Names#PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS
         */
        int PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS = Integer.parseInt(System.getProperty(
            Names.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS, "5000"));

        /**
         * The default maximum number of changes per message a {@link Publisher} can send.
         * <p>
         * Default is: 20
         * 
         * @see Names#PUBLISHER_MAXIMUM_CHANGES_PER_MESSAGE
         */
        int PUBLISHER_MAXIMUM_CHANGES_PER_MESSAGE = Integer.parseInt(System.getProperty(
            Names.PUBLISHER_MAXIMUM_CHANGES_PER_MESSAGE, "20"));

        /**
         * The period, in seconds, for context core executors statistics logging.
         * <p>
         * Default is: 30
         * 
         * @see Names#STATS_LOGGING_PERIOD_SECS
         */
        int STATS_LOGGING_PERIOD_SECS = Integer.parseInt(System.getProperty(Names.STATS_LOGGING_PERIOD_SECS, "30"));
        /**
         * The maximum number of map fields to print.
         * <p>
         * Default is: 30
         * 
         * @see Names#MAX_MAP_FIELDS_TO_PRINT
         */
        int MAX_MAP_FIELDS_TO_PRINT = Integer.parseInt(System.getProperty(Names.MAX_MAP_FIELDS_TO_PRINT, "30"));
        /**
         * The threshold value for logging when a task is slow, in nanos
         * <p>
         * Default is: 50000000 (50ms)
         * 
         * @see Names#SLOW_TASK_THRESHOLD_NANOS
         */
        long SLOW_TASK_THRESHOLD_NANOS =
            Long.parseLong(System.getProperty(Names.SLOW_TASK_THRESHOLD_NANOS, "50000000"));
        /**
         * The set of prefixes identifying RX commands that are not logged.
         * <p>
         * Default is nothing (log all)
         * 
         * @see Names#IGNORE_LOGGING_RX_COMMANDS_WITH_PREFIX
         */
        Set<String> IGNORE_LOGGING_RX_COMMANDS_WITH_PREFIX = CollectionUtils.newSetFromString(
            System.getProperty(Names.IGNORE_LOGGING_RX_COMMANDS_WITH_PREFIX), ",");

        /**
         * The number of threads used in the shared reconnect task scheduler used by all DataFission
         * {@link ProxyContext} instances in the runtime.
         * <p>
         * Default is 2.
         * 
         * @see Names#RECONNECT_THREAD_COUNT
         */
        int RECONNECT_THREAD_COUNT = Integer.parseInt(System.getProperty(Names.RECONNECT_THREAD_COUNT, "2"));
    }

    private DataFissionProperties()
    {
    }
}
