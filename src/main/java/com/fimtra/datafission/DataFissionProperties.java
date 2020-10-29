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

import java.util.concurrent.ConcurrentHashMap;

import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.core.Publisher;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.thimble.ThimbleExecutor;

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
    public interface Names
    {
        String BASE = "dataFission.";

        /**
         * The system property name to define the number of threads used in the core
         * {@link ThimbleExecutor} used by all DataFission {@link Context} instances in the runtime.
         * <br>
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
         * duration of an RPC. The duration timeout begins <b>after</b> the RPC execution starts.
         * <br>
         * E.g. <code>-DdataFission.rpcExecutionDurationTimeoutMillis=5000</code>
         */
        String RPC_EXECUTION_DURATION_TIMEOUT_MILLIS = BASE + "rpcExecutionDurationTimeoutMillis";

        /**
         * The system property name to define the period in milliseconds to wait before a
         * {@link ProxyContext} will attempt re-connecting to a {@link Context} after losing the
         * connection.<br>
         * E.g. <code>-DdataFission.proxyReconnectPeriodMillis=1000</code>
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
         * The system property name to define the threshold, in nanos, for logging a slow publish.<br>
         * E.g. <code>-DdataFission.slowPublishNanos=10000000</code>
         */
        String SLOW_PUBLISH_THRESHOLD_NANOS = BASE + "slowPublishNanos";

        /**
         * The system property name to define the number of threads assigned to the runtime-wide
         * reconnect task scheduler used by all {@link ProxyContext} instances.<br>
         * E.g. <code>-DdataFission.reconnectThreadCount=2</code>
         */
        String RECONNECT_THREAD_COUNT = BASE + "reconnectThreadCount";

        /**
         * The system property name to define the maximum size of the keys pool used for record
         * keys.<br>
         * E.g. <code>-DdataFission.keysPoolMaxSize=200</code>
         */
        String KEYS_POOL_MAX = BASE + "keysPoolMaxSize";

        /**
         * The system property name to define the size of the {@link LongValue} pool.
         * <p>
         * <b>MUST BE AN EVEN NUMBER.</b> <br>
         * E.g. <code>-DdataFission.longValuePoolSize=2048</code>
         */
        String LONG_VALUE_POOL_SIZE = BASE + "longValuePoolSize";

        /**
         * The system property name to define the size of the {@link TextValue} pool.<br>
         * E.g. <code>-DdataFission.textValuePoolSize=2048</code>
         */
        String TEXT_VALUE_POOL_SIZE = BASE + "textValuePoolSize";

        /**
         * The system property name to define the length limit to be eligible for the value to be
         * held in the {@link TextValue} pool.<br>
         * E.g. <code>-DdataFission.textLengthLimitForTextValuePool=5</code>
         */
        String STRING_LENGTH_LIMIT_FOR_TEXT_VALUE_POOL = BASE + "textLengthLimitForTextValuePool";

        /**
         * The system property name to define the estimated maximum number of concurrent threads
         * that will access {@link IRecord} objects in the runtime. This is used to specify the
         * concurrency of the {@link ConcurrentHashMap} components backing the records.<br>
         * E.g. <code>-DdataFission.maxRecordConcurrency=2</code>
         */
        String MAX_RECORD_CONCURRENCY = BASE + "maxRecordConcurrency";

        /**
         * The coalescing window (in milliseconds) for system record publishing. This helps to
         * control the number of messages that a publisher will transmit that are changes to system
         * records, which are generally not processed at the same rate as application records.<br>
         * E.g. <code>-DdataFission.systemRecordCoalesceWindowMillis=250</code>
         */
        String SYSTEM_RECORD_COALESCE_WINDOW_MILLIS = BASE + "systemRecordCoalesceWindowMillis";

        /**
         * The number of deltas pending processing for a record before starting to log them. This
         * helps to reduce chatty logs when network problems interrupt delta sequences. <br>
         * E.g. <code>-DdataFission.deltaCountLogThreshold=6</code>
         */
        String DELTA_COUNT_LOG_THRESHOLD = BASE + "deltaCountLogThreshold";

        /**
         * The number of microseconds to delay the processing of each subscribe message. This
         * reduces image flooding when a {@link ProxyContext} reconnects to a bounced
         * {@link Context}. <br>
         * E.g. <code>-DdataFission.subscribeDelayMicros=100</code>
         */
        String SUBSCRIBE_DELAY_MICROS = BASE + "subscribeDelayMicros";

        /**
         * The batch size to break a mass subscribe into smaller parts for processing. This helps to
         * reduce image flooding when a {@link ProxyContext} reconnects to a bounced
         * {@link Context}. <br>
         * E.g. <code>-DdataFission.subscribeBatchSize=50</code>
         */
        String SUBSCRIBE_BATCH_SIZE = BASE + "subscribeBatchSize";

        /**
         * The maximum pending event queue size before a thread will wait in
         * {@link IPublisherContext#publishAtomicChange(IRecord)} until the queue size goes below
         * this value. Only affects application threads. <br>
         * E.g. <code>-DdataFission.pendingEventThrottleThreshold=200</code>
         */
        String PENDING_EVENT_THROTTLE_THRESHOLD = BASE + "pendingEventThrottleThreshold";

        /**
         * The name of the system property to define the period, in milliseconds, for a
         * {@link Publisher} to publish updates to the
         * {@link ISystemRecordNames#CONTEXT_CONNECTIONS} record. <br>
         * E.g. <code>-DdataFission.connectionsRecordPublishPeriodMillis=30000</code>
         */
        String CONNECTIONS_RECORD_PUBLISH_PERIOD_MILLIS = BASE + "connectionsRecordPublishPeriodMillis";

        /**
         * The maximum size for the rx frame handling tasks pool in a {@link ProxyContext}.<br>
         * E.g. <code>-DdataFission.proxyRxFrameHandlerPoolMaxSize=1000</code>
         */
        String PROXY_RX_FRAME_HANDLER_POOL_MAX_SIZE = BASE + "proxyRxFrameHandlerPoolMaxSize";

        /**
         * Whether thread deadlock checking and thread dumps are enabled. <b>ENABLING THIS IS AN
         * EXPENSIVE OPERATION.</b> <br>
         * E.g. <code>-DdataFission.enableThreadDeadlockCheck=false</code>
         */
        String ENABLE_THREAD_DEADLOCK_CHECK = BASE + "enableThreadDeadlockCheck";

        /**
         * The name of the system property to define the period, in milliseconds, for thread
         * deadlock checks and thread dumps. Only relevant if {@link #ENABLE_THREAD_DEADLOCK_CHECK}
         * is true.<br>
         * E.g. <code>-DdataFission.threadDeadlockCheckPeriodMillis=300000</code>
         * 
         * @see Values#ENABLE_THREAD_DEADLOCK_CHECK
         */
        String THREAD_DEADLOCK_CHECK_PERIOD_MILLIS = BASE + "threadDeadlockCheckPeriodMillis";
        
        /**
         * The name of the system property to define if queue statistics logging is enabled. <br>
         * E.g. <code>-DdataFission.enableQStatsLogging=false</code>
         */
        String ENABLE_Q_STATS_LOGGING = BASE + "enableQStatsLogging";

        /**
         * The name of the system property to define the symmetric transformation. <br>
         * E.g. <code>-DdataFission.encryptedSessionTransformation=AES/ECB/PKCS5Padding</code>
         */
        String ENCRYPTED_SESSION_TRANSFORMATION = BASE + "encryptedSessionTransformation";

        /**
         * The name of the system property to define the rpc names (comma separated) to exclude from logging. <br>
         * E.g. <code>-DdataFission.excludeRpcLogging=runtimeDynamic,runtimeStatic</code>
         */
        String EXCLUDE_RPC_LOGGING = BASE + "excludeRpcLogging";
    }

    /**
     * The values of the properties described in {@link Names}
     * 
     * @author Ramon Servadei
     */
    public interface Values
    {
        /**
         * The number of threads used in the core {@link ThimbleExecutor} used by all DataFission
         * {@link Context} instances in the runtime.
         * <p>
         * These are the defaults for the following processor counts:
         * <ul>
         * <li>small (less than 8 processors) = 4
         * <li>medium (8 to 32 processors) = processors / 2
         * <li>large (over 32 processors) = 16
         * </ul>
         * 
         * @see Names#CORE_THREAD_COUNT
         */
        int CORE_THREAD_COUNT = Integer.parseInt(System.getProperty(Names.CORE_THREAD_COUNT,
            "" + (Runtime.getRuntime().availableProcessors() < 8 ? 4 : (Runtime.getRuntime().availableProcessors() < 32
                ? Runtime.getRuntime().availableProcessors() / 2 : 16))));

        /**
         * The number of threads used in the {@link ThimbleExecutor} for RPCs used by all
         * DataFission {@link Context} instances in the runtime.
         * <p>
         * These are the defaults for the following processor counts:
         * <ul>
         * <li>small (less than 8 processors) = 4
         * <li>medium (8 to 32 processors) = processors / 2
         * <li>large (over 32 processors) = 16
         * </ul>
         * 
         * @see Names#RPC_THREAD_COUNT
         */
        int RPC_THREAD_COUNT = Integer.parseInt(System.getProperty(Names.RPC_THREAD_COUNT,
            "" + (Runtime.getRuntime().availableProcessors() < 8 ? 4 : (Runtime.getRuntime().availableProcessors() < 32
                ? Runtime.getRuntime().availableProcessors() / 2 : 16))));

        /**
         * The timeout to wait for an RPC to start.
         * <p>
         * Default is: 5000
         * 
         * @see Names#RPC_EXECUTION_START_TIMEOUT_MILLIS
         */
        Long RPC_EXECUTION_START_TIMEOUT_MILLIS =
            Long.valueOf(System.getProperty(Names.RPC_EXECUTION_START_TIMEOUT_MILLIS, "5000"));

        /**
         * The duration to wait for an RPC to complete after it has started.
         * <p>
         * Default is: 5000
         * 
         * @see Names#RPC_EXECUTION_DURATION_TIMEOUT_MILLIS
         */
        Long RPC_EXECUTION_DURATION_TIMEOUT_MILLIS =
            Long.valueOf(System.getProperty(Names.RPC_EXECUTION_DURATION_TIMEOUT_MILLIS, "5000"));

        /**
         * The default period in milliseconds that a {@link ProxyContext} waits before reconnecting
         * to a {@link Context} after losing the connection.
         * <p>
         * Default is: 1000
         * 
         * @see Names#PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS
         */
        int PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS =
            Integer.parseInt(System.getProperty(Names.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS, "1000"));

        /**
         * The default maximum number of changes per message a {@link Publisher} can send.
         * <p>
         * Default is: 20
         * 
         * @see Names#PUBLISHER_MAXIMUM_CHANGES_PER_MESSAGE
         */
        int PUBLISHER_MAXIMUM_CHANGES_PER_MESSAGE =
            Integer.parseInt(System.getProperty(Names.PUBLISHER_MAXIMUM_CHANGES_PER_MESSAGE, "20"));

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
         * The threshold value for logging when a publish is slow, in nanos
         * <p>
         * Default is: 10000000 (10ms)
         *
         * @see Names#SLOW_PUBLISH_THRESHOLD_NANOS
         */
        long SLOW_PUBLISH_THRESHOLD_NANOS =
            Long.parseLong(System.getProperty(Names.SLOW_PUBLISH_THRESHOLD_NANOS, "10000000"));

        /**
         * The number of threads used in the shared reconnect task scheduler used by all DataFission
         * {@link ProxyContext} instances in the runtime.
         * <p>
         * Default is 2.
         * 
         * @see Names#RECONNECT_THREAD_COUNT
         */
        int RECONNECT_THREAD_COUNT = Integer.parseInt(System.getProperty(Names.RECONNECT_THREAD_COUNT, "1"));

        /**
         * The maximum size for the keys pool for records.
         * <p>
         * Default is 0 (unlimited).
         * 
         * @see Names#KEYS_POOL_MAX
         */
        int KEYS_POOL_MAX = Integer.parseInt(System.getProperty(Names.KEYS_POOL_MAX, "0"));

        /**
         * The size for the {@link LongValue} pool.
         * <p>
         * Default is 2048 (1024 to -1023).
         * 
         * @see Names#LONG_VALUE_POOL_SIZE
         */
        int LONG_VALUE_POOL_SIZE = Integer.parseInt(System.getProperty(Names.LONG_VALUE_POOL_SIZE, "2048"));

        /**
         * The size for the {@link TextValue} pool.
         * <p>
         * Default is 0 (unlimited).
         * 
         * @see Names#TEXT_VALUE_POOL_SIZE
         */
        int TEXT_VALUE_POOL_SIZE = Integer.parseInt(System.getProperty(Names.TEXT_VALUE_POOL_SIZE, "0"));

        /**
         * The text length limit to be eligible for the value to be held in the {@link TextValue}
         * pool.
         * <p>
         * Default is 5.
         * 
         * @see Names#STRING_LENGTH_LIMIT_FOR_TEXT_VALUE_POOL
         */
        int STRING_LENGTH_LIMIT_FOR_TEXT_VALUE_POOL =
            Integer.parseInt(System.getProperty(Names.STRING_LENGTH_LIMIT_FOR_TEXT_VALUE_POOL, "5"));

        /**
         * The estimated maximum number of concurrent threads that would access an {@link IRecord}.
         * This is used in constructing the {@link ConcurrentHashMap} components backing the
         * records.
         * <p>
         * Default is 2.
         * 
         * @see Names#MAX_RECORD_CONCURRENCY
         */
        int MAX_RECORD_CONCURRENCY = Integer.parseInt(System.getProperty(Names.MAX_RECORD_CONCURRENCY, "2"));

        /**
         * The coalescing window (in milliseconds) for system record publishing.
         * <p>
         * Default is 250.
         * 
         * @see Names#SYSTEM_RECORD_COALESCE_WINDOW_MILLIS
         */
        int SYSTEM_RECORD_COALESCE_WINDOW_MILLIS =
            Integer.parseInt(System.getProperty(Names.SYSTEM_RECORD_COALESCE_WINDOW_MILLIS, "250"));

        /**
         * The number of deltas pending processing for a record before starting to log them.
         * <p>
         * Default is 6.
         * 
         * @see Names#DELTA_COUNT_LOG_THRESHOLD
         */
        int DELTA_COUNT_LOG_THRESHOLD = Integer.parseInt(System.getProperty(Names.DELTA_COUNT_LOG_THRESHOLD, "6"));

        /**
         * The delay, in micro seconds, between handling subsequent subscribe messages.
         * <p>
         * Default is 0 (no delay).
         * 
         * @see Names#SUBSCRIBE_DELAY_MICROS
         */
        int SUBSCRIBE_DELAY_MICROS = Integer.parseInt(System.getProperty(Names.SUBSCRIBE_DELAY_MICROS, "0"));

        /**
         * The batch size for handling mass subscribes.
         * <p>
         * Default is 50.
         * 
         * @see Names#SUBSCRIBE_BATCH_SIZE
         */
        int SUBSCRIBE_BATCH_SIZE = Integer.parseInt(System.getProperty(Names.SUBSCRIBE_BATCH_SIZE, "50"));

        /**
         * The maximum pending event queue size before a thread will wait in
         * {@link IPublisherContext#publishAtomicChange(IRecord)} until the queue size goes below
         * this value. Only affects application threads.
         * <p>
         * Default is 200.
         * 
         * @see Names#PENDING_EVENT_THROTTLE_THRESHOLD
         */
        int PENDING_EVENT_THROTTLE_THRESHOLD =
            Integer.parseInt(System.getProperty(Names.PENDING_EVENT_THROTTLE_THRESHOLD, "200"));

        /**
         * The period, in milliseconds, for a {@link Publisher} to publish updates to the
         * {@link ISystemRecordNames#CONTEXT_CONNECTIONS} record. <br>
         * <p>
         * Default is 30000.
         * 
         * @see Names#CONNECTIONS_RECORD_PUBLISH_PERIOD_MILLIS
         */
        long CONNECTIONS_RECORD_PUBLISH_PERIOD_MILLIS =
            Long.parseLong(System.getProperty(Names.CONNECTIONS_RECORD_PUBLISH_PERIOD_MILLIS, "30000"));

        /**
         * The maximum size for the rx frame handling tasks pool in a {@link ProxyContext}.<br>
         * <p>
         * Default is 1000.
         * 
         * @see Names#PROXY_RX_FRAME_HANDLER_POOL_MAX_SIZE
         */
        int PROXY_RX_FRAME_HANDLER_POOL_MAX_SIZE =
            Integer.parseInt(System.getProperty(Names.PROXY_RX_FRAME_HANDLER_POOL_MAX_SIZE, "1000"));

        /**
         * Whether thread deadlock checking and thread dumps are enabled. <b>ENABLING THIS IS AN
         * EXPENSIVE OPERATION.</b>
         * <p>
         * Default is false.
         * 
         * @see Names#ENABLE_THREAD_DEADLOCK_CHECK
         */
        boolean ENABLE_THREAD_DEADLOCK_CHECK = Boolean.getBoolean(Names.ENABLE_THREAD_DEADLOCK_CHECK);

        /**
         * The period, in milliseconds, for thread deadlock checks and thread dumps. Only relevant
         * if {@link #ENABLE_THREAD_DEADLOCK_CHECK} is true.
         * <p>
         * Default is 300000 (5 mins).
         * 
         * @see Values#ENABLE_THREAD_DEADLOCK_CHECK
         * @see Names#THREAD_DEADLOCK_CHECK_PERIOD_MILLIS
         */
        int THREAD_DEADLOCK_CHECK_PERIOD_MILLIS =
            Integer.parseInt(System.getProperty(Names.THREAD_DEADLOCK_CHECK_PERIOD_MILLIS, "300000"));

        /**
         * Whether the queue statistics are logged.
         * <p>
         * Default is false
         */
        boolean ENABLE_Q_STATS_LOGGING = Boolean.getBoolean(Names.ENABLE_Q_STATS_LOGGING);

        /**
         * Defines the transformation string for encrypted sessions.
         * 
         * @see Names#ENCRYPTED_SESSION_TRANSFORMATION
         */
        String ENCRYPTED_SESSION_TRANSFORMATION =
            System.getProperty(Names.ENCRYPTED_SESSION_TRANSFORMATION, "AES/ECB/PKCS5Padding");

        /**
         * Defines the RPC names that are excluded from logging.
         * 
         * @see Names#EXCLUDE_RPC_LOGGING
         */
        String EXCLUDE_RPC_LOGGING = System.getProperty(Names.EXCLUDE_RPC_LOGGING,
            "runtimeDynamic,runtimeStatic,getServiceInfoForService,getHeartbeatConfig,getPlatformName,register,deregister");
    }

    private DataFissionProperties()
    {
    }
}
