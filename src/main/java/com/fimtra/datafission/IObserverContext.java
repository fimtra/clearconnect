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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.IStatusAttribute;
import com.fimtra.thimble.ISequentialRunnable;

/**
 * An observer context is used to locate already existing records and to register
 * {@link IRecordListener} objects to receive changes that occur to the records.
 * <p>
 * The observer context has the following system records:
 * <ul>
 * <li>A 'context records' record - this tracks all the record names in the context
 * <li>A 'context subscriptions' record - this tracks which records have observers
 * <li>A 'context status' record - this tracks all status attributes of the observer context, these
 * are pretty much freestyle
 * <li>A 'context RPC' record - this tracks all available RPCs
 * </ul>
 * 
 * @see ISystemRecordNames
 * @author Ramon Servadei
 */
public interface IObserverContext
{
    /**
     * Encapsulates all the system records available in a context. A system record is a special
     * record that exposes system-level information about a context. There are 4 system records:
     * <ul>
     * <li>A 'context records' record - this tracks all the record names in the context
     * <li>A 'context subscriptions' record - this tracks which records have observers
     * <li>A 'context status' record - this tracks all status attributes of the observer context,
     * these are pretty much freestyle
     * <li>A 'context RPC' record - this tracks all available RPCs
     * <li>A 'context connections' record - when remote connections exist, this record tracks
     * connection details for each remote {@link IObserverContext}
     * </ul>
     * 
     * @author Ramon Servadei
     */
    public interface ISystemRecordNames
    {
        String CONTEXT = "Context";

        /**
         * Record structure:
         * 
         * <pre>
         * KEY=recordName
         * VALUE=LongValue holding number of subscriptions for the record
         * </pre>
         * 
         * The 'context records' is a system record that lists all record names in the context. The
         * record keys are the record names in the context. The record value shows how many
         * subscribers exist for the record.
         * <p>
         * Keeping track of the subscribers for the record here is different to what the
         * {@link #CONTEXT_SUBSCRIPTIONS} does; the 'context subscriptions' shows the subscriptions
         * that exist regardless of whether the record exists.
         * <p>
         * As records are added/removed to the context, the context record will have record entries
         * added/removed to reflect this. This allows application code to detect when records are
         * added/removed and react as required.
         * <p>
         * Use {@link #getRecord(String)} to obtain the context record using this string.
         */
        String CONTEXT_RECORDS = CONTEXT + "Records";

        /**
         * Record structure:
         * 
         * <pre>
         * KEY=recordName
         * VALUE=LongValue holding number of subscriptions
         * </pre>
         * 
         * The 'context subscriptions' is a system record that tracks which records have
         * subscriptions. A subscription may exist before a record is created. The record keys are
         * the names of records that have subscriptions. The record values hold the number of
         * subscriptions for the record. <b>If a record no longer has subscriptions, its name will
         * be removed from the context subscriptions.</b>
         * <p>
         * As observers are added to the context (via
         * {@link #addObserver(IRecordListener, String...)}) entries will appear in this record.
         * This allows application code to detect when subscriptions for records are added/removed
         * and react as required.
         * <p>
         * Use {@link #getRecord(String)} to obtain the context subscriptions using this string.
         */
        String CONTEXT_SUBSCRIPTIONS = CONTEXT + "Subscriptions";

        /**
         * The 'context status' is a system record that lists status attributes of the context.
         * <p>
         * Use {@link #getRecord(String)} to obtain the context status using this string.
         * 
         * @see IStatusAttribute
         */
        String CONTEXT_STATUS = CONTEXT + "Status";

        /**
         * Record structure:
         * 
         * <pre>
         * KEY=RPC name
         * VALUE=args{comma separated list of IValue.TypeEnum for each argument},returns{IValue.TypeEnum for the return type}
         * </pre>
         * 
         * The 'context RPCs' is a system record that lists all available RPCs.
         * <p>
         * Use {@link #getRpc(String)} to obtain the context RPCs using this string.
         * 
         * e.g. for a method with a signature:
         * <code>TextValue executeFooBar(LongValue long1, DoubleValue double1)</code> <br>
         * The RPC record entry would be:
         * 
         * <pre>
         * KEY=executeFooBar
         * VALUE=args{LONG,DOUBLE},returns{TEXT}
         * </pre>
         */
        String CONTEXT_RPCS = CONTEXT + "Rpcs";

        /**
         * The 'context connections' is a system record that lists the connections to remote
         * observers. Record structure:
         * 
         * <pre>
         * KEY=remote connection name
         * VALUE={sub-map} with connection information, see {@link IContextConnectionsRecordFields} for the definition of the sub-map fields
         * </pre>
         * <p>
         * Use {@link #getRecord(String)} to obtain the context connections using this string.
         */
        String CONTEXT_CONNECTIONS = CONTEXT + "Connections";

        /**
         * Defines the fields in the sub-maps in a {@link ISystemRecordNames#CONTEXT_CONNECTIONS}
         * record.
         * 
         * @author Ramon Servadei
         */
        public interface IContextConnectionsRecordFields
        {
            String ___PUBLISHER = "Publisher";
            String __PROXY = "Proxy";
            String PUBLISHER_ID = ___PUBLISHER + " ID";
            String PUBLISHER_NODE = ___PUBLISHER + " node";
            String PUBLISHER_PORT = ___PUBLISHER + " port";
            String PROXY_ID = __PROXY + " ID";
            String PROXY_ENDPOINT = __PROXY + " endpoint";
            String MSGS_PER_MIN = "Msgs per min";
            String KB_PER_MIN = "Kb per min";
            String MESSAGE_COUNT = "Msgs published";
            String KB_COUNT = "Kb published";
            String SUBSCRIPTION_COUNT = "Subscriptions";
            String UPTIME = "Uptime(sec)";
            String PROTOCOL = "Protocol";
        }
    }

    /**
     * @return the name for the context
     */
    String getName();

    /**
     * Get an existing record by its name. <b>The record will NOT be thread safe for reading or
     * writing.</b>
     * <p>
     * <b>WARNING: Using this method to read a record's state can open your code up to concurrency
     * conditions in a multi-thread environment. Unless all record access is completely controlled,
     * the proper way to read a record's state is by adding a listener to the record and accessing
     * the image presented in the {@link IRecordListener#onChange(IRecord, IRecordChange)}
     * method.</b>
     * 
     * @return the record for the name, <code>null</code> if it doesn't exist in this context
     * @see IPublisherContext#createRecord(String, Map)
     */
    IRecord getRecord(String name);

    /**
     * @return a set of all the record names in this context, including system record names
     * @see {@link ContextUtils#isSystemRecordName(String)}
     */
    Set<String> getRecordNames();

    /**
     * This un-subscribes all {@link IRecordListener} instances from the named records then
     * re-subscribes them. This will re-trigger a full image to all observers of the record(s).
     * <p>
     * Note that this will use the latest successful permission token per record as given in
     * {@link #addObserver(String, IRecordListener, String...)}.
     * 
     * @param recordNames
     *            the names of the records to un-subscribe and re-subscribe for
     */
    void resubscribe(String... recordNames);

    /**
     * Add an observer that will be notified when entries to the named record(s) are added/updated
     * and removed. To be added as an observer, the permission token for the record must be valid.
     * <p>
     * On adding the observer, it will be notified with the current image of the record.
     * <p>
     * This is an idempotent operation.
     * <p>
     * This operation uses a permission token of {@link IPermissionFilter#DEFAULT_PERMISSION_TOKEN}.
     * 
     * @param observer
     *            the observer to add
     * @param recordNames
     *            the names of the records to observe
     * @return a future triggered when all records have been processed. The map contains a boolean
     *         result for each record name to indicate if the record is being observed or not. A
     *         <code>false</code> result for a record name means it is not being observed and this
     *         is due to the {@link IPublisherContext} having determined that the permission token
     *         is not valid for the record name.
     * @see #addObserver(String, IRecordListener, String...)
     */
    Future<Map<String, Boolean>> addObserver(IRecordListener observer, String... recordNames);

    /**
     * Add an observer that will be notified when entries to the named record(s) are added/updated
     * and removed. To be added as an observer, the permission token for the record must be valid.
     * <p>
     * On adding the observer, it will be notified with the current image of the record.
     * <p>
     * This is an idempotent operation.
     * <p>
     * This operation passes in a permission token to use. The {@link IPublisherContext} that
     * handles the registration can examine the token to determine if the observer has correct
     * permissions to receive the record changes.
     * <p>
     * On re-subscribing, the last used permission token for a record subscription is used. Only 1
     * permission token can be associated with a record subscription at any point in time.
     * 
     * @param permissionToken
     *            a token representing the permission to use for the operation
     * @param observer
     *            the observer to add
     * @param recordNames
     *            the names of the records to observe
     * @return a future triggered when all records have been processed. The map contains a boolean
     *         result for each record name to indicate if the record is being observed or not. A
     *         <code>false</code> result for a record name means it is not being observed and this
     *         is due to the {@link IPublisherContext} having determined that the permission token
     *         is not valid for the record name.
     * @see IPermissionFilter
     */
    Future<Map<String, Boolean>> addObserver(String permissionToken, IRecordListener observer, String... recordNames);

    /**
     * Remove the observer from the named record(s).
     * <p>
     * Note that unlike {@link #addObserver(IRecordListener, String...)}, removing observers has no
     * permission check associated with it.
     * 
     * @param observer
     *            the observer to remove
     * @param recordNames
     *            the names of the records
     * @return a latch triggered when the listener is removed from all records.
     */
    CountDownLatch removeObserver(IRecordListener observer, String... recordNames);

    /**
     * @return <code>true</code> if the context is active, <code>false</code> if it has been
     *         destroyed
     */
    boolean isActive();

    /**
     * Destroy the context
     * <p>
     * <b>NOTE: listeners are NOT notified that any records are removed. To have a
     * "notify and shutdown" operation, call
     * {@link ContextUtils#removeRecordsAndDestroyContext(com.fimtra.datafission.core.Context)} </b>
     */
    void destroy();

    /**
     * Get an {@link IRpcInstance} object for the given RPC name. The instance can be used to invoke
     * the RPC multiple times.
     * <p>
     * There is no specification for thread management of an RPC instance.
     * 
     * @param name
     *            the name of the RPC instance
     * 
     * @return the RPC instance for the name, or <code>null</code> if the RPC does not exist
     */
    IRpcInstance getRpc(String name);

    /**
     * Get the executor for utility type tasks
     * 
     * @return a {@link ScheduledExecutorService} for utility tasks
     */
    ScheduledExecutorService getUtilityExecutor();

    /**
     * @return the names of the records that are currently subscribed for via calls to
     *         {@link #addObserver(IRecordListener, String...)}
     */
    Set<String> getSubscribedRecords();

    /**
     * Execute the {@link ISequentialRunnable} using the core {@link Executor} of the context
     * 
     * @param sequentialRunnable
     *            the task to run
     */
    void executeSequentialCoreTask(ISequentialRunnable sequentialRunnable);
}