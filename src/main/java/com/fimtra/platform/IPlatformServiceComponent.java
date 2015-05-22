/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.platform.event.IRecordAvailableListener;
import com.fimtra.platform.event.IRecordSubscriptionListener;
import com.fimtra.platform.event.IRecordSubscriptionListener.SubscriptionInfo;
import com.fimtra.platform.event.IRpcAvailableListener;

/**
 * Common methods for platform service components (service instances and proxy instances)
 * 
 * @author Ramon Servadei
 */
public interface IPlatformServiceComponent
{
    /**
     * @return the name of the platform
     */
    String getPlatformName();

    /**
     * @return the name of the service this represents
     */
    String getPlatformServiceFamily();

    /**
     * @return <code>true</code> if the component is active, <code>false</code> if inactive
     *         (destroyed)
     */
    boolean isActive();

    /**
     * @return All the <b>current</b> record names in this platform service component. This includes
     *         system record names.
     * @see ContextUtils#isSystemRecordName(String)
     */
    Set<String> getAllRecordNames();

    /**
     * Add a listener to receive changes to the specified records. Invoking this method will cause a
     * subscription to be issued for any records not already subscribed for by the component. This
     * method can be called to register multiple listeners.
     * <p>
     * This method is idempotent; calling this multiple times with the same listener and record
     * names is the same as just calling it once
     * <p>
     * <b>The listener will receive all updates asynchronously</b>
     * 
     * @param listener
     *            the listener to add
     * @param recordNames
     *            the record name(s) to subscribe the listener to
     * @return a latch that is triggered when the listener is added to all records; this allows
     *         synchronous operation of this method
     */
    CountDownLatch addRecordListener(IRecordListener listener, String... recordNames);

    /**
     * Remove the listener from receiving changes for the specified records. If no more listeners
     * are attached to any record, the record will no longer be subscribed for from this platform
     * service component.
     * 
     * @param listener
     *            the listener to remove
     * @param recordNames
     *            the record name(s) to unsubscribe the listener from
     * @return a latch that is triggered when the listener is removed from all records; this allows
     *         synchronous operation of this method
     */
    CountDownLatch removeRecordListener(IRecordListener listener, String... recordNames);

    /**
     * Add a listener to receive notifications when new records are added or removed in the platform
     * service component. This method can be called to register multiple listeners.
     * <p>
     * <b>The listener will receive all updates asynchronously</b>
     * <p>
     * Once added the listener receives the existing records asynchronously.
     * 
     * @param recordListener
     *            the record listener to add to receive notifications of added and removed records
     *            in the platform service component
     * @return <code>true</code> if the listener was added, <code>false</code> if it was already
     *         added
     */
    boolean addRecordAvailableListener(IRecordAvailableListener recordListener);

    /**
     * Remove the listener from receiving notifications when records are added or removed in the
     * platform service component.
     * 
     * @param recordListener
     *            the record listener to remove
     * @return <code>true</code> if the listener was removed, <code>false</code> if it was not
     *         removed because it was not registered
     */
    boolean removeRecordAvailableListener(IRecordAvailableListener recordListener);

    /**
     * Add a listener to receive notifications when records are subscribed for.
     * <p>
     * Once added the listener receives the existing subscriptions asynchronously.
     * 
     * @param listener
     *            the listener to add
     * @return <code>true</code> if the listener was added, <code>false</code> otherwise (it already
     *         is added)
     */
    boolean addRecordSubscriptionListener(IRecordSubscriptionListener listener);

    /**
     * Remove the listener from receiving notifications when records are subscribed for in the
     * platform service component.
     * 
     * @param listener
     *            the listener to remove
     * @return <code>true</code> if the listener was removed, <code>false</code> if it was not
     *         removed because it was not registered
     */
    boolean removeRecordSubscriptionListener(IRecordSubscriptionListener listener);

    /**
     * Get all the <b>current</b> RPCs that exist in the platform service component. The RPC
     * instances can be cached and used ad-hoc.
     * <p>
     * NOTE: In a proxy, this map may not be fully populated directly after the proxy construction;
     * it needs time to receive the RPC definitions from the service. Use the
     * {@link #addRpcAvailableListener(IRpcAvailableListener)} to find out when RPCs are available.
     * 
     * @return a map of the RPCs, keyed by the RPC name.
     */
    Map<String, IRpcInstance> getAllRpcs();

    /**
     * Get all the <b>current</b> subscriptions for records in the platform service component. A
     * subscription can exist even if there is no record.
     * 
     * @see #addRecordSubscriptionListener(IRecordSubscriptionListener)
     * @return a map of the subscriptions, keyed by the record name.
     */
    Map<String, SubscriptionInfo> getAllSubscriptions();

    /**
     * This is a convenience method to invoke an RPC without needing to write code that waits for
     * the RPC to be available. This method will also wait for the RPC result.
     * <p>
     * <b>THIS WILL BLOCK UNTIL THE RPC BECOMES AVAILABLE AND EXECUTES.</b>
     * 
     * @param rpcName
     *            the RPC name to invoke
     * @param rpcArgs
     *            the RPC arguments
     * @param discoveryTimeout
     *            the timeout in millis to wait for the RPC to become available
     * 
     * @return the RPC result
     * @throws TimeOutException
     *             if the RPC was not available in the allotted time or it executed but experienced
     *             a timeout
     * @throws ExecutionException
     *             if the arguments are wrong or there was an execution exception
     */
    IValue executeRpc(long discoveryTimeoutMillis, String rpcName, IValue... rpcArgs) throws TimeOutException,
        ExecutionException;

    /**
     * This is a convenience method to invoke an RPC without needing to write code that waits for
     * the RPC to be available.
     * <p>
     * <b>THIS WILL BLOCK UNTIL THE RPC BECOMES AVAILABLE AND EXECUTES.</b>
     * 
     * @param rpcName
     *            the RPC name to invoke
     * @param rpcArgs
     *            the RPC arguments
     * @param discoveryTimeout
     *            the timeout in millis to wait for the RPC to become available
     * 
     * @throws TimeOutException
     *             if the RPC was not available in the allotted time or it executed but experienced
     *             a timeout
     * @throws ExecutionException
     *             if the arguments are wrong or there was an execution exception
     */
    void executeRpcNoResponse(long discoveryTimeoutMillis, String rpcName, IValue... rpcArgs) throws TimeOutException,
        ExecutionException;

    /**
     * Add a listener to receive notifications when RPCs become available in the platform service
     * component. This method can be called to register multiple listeners.
     * <p>
     * <b>The listener will receive all updates asynchronously</b>
     * <p>
     * Once added the listener receives the existing RPCs asynchronously.
     * 
     * @param rpcListener
     *            the listener to add
     * @return <code>true</code> if the listener was added, <code>false</code> if it was already
     *         added
     */
    boolean addRpcAvailableListener(IRpcAvailableListener rpcListener);

    /**
     * Remove a listener from receiving notifications when RPCs become available in the platform
     * service
     * 
     * @param rpcListener
     *            the listener to remove
     * @return <code>true</code> if the listener was removed, <code>false</code> if the listener was
     *         not removed because it was not added in the first place
     */
    boolean removeRpcAvailableListener(IRpcAvailableListener rpcListener);

    /**
     * Get the executor for utility type tasks
     * 
     * @return a {@link ScheduledExecutorService} for utility tasks
     */
    ScheduledExecutorService getUtilityExecutor();
}