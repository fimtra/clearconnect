/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.platform.core.PlatformUtils;
import com.fimtra.platform.event.IFtStatusListener;
import com.fimtra.platform.event.IRecordAvailableListener;
import com.fimtra.thimble.ISequentialRunnable;

/**
 * A platform service instance provides the capability to construct records and RPCs. The platform
 * service instance can publish record changes to one or more connected platform service proxy
 * components on the platform. Each proxy connects to the platform service instance via the TCP
 * server socket of the platform service.
 * <p>
 * A platform service instance conceptually supports a logical platform service (also termed a
 * "service family"); the platform service or "service family" is the context for records
 * constructed and hosted in the instance (and any other instances of the same service family). The
 * logical platform service is identified by its family name. Many platform service instances can
 * support the logical platform service (see below). The platform service instance provides the
 * create, read, update, delete (CRUD) operations for records.
 * <p>
 * NOTE: a platform service instance is <b>uniquely</b> identified in the entire platform by a
 * platform service instance ID (or "service instance ID"). This is composed of the service family
 * and the member name. Within a service family, the member name is unique. Be aware that the same
 * member name can exist in other services. See
 * {@link PlatformUtils#composePlatformServiceInstanceID(String, String)}.
 * <p>
 * <h3>Platform Service Redundancy: fault-tolerance and load-balancing</h3>
 * A service instance is part of a logical platform service. The {@link #getPlatformServiceFamily()}
 * describes the logical platform service by name. Many platform service instances can exist for the
 * same logical platform service; this provides the fault-tolerance or load-balancing capability of
 * a platform service.
 * <p>
 * A platform service is either fault-tolerant or load-balancing; the two abilities are
 * mutually-exclusive. Platform services instances are registered with the registry service with the
 * same redundancy mode per platform service; service instances supporting the same platform service
 * cannot mix and match redundancy modes - they all register with the same mode. When a platform
 * service proxy is connected to a platform service instance, it queries the registry for the
 * service instance to use for the platform service. Thus the registry performs the role of
 * redundancy manager for a platform service as it decides which instance should be used when a
 * proxy attempts to connect.
 * 
 * @author Ramon Servadei
 */
public interface IPlatformServiceInstance extends IPlatformServiceComponent
{
    /**
     * Convenience method to get an existing record or create it if it does not exist
     * 
     * @see #createRecord(String)
     * @see #getRecord(String)
     * @param name
     *            the record name
     * @return the record
     */
    IRecord getOrCreateRecord(String name);

    /**
     * Create a record.
     * <p>
     * Calling this method will indirectly invoke the
     * {@link IRecordAvailableListener#onRecordAvailable(String)} method of any
     * {@link IRecordAvailableListener} listeners in all connected {@link IPlatformServiceProxy}
     * instances.
     * 
     * @param name
     *            the name of the record
     * @return <code>true</code> if the record was created, <code>false</code> if it already existed
     */
    boolean createRecord(String name);

    /**
     * Get a record
     * 
     * @param name
     *            the name of the record
     * @return the record or <code>null</code> if the record did not exist
     */
    IRecord getRecord(String name);

    /**
     * Publish a record. Calling this method will publish changes in the record since the last call
     * to this method.
     * <p>
     * This method blocks any changes to the record whilst the method executes.
     * 
     * @see IPlatformServiceProxy#addRecordListener(com.fimtra.datafission.IRecordListener,
     *      String...)
     * @param record
     *            the record to publish
     * @return a {@link CountDownLatch} that is triggered when the change has been published to all
     *         observers. If there is no change to publish or there are no observers, the latch is
     *         still triggered. <br>
     *         If the record is null or for a different service then this method will return
     *         <code>null</code>.
     */
    CountDownLatch publishRecord(IRecord record);

    /**
     * Delete a record.
     * <p>
     * Calling this method will indirectly invoke the
     * {@link IRecordAvailableListener#onRecordUnavailable(String)} method of any
     * {@link IRecordAvailableListener} listeners in all connected {@link IPlatformServiceProxy}
     * instances.
     * 
     * @param record
     *            the record to delete
     * @return <code>true</code> if the record was deleted, <code>false</code> otherwise
     */
    boolean deleteRecord(IRecord record);

    /**
     * Publish the RPC so it may be invoked. This can be called to publish multiple RPCs.
     * 
     * @param rpc
     *            the RPC publish
     * @return <code>true</code> if the RPC was published, <code>false</code> otherwise
     */
    boolean publishRPC(IRpcInstance rpc);

    /**
     * Unpublish the RPC so it may no longer be invoked.
     * 
     * @param rpc
     *            the RPC to unpublish
     * @return <code>true</code> if the RPC was unpublished, <code>false</code> if it was not
     */
    boolean unpublishRPC(IRpcInstance rpc);

    /**
     * Get the member name of this platform service instance.
     * 
     * @return the member name of this platform service instance.
     */
    String getPlatformServiceMemberName();

    /**
     * @return the end-point address of the {@link IEndPointService} supporting this platform
     *         service instance
     */
    EndPointAddress getEndPointAddress();

    /**
     * @return the wire-protocol used by this service instance
     */
    WireProtocolEnum getWireProtocol();

    /**
     * @return the redundancy mode of this instance. NOTE: all instances of the same platform
     *         service MUST share the same redundancy mode.
     */
    RedundancyModeEnum getRedundancyMode();

    /**
     * Add the status listener to receive events when the service becomes active/standby.
     * <p>
     * <b>This is only functional if the service is running in
     * {@link RedundancyModeEnum#FAULT_TOLERANT} mode</b>
     * 
     * @param ftStatusListener
     *            the listener to register
     */
    void addFtStatusListener(IFtStatusListener ftStatusListener);

    /**
     * Remove the status listener
     * <p>
     * <b>This is only functional if the service is running in
     * {@link RedundancyModeEnum#FAULT_TOLERANT} mode</b>
     * 
     * @param ftStatusListener
     *            the listener to remove
     */
    void removeFtStatusListener(IFtStatusListener ftStatusListener);

    /**
     * Execute the {@link ISequentialRunnable} using the core {@link Executor} of the service
     * 
     * @param sequentialRunnable
     *            the task to run
     */
    void executeSequentialCoreTask(ISequentialRunnable sequentialRunnable);
}
