/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
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
package com.fimtra.clearconnect;

import com.fimtra.clearconnect.core.PlatformRegistry;
import com.fimtra.clearconnect.event.IRecordConnectionStatusListener;
import com.fimtra.clearconnect.event.IServiceConnectionStatusListener;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IRecord;
import com.fimtra.tcpchannel.TcpChannel;

/**
 * A platform service proxy connects to a logical platform service that is supported by one or more
 * platform service instances. A platform service proxy is an observer of the platform service. The
 * proxy cannot (directly) alter records. It can simply subscribe for record changes and call RPCs
 * on the platform service.
 * <p>
 * A platform service proxy provides remote access to a platform service instance running elsewhere
 * on the platform (or even in the same process space). The proxy has a TCP connection to the
 * platform service instance. The connection allows the proxy to issue record subscription requests
 * to the service instance and to invoke RPCs on the service instance. The service instance uses the
 * connection to send record changes and RPC responses.
 * <p>
 * On construction, a platform service proxy will subscribe for the following records from its
 * connected platform service instance:
 * <ul>
 * <li>{@link ISystemRecordNames#CONTEXT_RECORDS}
 * <li>{@link ISystemRecordNames#CONTEXT_RPCS}
 * <li>{@link ISystemRecordNames#CONTEXT_SUBSCRIPTIONS}
 * </ul>
 * With these records, the proxy will be able to expose when new records and RPCs are available in
 * the service and what subscriptions exist.
 * <p>
 * If the proxy is disconnected from its platform service instance, it queries the
 * {@link PlatformRegistry} to discover what platform service instance should be used for a
 * reconnection attempt. This process continues indefinitely. The registry has discretion to select
 * the appropriate platform service instance based on the platform service's redundancy mode (either
 * fault-tolerant or load-balanced). For a full discussion on the fault-tolerance and load-balancing
 * modes, refer to the {@link IPlatformServiceInstance} documentation.
 * 
 * @author Ramon Servadei
 */
public interface IPlatformServiceProxy extends IPlatformServiceComponent
{
    /**
     * Add a listener to receive notifications about the connection status of the proxy to the
     * platform service
     * 
     * @param listener
     *            the listener to add
     * @return <code>true</code> if the listener was added, <code>false</code> otherwise
     */
    boolean addServiceConnectionStatusListener(IServiceConnectionStatusListener listener);

    /**
     * Remove the listener from receiving status notifications.
     * 
     * @param listener
     *            the listener to remove
     * @return <code>true</code> if the listener was removed, <code>false</code> if it was not
     *         removed because it was not registered
     */
    boolean removeServiceConnectionStatusListener(IServiceConnectionStatusListener listener);

    /**
     * Add a listener to receive notifications when records are connected or disconnected. This
     * provides indication of the validity of the data of a record; disconnected records should be
     * viewed as having stale data.
     * <p>
     * Once added the listener receives the existing record statuses asynchronously.
     * 
     * @param listener
     *            the listener to add
     * @return <code>true</code> if the listener was added, <code>false</code> otherwise
     */
    boolean addRecordConnectionStatusListener(IRecordConnectionStatusListener listener);

    /**
     * Remove the listener from receiving status notifications.
     * 
     * @param listener
     *            the listener to remove
     * @return <code>true</code> if the listener was removed, <code>false</code> if it was not
     *         removed because it was not registered
     */
    boolean removeRecordConnectionStatusListener(IRecordConnectionStatusListener listener);

    /**
     * Obtain an immutable instance of the named record. If the record is currently subscribed, this
     * method uses the current image held locally.
     * <p>
     * <b>If the record is NOT subscribed, this will cause a subscription to be issued to the
     * service (generally over the network) to get the record and then unsubscribe when the image is
     * received. This is not a cheap method in this situation.</b>
     * 
     * @param recordName
     *            the record name for the record to get
     * @param timeoutMillis
     *            the timeout in millis to wait for the remote call to complete
     * @return an immutable image of the record image
     */
    IRecord getRecordImage(String recordName, long timeoutMillis);

    /**
     * @return the period in milliseconds to wait before trying a reconnect to the service instance
     */
    int getReconnectPeriodMillis();

    /**
     * Set the period to wait before attempting to reconnect after the TCP connection has been
     * unexpectedly broken
     * 
     * @param reconnectPeriodMillis
     *            the period in milliseconds to wait before trying a reconnect to the service
     *            instance
     */
    void setReconnectPeriodMillis(int reconnectPeriodMillis);

    /**
     * @return a short-hand string describing both connection points of the {@link TcpChannel} of
     *         this proxy
     */
    String getShortSocketDescription();

    /**
     * @return <code>true</code> if the proxy has an active connection to its service,
     *         <code>false</code> otherwise. The connection status can change over time.
     * @see IPlatformServiceProxy#addServiceConnectionStatusListener(IServiceConnectionStatusListener)
     */
    boolean isConnected();
}