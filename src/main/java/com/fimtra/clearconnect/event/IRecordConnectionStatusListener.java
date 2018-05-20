/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
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
package com.fimtra.clearconnect.event;

import com.fimtra.clearconnect.IPlatformServiceProxy;

/**
 * A listener that provides notifications when records are 'connected', 'connecting' or
 * 'disconnected'. Initially a record has a 'connected' state (this reflects the proxy being
 * connected). When a disruption occurs in the connection, each record status moves to 'connecting'.
 * If the connection succeeds, the state moves to 'connected'. If the connection fails, the state
 * moves to 'disconnected'. When a re-connection is attempted by the proxy, the status moves again
 * to 'connecting'.
 * <p>
 * This effectively gives a reflection of the network connection between the component and the
 * service and provides the means to detect when data for a record is valid or stale.
 * <h2>Threading</h2>
 * <ul>
 * <li>When a listener instance is registered with only one {@link IPlatformServiceProxy}, the
 * callback methods are guaranteed to not execute concurrently. However, they may be executed by
 * different threads.
 * <li>When a listener instance is registered with multiple proxies, the callback methods may
 * execute concurrently.
 * </ul>
 * 
 * @author Ramon Servadei
 */
public interface IRecordConnectionStatusListener extends IEventListener
{
    /**
     * Called when the record is connected and its data can be considered valid
     * 
     * @param recordName
     *            the name of the record that has been connected
     */
    void onRecordConnected(String recordName);

    /**
     * Called when the record is (re)connecting - data is still not considered valid. After this
     * call-back, either {@link #onRecordConnected(String)} or {@link #onRecordDisconnected(String)}
     * will be called, depending on the connection outcome.
     * 
     * @param recordName
     *            the name of the record that is being re-connected
     */
    void onRecordConnecting(String recordName);

    /**
     * Called when a record is disconnected (network connection lost or the remote service is gone)
     * 
     * @param recordName
     *            the name of the record that has been disconnected
     */
    void onRecordDisconnected(String recordName);
}