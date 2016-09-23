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
 * A listener that provides notifications when the connection state of a
 * {@link IPlatformServiceProxy} changes.
 * <p>
 * The state transition model is:
 * 
 * <pre>
 * onConnected -> onDisconnected -> onReconnecting
 *     ^                ^                 |
 *     |                |                 |
 *     ------------------------------------
 * </pre>
 * 
 * The starting state depends on the state of the connection at the point in time that the listener
 * was registered.
 * <h2>Threading</h2>
 * <ul>
 * <li>When a listener instance is registered with only one {@link IPlatformServiceProxy}, the
 * callback methods are guaranteed to not execute concurrently. However, they may be executed by
 * different threads.
 * <li>When a listener instance is registered with multiple components, the callback methods may
 * execute concurrently.
 * </ul>
 * 
 * @see IPlatformServiceProxy#addServiceConnectionStatusListener(IServiceConnectionStatusListener)
 * @author Ramon Servadei
 */
public interface IServiceConnectionStatusListener
{
    /**
     * Called when a platform service proxy is connected to its service.
     * 
     * @param serviceFamily
     *            the name of the service that is now connected
     * @param identityHash
     *            an identity hash to uniquely identify this platform service proxy instance
     */
    void onConnected(String serviceFamily, int identityHash);

    /**
     * Called when a platform service proxy is being re-connected to its service.
     * 
     * @param serviceFamily
     *            the name of the service that is being re-connected
     * @param identityHash
     *            an identity hash to uniquely identify this platform service proxy instance
     */
    void onReconnecting(String serviceFamily, int identityHash);

    /**
     * Called when a platform service proxy is disconnected from its service. The
     * {@link #onReconnecting(IPlatformServiceProxy, int)} will be called a short time after this.
     * 
     * @param serviceFamily
     *            the name of the service that is now disconnected
     * @param identityHash
     *            an identity hash to uniquely identify this platform service proxy instance
     */
    void onDisconnected(String serviceFamily, int identityHash);
}
