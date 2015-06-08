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
 * 
 * @author Ramon Servadei
 */
public interface IServiceConnectionStatusListener
{
    /**
     * Called when a platform service proxy is connected to its service.
     * 
     * @param platformServiceName
     *            the name of the service that is now connected
     * @param identityHash
     *            an identity hash to uniquely identify this platform service proxy instance
     */
    void onConnected(String platformServiceName, int identityHash);

    /**
     * Called when a platform service proxy is being re-connected to its service.
     * 
     * @param platformServiceName
     *            the name of the service that is being re-connected
     * @param identityHash
     *            an identity hash to uniquely identify this platform service proxy instance
     */
    void onReconnecting(String platformServiceName, int identityHash);

    /**
     * Called when a platform service proxy is disconnected from its service. Generally, the
     * {@link #onReconnecting(IPlatformServiceProxy, int)} will be called some-time after this.
     * 
     * @param platformServiceName
     *            the name of the service that is now disconnected
     * @param identityHash
     *            an identity hash to uniquely identify this platform service proxy instance
     */
    void onDisconnected(String platformServiceName, int identityHash);
}
