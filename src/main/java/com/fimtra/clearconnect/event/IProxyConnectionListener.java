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

import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.IPlatformServiceProxy;

/**
 * A listener that provides notifications when an {@link IPlatformServiceProxy} connects to a
 * service.
 * <p>
 * This listener allows application code to detect when an {@link IPlatformServiceInstance} receives
 * a new proxy connection.
 * 
 * @author Ramon Servadei
 */
public interface IProxyConnectionListener
{
    /**
     * Called when a platform service proxy connects to the service.
     * 
     * @param proxyId
     *            the name of the proxy that is now connected
     */
    void onConnected(String proxyId);

    /**
     * Called when a platform service proxy disconnects from the service.
     * 
     * @param proxyId
     *            the name of the proxy that is not connected anymore
     */
    void onDisconnected(String proxyId);
}
