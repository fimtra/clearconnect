/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.event;

import com.fimtra.platform.IPlatformServiceProxy;

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
