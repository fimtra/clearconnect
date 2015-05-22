/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.event;

import com.fimtra.datafission.IRpcInstance;

/**
 * A listener that provides notifications when RPCs are available or unavailable
 * 
 * @author Ramon Servadei
 */
public interface IRpcAvailableListener
{
    /**
     * Called when a new RPC is available in the platform service component.
     * 
     * @param rpc
     *            the rpc that is available for calling
     */
    void onRpcAvailable(IRpcInstance rpc);

    /**
     * Called when an RPC is removed from the platform service component.
     * 
     * @param rpc
     *            the rpc that has been removed
     */
    void onRpcUnavailable(IRpcInstance rpc);
}