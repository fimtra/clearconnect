/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.event;

/**
 * Receives notifications when the agent is connected/disconnected to the platform registry.
 * 
 * @author Ramon Servadei
 */
public interface IRegistryAvailableListener
{
    /**
     * Called when the agent is connected to the registry
     */
    void onRegistryConnected();

    /**
     * Called when the agent is no longer connected to the registry
     */
    void onRegistryDisconnected();
}