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
 * Receives notifications when platform service <b>instances</b> become available/unavailable across
 * the entire platform that the registry service manages. This instance may be part of a
 * load-balanced or fault-tolerant service family.
 * <p>
 * This is a low-level service listener for applications that know what they are doing with service
 * instance connections!
 * 
 * @see
 * @author Ramon Servadei
 */
public interface IServiceInstanceAvailableListener
{
    /**
     * Triggered when a platform service instance becomes available.
     * 
     * @param serviceInstanceId
     *            the ID of the platform service instance that is now available
     */
    void onServiceInstanceAvailable(String serviceInstanceId);

    /**
     * Triggered when a platform service instance becomes unavailable.
     * 
     * @param serviceInstanceId
     *            the ID of the platform service instance that is now unavailable
     */
    void onServiceInstanceUnavailable(String serviceInstanceId);
}