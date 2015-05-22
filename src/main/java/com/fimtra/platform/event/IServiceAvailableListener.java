/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.event;

import com.fimtra.platform.IPlatformServiceInstance;

/**
 * Receives notifications when platform services become available/unavailable across the entire
 * platform that the registry service manages. A platform service is available when there is at
 * least one platform service <b>instance</b> that supports the platform service.
 * 
 * @see IPlatformServiceInstance
 * @author Ramon Servadei
 */
public interface IServiceAvailableListener
{
    /**
     * Triggered when a platform service becomes available. This occurs when there is at least one
     * platform service <b>instance</b> available that supports the platform service.
     * 
     * @param serviceFamily
     *            the name of the platform service that is now available
     */
    void onServiceAvailable(String serviceFamily);

    /**
     * Triggered when a platform service becomes unavailable. This occurs when there are no more
     * platform service <b>instances</b> available that support the platform service.
     * 
     * @param serviceFamily
     *            the name of the platform service that is no longer available
     */
    void onServiceUnavailable(String serviceFamily);
}