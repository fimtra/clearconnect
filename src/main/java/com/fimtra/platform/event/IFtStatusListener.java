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
import com.fimtra.platform.RedundancyModeEnum;

/**
 * A component that receives notifications when an {@link IPlatformServiceInstance} in
 * {@link RedundancyModeEnum#FAULT_TOLERANT} mode is the master or standby.
 * 
 * @author Ramon Servadei
 */
public interface IFtStatusListener
{
    /**
     * Called when the platform service instance is active (the master) in an FT platform service
     * family.
     * <p>
     * Only one instance in an FT service family is active at any time.
     * 
     * @param serviceFamily
     *            the service family of the {@link PlatformServiceInstance} that the callback is for
     * @param serviceMember
     *            the service member of the {@link PlatformServiceInstance} that the callback is for
     */
    void onActive(String serviceFamily, String serviceMember);

    /**
     * Called when the platform service is on standby in an FT platform service family.
     * <p>
     * Generally a service is on standby until it becomes the master.
     * 
     * @param serviceFamily
     *            the service family of the {@link PlatformServiceInstance} that the callback is for
     * @param serviceMember
     *            the service member of the {@link PlatformServiceInstance} that the callback is for
     */
    void onStandby(String serviceFamily, String serviceMember);
}
