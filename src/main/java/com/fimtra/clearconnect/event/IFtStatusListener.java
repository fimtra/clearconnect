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
import com.fimtra.clearconnect.RedundancyModeEnum;

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
