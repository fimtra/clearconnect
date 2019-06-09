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

/**
 * Receives notifications when platform services become available/unavailable across the entire
 * platform that the registry service manages. A platform service is available when there is at
 * least one platform service <b>instance</b> that supports the platform service.
 * 
 * @see IEventListener for threading
 * @author Ramon Servadei
 */
public interface IServiceAvailableListener extends IEventListener
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