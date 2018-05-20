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
 * Receives notifications when platform service <b>instances</b> become available/unavailable across
 * the entire platform that the registry service manages. This instance may be part of a
 * load-balanced or fault-tolerant service family.
 * <p>
 * This is a low-level service listener for applications that know what they are doing with service
 * instance connections!
 * <h2>Threading</h2> <b>Callbacks must be thread-safe.</b> They will be executed by at least 2
 * threads, possibly concurrently:
 * <ul>
 * <li>The image-on-subscribe is handled by a dedicated image notifier thread (image thread).
 * <li>Normal updates are handled by a different thread (update thread).
 * </ul>
 * The image and update threads will be different and there is no guarantee that images will be
 * notified before real-time updates.
 * 
 * @see
 * @author Ramon Servadei
 */
public interface IServiceInstanceAvailableListener extends IEventListener
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