/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
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
package com.fimtra.clearconnect;

/**
 * Expresses the redundancy mode for a platform service. The two modes are mutually exclusive; a
 * service is either fault-tolerant or load-balanced.
 * 
 * @author Ramon Servadei
 */
public enum RedundancyModeEnum
{
    /**
     * In this mode, {@link IPlatformServiceProxy} connections to a service all go to the same
     * platform service instance. If the instance drops off the platform, all connections are routed
     * to the next appropriate platform service instance.
     * <p>
     * NOTE: this is service fault-tolerance, not data fault-tolerance.
     */
    FAULT_TOLERANT,
    /**
     * In this mode, {@link IPlatformServiceProxy} connections to a service are balanced across all
     * the platform service instances of a platform service (in a round-robin style).
     * <p>
     * NOTE: this is CONNECTION load-balancing, not data load-balancing.
     */
    LOAD_BALANCED
}
