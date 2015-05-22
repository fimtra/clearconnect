/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform;

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
