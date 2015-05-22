/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform;

import com.fimtra.platform.core.PlatformRegistry;

/**
 * Defines the properties and property keys used by platform-core
 * 
 * @author Ramon Servadei
 */
public abstract class PlatformCoreProperties
{
    /**
     * The names of the properties
     * 
     * @author Ramon Servadei
     */
    public static interface Names
    {
        /**
         * The base token for the name-space for platform specific property names.
         */
        String BASE = "platform.";

        /**
         * The system property name to define the timeout in milliseconds that an agent waits for
         * the platform name.<br>
         * E.g. <code>-Dplatform.agentInitialisationTimeout=2000</code><br>
         */
        String PLATFORM_AGENT_INITIALISATION_TIMEOUT_MILLIS = BASE + "agentInitialisationTimeout";

        /**
         * The system property name to define the TCP server port used by the
         * {@link PlatformRegistry}.<br>
         * E.g. <code>-Dplatform.registryPort=54321</code><br>
         */
        String REGISTRY_PORT = BASE + "registryPort";

        /**
         * The system property name to define the start (inclusive) of the range of ports to use for
         * default allocated TCP server ports.<br>
         * E.g. <code>-Dplatform.serverPortRange.start=54321</code>
         */
        String TCP_SERVER_PORT_RANGE_START = BASE + "tcpServerPortRange.start";

        /**
         * The system property name to define the end (exclusive) of the range of ports to use for
         * default allocated TCP server ports.<br>
         * E.g. <code>-Dplatform.serverPortRange.end=54323</code>
         */
        String TCP_SERVER_PORT_RANGE_END = BASE + "tcpServerPortRange.end";
    }

    /**
     * The values of the properties described in {@link Names}
     * 
     * @author Ramon Servadei
     */
    public static interface Values
    {
        /**
         * The default TCP server port assignment for the {@link PlatformRegistry}.
         * <p>
         * Default is: 22222
         * <p>
         * See
         * http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.txt
         * 
         * @see Names#REGISTRY_PORT
         */
        int REGISTRY_PORT = Integer.parseInt(System.getProperty(Names.REGISTRY_PORT, "22222"));

        /**
         * The start (inclusive) of the range of TCP ports for default service port assignment.
         * <p>
         * Default is: {@link #REGISTRY_PORT} + 1
         * <p>
         * System property to change this: <tt>platform.defaultServerPortRange.start</tt><br>
         * e.g. <code>-Dplatform.tcpServerPortRange.start=12345</code><br>
         * <p>
         * See
         * http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.txt
         * 
         * @see #TCP_SERVER_PORT_RANGE_END
         * @see Names#TCP_SERVER_PORT_RANGE_START
         */
        int TCP_SERVER_PORT_RANGE_START = Integer.parseInt(System.getProperty(Names.TCP_SERVER_PORT_RANGE_START, ""
            + (REGISTRY_PORT + 1)));
        /**
         * The end (EXCLUSIVE) of the range of TCP ports for default service port assignment.
         * <p>
         * Default is: {@link #REGISTRY_PORT} + 10
         * <p>
         * System property to change this: <tt>platform.defaultServerPortRange.end</tt><br>
         * e.g. <code>-Dplatform.tcpServerPortRange.end=54321</code><br>
         * <p>
         * See
         * http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.txt
         * 
         * @see #TCP_SERVER_PORT_RANGE_START
         * @see Names#TCP_SERVER_PORT_RANGE_END
         */
        int TCP_SERVER_PORT_RANGE_END = Integer.parseInt(System.getProperty(Names.TCP_SERVER_PORT_RANGE_END, ""
            + (REGISTRY_PORT + 10)));

        /**
         * The period in milliseconds that an agent will wait for intialisation with a registry to
         * complete.
         * <p>
         * Default is: 30000
         * 
         * @see Names#PLATFORM_AGENT_INITIALISATION_TIMEOUT_MILLIS
         */
        long PLATFORM_AGENT_INITIALISATION_TIMEOUT_MILLIS = Long.parseLong(System.getProperty(
            Names.PLATFORM_AGENT_INITIALISATION_TIMEOUT_MILLIS, "30000"));
    }

    private PlatformCoreProperties()
    {
    }
}
