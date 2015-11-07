/*
 * Copyright (c) 2013 Ramon Servadei, Paul Mackinlay, Fimtra
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

import com.fimtra.clearconnect.core.PlatformRegistry;

/**
 * Defines the properties and property keys used by platform-core
 * 
 * @author Ramon Servadei
 * @author Paul Mackinlay
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
		 * The system property name to define the timeout in milliseconds that an agent waits for
		 * services to become available.<br>
		 * E.g. <code>-Dplatform.agentServicesAvailableTimeout=10000</code><br>
		 */
		String PLATFORM_AGENT_SERVICES_AVAILABLE_TIMEOUT_MILLIS = BASE + "agentServicesAvailableTimeout";

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

		/**
		 * The period in milliseconds that an agent will wait for services to become available.
		 * <p>
		 * Default is: 60000
		 * 
		 * @see Names#PLATFORM_AGENT_SERVICES_AVAILABLE_TIMEOUT_MILLIS
		 */
		long PLATFORM_AGENT_SERVICES_AVAILABLE_TIMEOUT_MILLIS = Long.parseLong(System.getProperty(
				Names.PLATFORM_AGENT_SERVICES_AVAILABLE_TIMEOUT_MILLIS, "60000"));
    }

    private PlatformCoreProperties()
    {
    }
}