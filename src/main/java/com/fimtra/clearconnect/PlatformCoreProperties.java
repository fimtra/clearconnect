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
         * The system property name to define the period in SECONDS that the registry uses for
         * publishing changes in its registry records.<br>
         * E.g. <code>-Dplatform.registryRecordPublishPeriodSecs=2</code><br>
         */
        String REGISTRY_RECORD_PUBLISH_PERIOD_SECS = BASE + "registryRecordPublishPeriodSecs";
        
        /**
         * The system property name to define the maximum tries a platform agent will use for
         * registering a service.<br>
         * E.g. <code>-Dplatform.agentMaxServiceRegisterTries=2</code><br>
         */
        String PLATFORM_AGENT_MAX_SERVICE_REGISTER_TRIES = BASE + "agentMaxServiceRegisterTries";
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
        
        /**
         * The period in SECONDS that the registry uses for publishing changes in its registry
         * records.
         * <p>
         * Default is: 2
         * 
         * @see Names#REGISTRY_RECORD_PUBLISH_PERIOD_SECS
         */
        long REGISTRY_RECORD_PUBLISH_PERIOD_SECS = Long.parseLong(System.getProperty(
            Names.REGISTRY_RECORD_PUBLISH_PERIOD_SECS, "2"));

        /**
         * The maximum tries a platform agent will use for registering a service.
         * <p>
         * Default is: 3
         * 
         * @see Names#PLATFORM_AGENT_MAX_SERVICE_REGISTER_TRIES
         */
        int PLATFORM_AGENT_MAX_SERVICE_REGISTER_TRIES = Integer.parseInt(System.getProperty(
            Names.PLATFORM_AGENT_MAX_SERVICE_REGISTER_TRIES, "3"));
    }

    private PlatformCoreProperties()
    {
    }
}