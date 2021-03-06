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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.core.PlatformRegistry;
import com.fimtra.clearconnect.core.PlatformUtils;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.clearconnect.event.IServiceAvailableListener;
import com.fimtra.clearconnect.event.IServiceInstanceAvailableListener;
import com.fimtra.executors.IContextExecutor;

/**
 * A platform registry agent (the 'agent') is the API entry into the ClearConnect platform. At the
 * centre of the platform is the {@link PlatformRegistry} (the 'registry'). The registry maintains a
 * register of all {@link IPlatformServiceInstance} ('service instances') available on the platform.
 * Service instances are created and registered via the agent. The agent itself registers with the
 * registry and so also becomes part of the platform.
 * <p>
 * An agent creates service instances that support a 'service'. Once a service instance is
 * registered, its service is known to all platform agents. The service can be accessed by
 * {@link IPlatformServiceProxy} ('proxies') that the agent constructs. The proxies allow
 * interaction with a service via one of the service instances.
 * <p>
 * Applications can listen for newly created services via the
 * {@link #addServiceAvailableListener(IServiceAvailableListener)} method and obtain remote access
 * to these via the {@link IPlatformServiceProxy} interface. Applications can create services via
 * the {@link #createPlatformServiceInstance(String, String, String, WireProtocolEnum, RedundancyModeEnum)}
 * method and use the {@link IPlatformServiceInstance} by obtaining it via the
 * {@link #getPlatformServiceInstance(String, String)} method.
 * <p>
 * Service names are unique across the platform. For a discussion on the fault-tolerance and
 * load-balancing service modes, refer to the {@link IPlatformServiceInstance} documentation.
 *
 * @see IPlatformServiceInstance
 * @see IPlatformServiceProxy
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public interface IPlatformRegistryAgent
{
    /**
     * Represents when the registry is not available during agent construction.
     * 
     * @author Ramon Servadei
     */
    class RegistryNotAvailableException extends IOException
    {
        private static final long serialVersionUID = 1L;

        public RegistryNotAvailableException(String message)
        {
            super(message);
        }
    }

    /**
     * @return the name of the agent
     */
    String getAgentName();

    /**
     * @return the name of the platform
     */
    String getPlatformName();

    /**
	 * This is a convenience method that will block for 60 seconds until the named service becomes
	 * available. If the service is not available, a {@link RuntimeException} is thrown.
	 * <p>
	 * An agent needs to wait for services to be available before it is 'useful'. Typically this can
	 * be done by registering an {@link IServiceAvailableListener} using
	 * {@link #addServiceAvailableListener(IServiceAvailableListener)}. However this can be a bit
	 * tedious if you just want to know when a particular service is available. This method exists
	 * for this.
	 * 
	 * @param serviceFamily
	 *            the name of the platform service being expected, if <code>null</code> then any
	 *            platform service can be expected
	 * @throws RuntimeException
	 *             if the service is not found after 60 seconds (the timeout is configurable)
	 */
    void waitForPlatformService(String serviceFamily);

    /**
     * Destroy the agent and disconnect it from the registry service. This also destroys all
     * platform service instances and proxies.
     */
    void destroy();

    /**
     * @return the period in milliseconds to wait before trying a reconnect to the registry
     */
    int getRegistryReconnectPeriodMillis();

    /**
     * Set the period to wait before attempting to reconnect to the registry after the TCP
     * connection has been unexpectedly broken
     * 
     * @param reconnectPeriodMillis
     *            the period in milliseconds to wait before trying a reconnect to the registry
     */
    void setRegistryReconnectPeriodMillis(int reconnectPeriodMillis);

    /**
     * Add a listener to receive events when the agent is connected/disconnected to the registry
     * 
     * @param listener
     *            the listener to add
     */
    void addRegistryAvailableListener(IRegistryAvailableListener listener);

    /**
     * Remove a listener from receiving events when the agent is connected/disconnected to the
     * registry
     * 
     * @param listener
     *            the listener to add
     */
    void removeRegistryAvailableListener(IRegistryAvailableListener listener);

    /**
     * Add a listener to receive platform service availability signals. This method can be called to
     * register multiple listeners.
     * 
     * @param listener
     *            the listener to register
     * @return <code>true</code> if the listener was added, <code>false</code> if it already was
     *         registered
     */
    boolean addServiceAvailableListener(IServiceAvailableListener listener);

    /**
     * Remove a previously added listener. After this method completes, the listener will no longer
     * receive platform service availability signals.
     * 
     * @param listener
     *            the listener to remove
     * @return <code>true</code> if the listener was removed, <code>false</code> if it was not
     *         removed because it was not registered in the first place
     */
    boolean removeServiceAvailableListener(IServiceAvailableListener listener);

    /**
     * Add a listener to receive notifications about distinct platform service <b>instance</b>
     * availability. This method can be called to register multiple listeners.
     * 
     * @param listener
     *            the listener to register
     * @return <code>true</code> if the listener was added, <code>false</code> if it already was
     *         registered
     */
    boolean addServiceInstanceAvailableListener(IServiceInstanceAvailableListener listener);

    /**
     * Remove a previously added listener. After this method completes, the listener will no longer
     * receive platform service availability signals.
     * 
     * @param listener
     *            the listener to remove
     * @return <code>true</code> if the listener was removed, <code>false</code> if it was not
     *         removed because it was not registered in the first place
     */
    boolean removeServiceInstanceAvailableListener(IServiceInstanceAvailableListener listener);

    /**
     * Create an {@link IPlatformServiceInstance} instance that uses a default assigned port and
     * the default event executor. The registry agent will maintain a reference to the created
     * platform service instance by its platform service instance ID, see
     * {@link PlatformUtils#composePlatformServiceInstanceID(String, String)}. Additionally, the
     * registry agent registers the new platform service instance with the platform registry against
     * the given platform service.
     * 
     * @see #createPlatformServiceInstance(String, String, String, int, WireProtocolEnum,
     *      RedundancyModeEnum, IContextExecutor, IContextExecutor, ScheduledExecutorService,
     *      TransportTechnologyEnum)
     */
    boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String hostName,
        WireProtocolEnum wireProtocol, RedundancyModeEnum redundacyMode, TransportTechnologyEnum transportTechnology);

    /**
     * Create an {@link IPlatformServiceInstance} instance that uses the default event executor. The
     * registry agent will maintain a reference to the created platform service instance by its
     * platform service instance ID, see
     * {@link PlatformUtils#composePlatformServiceInstanceID(String, String)}. Additionally, the
     * registry agent registers the new platform service instance with the platform registry against
     * the given platform service.
     * 
     * @see #createPlatformServiceInstance(String, String, String, int, WireProtocolEnum,
     *      RedundancyModeEnum, IContextExecutor, IContextExecutor, ScheduledExecutorService,
     *      TransportTechnologyEnum)
     */
    boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String hostName, int port,
        WireProtocolEnum wireProtocol, RedundancyModeEnum redundacyMode, TransportTechnologyEnum transportTechnology);

    /**
     * Create an {@link IPlatformServiceInstance} instance that uses a default assigned port and the
     * default event executor. The registry agent will maintain a reference to the created platform
     * service instance by its platform service instance ID, see
     * {@link PlatformUtils#composePlatformServiceInstanceID(String, String)}. Additionally, the
     * registry agent registers the new platform service instance with the platform registry against
     * the given platform service.
     * 
     * @see #createPlatformServiceInstance(String, String, String, int, WireProtocolEnum,
     *      RedundancyModeEnum, IContextExecutor, IContextExecutor, ScheduledExecutorService,
     *      TransportTechnologyEnum)
     */
    boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String hostName,
        WireProtocolEnum wireProtocol, RedundancyModeEnum redundacyMode);

    /**
     * Create an {@link IPlatformServiceInstance} instance that uses the default event executor. The
     * registry agent will maintain a reference to the created platform service instance by its
     * platform service instance ID, see
     * {@link PlatformUtils#composePlatformServiceInstanceID(String, String)}. Additionally, the
     * registry agent registers the new platform service instance with the platform registry against
     * the given platform service.
     * 
     * @see #createPlatformServiceInstance(String, String, String, int, WireProtocolEnum,
     *      RedundancyModeEnum, IContextExecutor, IContextExecutor, ScheduledExecutorService,
     *      TransportTechnologyEnum)
     */
    boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String hostName, int port,
        WireProtocolEnum wireProtocol, RedundancyModeEnum redundacyMode);

    /**
     * Create an {@link IPlatformServiceInstance} instance. The registry agent will maintain a
     * reference to the created platform service instance by its platform service instance ID, see
     * {@link PlatformUtils#composePlatformServiceInstanceID(String, String)}. Additionally, the
     * registry agent registers the new platform service instance with the platform registry against
     * the given platform service.
     * 
     * @see IServiceAvailableListener
     * @see RedundancyModeEnum
     * @param serviceFamily
     *            the name of the platform service this platform service instance supports
     * @param serviceMember
     *            the name of this platform service member in the platform service family
     * @param host
     *            the hostname the platform service instance will use for its TCP socket
     * @param port
     *            the TCP port for the platform service instance
     * @param wireProtocol
     *            the wire protocol the instance will use for distributing records to remote proxy
     *            instances
     * @param redundacyMode
     *            the redundancy mode for the platform service instance. Note: all instances of the
     *            same platform service <b>must</b> use the same redundancy mode.
     * @param coreExecutor
     *            the core executor for the events of the service, <code>null</code> to use the
     *            default event executor
     * @param rpcExecutor
     *            the executor for handling RPC calls for the service, <code>null</code> to use the
     *            default RPC executor
     * @param utilityExecutor
     *            the {@link ScheduledExecutorService} for handling utility tasks and timer tasks
     *            for the service, <code>null</code> to use the default utility executor
     * @param transportTechnology
     *            the transport technology that the service instance will use
     * @return <code>true</code> if the platform service instance was created, <code>false</code>
     *         otherwise (the platform service instance ID might be non-unique or may already exist)
     */
    boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String host, int port,
        WireProtocolEnum wireProtocol, RedundancyModeEnum redundacyMode, IContextExecutor coreExecutor,
            IContextExecutor rpcExecutor, ScheduledExecutorService utilityExecutor,
        TransportTechnologyEnum transportTechnology);

    /**
     * Get a local platform service instance by platform service name and platform service member
     * name.
     * <p>
     * <b>This returns the SAME service instance for a given service instance ID (the serviceFamily
     * + serviceMember).</b>
     * 
     * @param serviceFamily
     *            the name of the platform service
     * @param serviceMember
     *            the name of the platform service member
     * @return the platform service instance or <code>null</code> if it doesn't exist
     */
    IPlatformServiceInstance getPlatformServiceInstance(String serviceFamily, String serviceMember);

    /**
     * Destroy a platform service instance. The registry agent will deregister the platform service
     * instance from the platform registry. If there are no more platform service instances for the
     * platform service, the registry will also remove the platform service.
     * 
     * @see IServiceAvailableListener
     * @param serviceFamily
     *            the name of the platform service
     * @param serviceMember
     *            the name of the service member to destroy
     * @return <code>true</code> if the platform service instance was destroyed, <code>false</code>
     *         otherwise
     */
    boolean destroyPlatformServiceInstance(String serviceFamily, String serviceMember);

    /**
     * Get a proxy to a platform service.
     * <p>
     * <b>This returns the SAME proxy instance for the same serviceFamily.</b>
     * 
     * @param serviceFamily
     *            the name of the platform service to get the proxy for
     * @return a proxy to the platform service or <code>null</code> if the platform service does not
     *         exist (there are no platform service instances for the platform service)
     */
    IPlatformServiceProxy getPlatformServiceProxy(String serviceFamily);

    /**
     * Get a proxy to a platform service <b>instance</b>.
     * <p>
     * <b>This returns the SAME proxy instance for the same service instance.</b>
     * 
     * @param serviceFamily
     *            the name of the platform service
     * @param serviceMember
     *            the name of the service member
     * @return a proxy to the platform service <b>instance</b> or <code>null</code> if the platform
     *         service instance does not exist
     */
    IPlatformServiceProxy getPlatformServiceInstanceProxy(String serviceFamily, String serviceMember);

    /**
     * Destroy the proxy that was created for the service family. Any references to the proxy will
     * have a 'dead' proxy.
     * 
     * @param serviceFamily
     *            the name of the platform service that the proxy is for
     * @return <code>true</code> if the proxy was destoyed, <code>false</code> otherwise
     * @see IPlatformServiceComponent#isActive()
     */
    boolean destroyPlatformServiceProxy(String serviceFamily);

    /**
     * Destroy the proxy that was created for the service instance. Any references to the proxy will
     * have a 'dead' proxy.
     * 
     * @param serviceInstanceId
     *            the ID of the platform service instance that the proxy is for
     * @return <code>true</code> if the proxy was destoyed, <code>false</code> otherwise
     * @see IPlatformServiceComponent#isActive()
     */
    boolean destroyPlatformServiceInstanceProxy(String serviceInstanceId);

    /**
     * Get the executor for utility type tasks
     * 
     * @return a {@link ScheduledExecutorService} for utility tasks
     * @deprecated do not use this - this will be removed in ClearConnect 4.0.0
     */
    @Deprecated
    ScheduledExecutorService getUtilityExecutor();

    /**
     * @return a detached copy of the map of all the service names to active
     *         {@link IPlatformServiceProxy} instances created via calls to
     *         {@link #getPlatformServiceProxy(String)}
     */
    Map<String, IPlatformServiceProxy> getActiveProxies();

    /**
     * @return the {@link EndPointAddress} for the current registry connection, <code>null</code> if
     *         not connected
     */
    EndPointAddress getRegistryEndPoint();
}
