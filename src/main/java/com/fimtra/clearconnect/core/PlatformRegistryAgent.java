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
package com.fimtra.clearconnect.core;

import static com.fimtra.datafission.core.IStatusAttribute.Connection.CONNECTED;
import static com.fimtra.datafission.core.IStatusAttribute.Connection.DISCONNECTED;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointAddressFactory;
import com.fimtra.channel.TransportChannelBuilderFactoryLoader;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.IPlatformServiceProxy;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.PlatformCoreProperties.Values;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames;
import com.fimtra.clearconnect.core.PlatformRegistry.ServiceInfoRecordFields;
import com.fimtra.clearconnect.event.EventListenerUtils;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.clearconnect.event.IServiceAvailableListener;
import com.fimtra.clearconnect.event.IServiceInstanceAvailableListener;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.IStatusAttribute;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.core.ProxyContext.IRemoteSystemRecordNames;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.executors.IContextExecutor;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.FastDateFormat;
import com.fimtra.util.Log;
import com.fimtra.util.NotifyingCache;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.ThreadUtils;
import com.fimtra.util.is;

/**
 * The primary implementation for interacting with the platform.
 * <p>
 * This has a round-robin approach to connecting to the registry service if it loses its connection.
 *
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public final class PlatformRegistryAgent implements IPlatformRegistryAgent
{
    static
    {
        // bootstrap the banner
        final String version = PlatformUtils.VERSION;
    }

    static boolean platformRegistryRpcsAvailable(final Set<String> rpcNames)
    {
        return rpcNames.contains(PlatformRegistry.REGISTER) && rpcNames.contains(PlatformRegistry.DEREGISTER)
                && rpcNames.contains(PlatformRegistry.GET_PLATFORM_NAME) && rpcNames.contains(
                PlatformRegistry.GET_HEARTBEAT_CONFIG);
    }

    private final static class RegisterRpcNotAvailableException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        RegisterRpcNotAvailableException()
        {
        }
    }

    private static String addProcId(String agentName)
    {
        final String processName = ManagementFactory.getRuntimeMXBean().getName();
        if (!agentName.contains(processName))
        {
            return agentName + "_" + processName;
        }
        return agentName;
    }

    final long startTime;
    final String agentName;
    final String hostQualifiedAgentName;
    volatile String platformName;
    final ProxyContext registryProxy;
    final Lock createLock;
    /**
     * Ensures idempotent call to {@link #destroy()}
     */
    final AtomicBoolean destroyCalled;
    /**
     * The services registered through this agent. Key=function of {serviceFamily,serviceMember}
     */
    final ConcurrentMap<String, PlatformServiceInstance> localPlatformServiceInstances;
    final ConcurrentMap<String, PlatformServiceProxy> serviceProxies;
    final ConcurrentMap<String, PlatformServiceProxy> serviceInstanceProxies;
    final NotifyingCache<IServiceInstanceAvailableListener, String> serviceInstanceAvailableListeners;
    final NotifyingCache<IServiceAvailableListener, String> serviceAvailableListeners;
    final NotifyingCache<IRegistryAvailableListener, String> registryAvailableListeners;
    final AtomicReference<IStatusAttribute.Connection> registryConnectionState;
    final ScheduledExecutorService agentExecutor;

    ScheduledFuture<?> dynamicAttributeUpdateTask;
    PlatformServiceConnectionMonitor registryConnectionMonitor;

    /**
     * Construct the agent connecting to the registry service on the specified host and use the
     * default registry TCP port
     * <p>
     * <b>THIS CONSTRUCTOR PROVIDES NO REGISTRY CONNECTION REDUNDANCY.</b>
     *
     * @param agentName the name of the agent
     * @see PlatformCoreProperties.Values#REGISTRY_PORT
     */
    public PlatformRegistryAgent(String agentName, String registryNode) throws IOException
    {
        this(agentName, registryNode, PlatformCoreProperties.Values.REGISTRY_PORT);
    }

    /**
     * Construct the agent connecting to the registry service on the specified host and TCP port
     * <p>
     * <b>THIS CONSTRUCTOR PROVIDES NO REGISTRY CONNECTION REDUNDANCY.</b>
     *
     * @param agentName the name of the agent
     */
    public PlatformRegistryAgent(String agentName, String registryNode, int registryPort) throws IOException
    {
        this(agentName, new EndPointAddress(registryNode, registryPort));
    }

    /**
     * Construct the agent connecting to one of the available registry servers in the
     * {@link InetSocketAddress} array.
     *
     * @param agentName         the name of the agent - this must be unique across all agents on the platform
     * @param registryAddresses the addresses of registry servers to use - this provides redundancy for registry
     *                          connections
     * @throws RegistryNotAvailableException if the registry is not available
     */
    public PlatformRegistryAgent(String agentName, EndPointAddress... registryAddresses)
            throws RegistryNotAvailableException
    {
        this(agentName, DataFissionProperties.Values.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS,
                registryAddresses);
    }

    /**
     * Construct the agent connecting to one of the available registry servers in the
     * {@link InetSocketAddress} array.
     *
     * @param agentName                     the name of the agent - this must be unique across all agents on the platform
     * @param registryReconnectPeriodMillis the registry reconnection period in milliseconds
     * @param registryAddresses             the addresses of registry servers to use - this provides redundancy for registry
     *                                      connections
     * @throws RegistryNotAvailableException if the registry is not available
     */
    @SuppressWarnings({ "unused" })
    public PlatformRegistryAgent(final String agentName, int registryReconnectPeriodMillis,
            EndPointAddress... registryAddresses) throws RegistryNotAvailableException
    {
        Log.log(this, "Registry addresses: ", Arrays.toString(registryAddresses));
        this.startTime = System.currentTimeMillis();
        this.agentName = addProcId(agentName) + "_" + new FastDateFormat().yyyyMMddHHmmssSSS(System.currentTimeMillis());
        this.hostQualifiedAgentName = PlatformUtils.composeHostQualifiedName(this.agentName);
        this.createLock = new ReentrantLock();
        this.destroyCalled = new AtomicBoolean(false);
        this.localPlatformServiceInstances = new ConcurrentHashMap<>();
        this.serviceProxies = new ConcurrentHashMap<>();
        this.serviceInstanceProxies = new ConcurrentHashMap<>();
        this.registryConnectionState = new AtomicReference<>(DISCONNECTED);

        // make the actual connection to the registry
        this.registryProxy = new ProxyContext(
                PlatformUtils.composeProxyName(PlatformRegistry.SERVICE_NAME, this.agentName),
                PlatformRegistry.CODEC,
                TransportChannelBuilderFactoryLoader.load(PlatformRegistry.CODEC.getFrameEncodingFormat(),
                        registryAddresses), PlatformRegistry.SERVICE_NAME);

        this.registryProxy.setReconnectPeriodMillis(registryReconnectPeriodMillis);
        this.agentExecutor = ThreadUtils.newScheduledExecutorService("agent-executor-" + agentName, 1);

        this.registryAvailableListeners =
                new NotifyingCache<IRegistryAvailableListener, String>(this.agentExecutor)
                {
                    @Override
                    protected void notifyListenerDataAdded(IRegistryAvailableListener listener, String key,
                            String data)
                    {
                        listener.onRegistryConnected();
                    }

                    @Override
                    protected void notifyListenerDataRemoved(IRegistryAvailableListener listener, String key,
                            String data)
                    {
                        listener.onRegistryDisconnected();
                    }
                };

        this.serviceAvailableListeners =
                PlatformUtils.createServiceAvailableNotifyingCache(this.registryProxy,
                        IRegistryRecordNames.SERVICES, this);

        this.serviceInstanceAvailableListeners =
                PlatformUtils.createServiceInstanceAvailableNotifyingCache(this.registryProxy,
                        IRegistryRecordNames.SERVICE_INSTANCES_PER_SERVICE_FAMILY, this);

        // "split-brain" protection
        // setup listening for services lost from the registry - we use this to detect if the
        // registry loses a service but we still have the service...
        this.serviceInstanceAvailableListeners.addListener(
                EventListenerUtils.synchronizedListener(new IServiceInstanceAvailableListener()
                {
                    @Override
                    public void onServiceInstanceUnavailable(final String serviceInstanceId)
                    {
                        // only handle service unavailable signals when the registry is connected -
                        // otherwise let the registry re-connection tasks handle this
                        reRegisterServiceIfConnectedToRegistry(serviceInstanceId);
                    }

                    @Override
                    public void onServiceInstanceAvailable(String serviceInstanceId)
                    {
                        // noop
                    }
                }));

        this.registryProxy.addObserver((imageCopy, atomicChange) -> {
            if (platformRegistryRpcsAvailable(imageCopy.keySet()))
            {
                onRegistryConnected();
            }
        }, IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);

        // wait for the registry name to be received...
        synchronized (this.createLock)
        {
            if (this.platformName == null)
            {
                try
                {
                    this.createLock.wait(
                            PlatformCoreProperties.Values.PLATFORM_AGENT_INITIALISATION_TIMEOUT_MILLIS);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(
                            "Interrupted whilst waiting for registry name from " + registryAddresses[0], e);
                }
            }
        }
        if (this.platformName == null)
        {
            destroy();
            throw new RegistryNotAvailableException(
                    "Registry name has not been received from " + registryAddresses[0]);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this::destroy, agentName + "-shutdownHook"));

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this));
    }

    void onRegistryConnected()
    {
        this.createLock.lock();
        try
        {
            if (this.registryConnectionState.get() != CONNECTED)
            {
                // NOTE: the RPC record may be updated whilst we check it
                // remember; record access via "getRecord" is not thread safe
                final IRecord remoteRpcs =
                        this.registryProxy.getRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
                final Set<String> rpcNames =
                        remoteRpcs == null ? Collections.emptySet() : new HashSet<>(remoteRpcs.keySet());
                if (!platformRegistryRpcsAvailable(rpcNames))
                {
                    Log.log(this, "Waiting for registry RPCs, currently have: ",
                            ObjectUtils.safeToString(rpcNames));
                }
                else
                {
                    this.registryConnectionState.getAndSet(CONNECTED);
                    this.agentExecutor.submit(this::finishRegistryConnection);
                }
            }
        }
        finally
        {
            this.createLock.unlock();
        }
    }

    private void finishRegistryConnection()
    {
        this.createLock.lock();
        try
        {
            try
            {
                final String rpcGetPlatformNameResult =
                        this.registryProxy.getRpc(PlatformRegistry.GET_PLATFORM_NAME).execute().textValue();

                // configure the channel watchdog heartbeat
                String heartbeatConfig = this.registryProxy.getRpc(
                        PlatformRegistry.GET_HEARTBEAT_CONFIG).execute().textValue();
                int indexOf = heartbeatConfig.indexOf(":");
                if (indexOf > -1)
                {
                    try
                    {
                        ChannelUtils.WATCHDOG.configure(
                                Integer.parseInt(heartbeatConfig.substring(0, indexOf)),
                                Integer.parseInt(heartbeatConfig.substring(indexOf + 1)));
                    }
                    catch (Exception e)
                    {
                        Log.log(this, "Could not configure heartbeat for channel watchdog", e);
                    }
                }

                synchronized (this.createLock)
                {
                    this.platformName = rpcGetPlatformNameResult;
                    this.createLock.notifyAll();
                }
                this.registryAvailableListeners.notifyListenersDataAdded(this.platformName,
                        this.platformName);
            }
            catch (Exception e)
            {
                Log.log(this, "Could not get platform name!");
            }

            Log.banner(this, "*** REGISTRY CONNECTED *** " + ObjectUtils.safeToString(getRegistryEndPoint()));

            setupRuntimeAttributePublishing();

            startRegistryConnectionMonitor();
        }
        finally
        {
            this.createLock.unlock();
        }

        // This must happen OUTSIDE the create lock else the lock is held whilst waiting for results
        // of RPC registration which can block the registry trying to call the ftServiceInstanceStatus RPC
        // on the service (presumably the thread handling the registry connect was also the context for
        // handling RPC discovery from the registry)

        // (re)publish any service instances managed by this agent
        for (final Iterator<Map.Entry<String, PlatformServiceInstance>> it =
             this.localPlatformServiceInstances.entrySet().iterator(); it.hasNext(); )
        {
            PlatformServiceInstance platformServiceInstance = it.next().getValue();
            Log.log(this, "Preparing to register ", ObjectUtils.safeToString(platformServiceInstance));
            registerServiceWithRetry(platformServiceInstance, it::remove);
        }
    }

    void onRegistryDisconnected()
    {
        this.createLock.lock();
        try
        {
            if (this.registryConnectionState.getAndSet(DISCONNECTED) != DISCONNECTED)
            {
                Log.banner(this, "*** REGISTRY DISCONNECTED ***");
                for (String serviceFamily : this.serviceAvailableListeners.keySet())
                {
                    if (this.serviceAvailableListeners.notifyListenersDataRemoved(serviceFamily))
                    {
                        Log.log(this, "Dropped service: '", serviceFamily, "'");
                    }
                }
                for (String serviceInstance : this.serviceInstanceAvailableListeners.keySet())
                {
                    if (this.serviceInstanceAvailableListeners.notifyListenersDataRemoved(serviceInstance))
                    {
                        Log.log(this, "Dropped serviceInstance: '", serviceInstance, "'");
                    }
                }
                this.registryAvailableListeners.notifyListenersDataRemoved(this.platformName);
                this.platformName = null;
            }
        }
        finally
        {
            this.createLock.unlock();
        }
    }

    @Override
    public EndPointAddress getRegistryEndPoint()
    {
        return this.registryProxy.isConnected() ? this.registryProxy.getEndPointAddress() : null;
    }

    @Override
    public void waitForPlatformService(final String serviceFamily)
    {
        final CountDownLatch servicesAvailable = new CountDownLatch(1);
        final IServiceAvailableListener listener =
                EventListenerUtils.synchronizedListener(new IServiceAvailableListener()
                {
                    @Override
                    public void onServiceUnavailable(String serviceFamily)
                    {
                    }

                    @Override
                    public void onServiceAvailable(String serviceFamilyAvailable)
                    {
                        if (serviceFamily == null || is.eq(serviceFamilyAvailable, serviceFamily))
                        {
                            servicesAvailable.countDown();
                        }
                    }
                });
        addServiceAvailableListener(listener);
        Log.log(this, "Waiting for availability of service '", serviceFamily, "' ...");
        try
        {
            try
            {
                if (!servicesAvailable.await(
                        PlatformCoreProperties.Values.PLATFORM_AGENT_SERVICES_AVAILABLE_TIMEOUT_MILLIS,
                        TimeUnit.MILLISECONDS))
                {
                    throw new RuntimeException("Service '" + serviceFamily + "' is not available");
                }
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(
                        "Interrupted whilst waiting for " + serviceFamily + " to be available", e);
            }
        }
        finally
        {
            removeServiceAvailableListener(listener);
        }
        Log.log(this, "Service available '", serviceFamily, "'");
    }

    @Override
    public int getRegistryReconnectPeriodMillis()
    {
        return this.registryProxy.getReconnectPeriodMillis();
    }

    @Override
    public void setRegistryReconnectPeriodMillis(int reconnectPeriodMillis)
    {
        this.registryProxy.setReconnectPeriodMillis(reconnectPeriodMillis);
    }

    @Override
    public boolean addServiceAvailableListener(final IServiceAvailableListener listener)
    {
        return this.serviceAvailableListeners.addListener(listener);
    }

    @Override
    public boolean removeServiceAvailableListener(IServiceAvailableListener listener)
    {
        return this.serviceAvailableListeners.removeListener(listener);
    }

    @Override
    public boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String host,
            WireProtocolEnum wireProtocol, RedundancyModeEnum redundancyMode)
    {
        return createPlatformServiceInstance(serviceFamily, serviceMember, host,
                PlatformUtils.getNextAvailableServicePort(), wireProtocol, redundancyMode);
    }

    @Override
    public boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String host,
            int port, WireProtocolEnum wireProtocol, RedundancyModeEnum redundancyMode)
    {
        return createPlatformServiceInstance(serviceFamily, serviceMember, host, port, wireProtocol,
                redundancyMode, TransportTechnologyEnum.getDefaultFromSystemProperty());
    }

    @Override
    public boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String hostName,
            WireProtocolEnum wireProtocol, RedundancyModeEnum redundancyMode,
            TransportTechnologyEnum transportTechnology)
    {
        return createPlatformServiceInstance(serviceFamily, serviceMember, hostName,
                transportTechnology.getNextAvailableServicePort(), wireProtocol, redundancyMode,
                transportTechnology);
    }

    @Override
    public boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String hostName,
            int port, WireProtocolEnum wireProtocol, RedundancyModeEnum redundancyMode,
            TransportTechnologyEnum transportTechnology)
    {
        return createPlatformServiceInstance(serviceFamily, serviceMember, hostName, port, wireProtocol,
                redundancyMode, null, null, null, transportTechnology);
    }

    @Override
    public boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String host,
            int port, WireProtocolEnum wireProtocol, RedundancyModeEnum redundancyMode,
            IContextExecutor coreExecutor, IContextExecutor rpcExecutor,
            ScheduledExecutorService utilityExecutor, TransportTechnologyEnum transportTechnology)
    {
        this.createLock.lock();
        try
        {
            final String platformServiceInstanceID =
                    PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember);
            PlatformServiceInstance platformServiceInstance =
                    this.localPlatformServiceInstances.get(platformServiceInstanceID);
            if (platformServiceInstance != null && platformServiceInstance.isActive())
            {
                return false;
            }
            try
            {
                platformServiceInstance =
                        new PlatformServiceInstance(this.platformName, serviceFamily, serviceMember,
                                wireProtocol, redundancyMode, host, port, coreExecutor, rpcExecutor,
                                utilityExecutor, transportTechnology);
                registerService(platformServiceInstance);
                this.localPlatformServiceInstances.put(platformServiceInstanceID, platformServiceInstance);
                return true;
            }
            catch (Exception e)
            {
                Log.log(this,
                        "Could not create service " + platformServiceInstanceID + " at " + host + ":" + port,
                        e);
                if (platformServiceInstance != null)
                {
                    platformServiceInstance.destroy();
                }
                return false;
            }
        }
        finally
        {
            this.createLock.unlock();
        }
    }

    void registerService(final PlatformServiceInstance serviceInstance)
            throws TimeOutException, ExecutionException
    {
        final IRpcInstance registerRpc = this.registryProxy.getRpc(PlatformRegistry.REGISTER);
        if (registerRpc == null)
        {
            throw new RegisterRpcNotAvailableException();
        }

        if (serviceInstance.isActive())
        {
            // *NEW* FT services always start as standby
            serviceInstance.initialiseFtState();

            final CountDownLatch latch = new CountDownLatch(1);
            final IServiceInstanceAvailableListener listener =
                    EventListenerUtils.synchronizedListener(new IServiceInstanceAvailableListener() {
                        @Override
                        public void onServiceInstanceAvailable(String serviceInstanceId)
                        {
                            final String registeredServiceInstanceId = serviceInstance.context.getName();
                            if (is.eq(serviceInstanceId, registeredServiceInstanceId))
                            {
                                removeServiceInstanceAvailableListener(this);
                                latch.countDown();
                            }
                        }

                        @Override
                        public void onServiceInstanceUnavailable(String serviceInstanceId)
                        {
                        }

                    });
            addServiceInstanceAvailableListener(listener);

            try
            {
                Log.log(this, "Registering ", ObjectUtils.safeToString(serviceInstance));
                try
                {
                    registerRpc.execute(TextValue.valueOf(serviceInstance.getPlatformServiceFamily()),
                            TextValue.valueOf(serviceInstance.getWireProtocol().toString()), TextValue.valueOf(serviceInstance.getEndPointAddress().getNode()),
                            LongValue.valueOf(serviceInstance.getEndPointAddress().getPort()), TextValue.valueOf(serviceInstance.getPlatformServiceMemberName()),
                            TextValue.valueOf(serviceInstance.getRedundancyMode().toString()), TextValue.valueOf(this.agentName),
                            TextValue.valueOf(serviceInstance.publisher.getTransportTechnology().toString()));
                }
                catch (ExecutionException e)
                {
                    if (e.getCause() instanceof AlreadyRegisteredException)
                    {
                        final AlreadyRegisteredException details = (AlreadyRegisteredException) e.getCause();
                        if (is.eq(this.agentName, details.agentName) && is.eq(details.port,
                                serviceInstance.endPointAddress.getPort()) && is.eq(details.nodeName,
                                serviceInstance.endPointAddress.getNode()) && is.eq(details.redundancyMode,
                                serviceInstance.getRedundancyMode().toString()))
                        // NOTE: we're not checking the transport tech or wire protocol
                        {
                            Log.log(this, "Registry has already registered ",
                                    ObjectUtils.safeToString(serviceInstance));
                        }
                    }
                    else
                    {
                        throw e;
                    }
                }

                // now setup an "expectation" that the service will become registered - if the service
                // is not registered in 30 secs, say, then something has gone wrong and a re-register is
                // needed
                try
                {
                    if (!latch.await(Values.PLATFORM_AGENT_SERVICE_REGISTRATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                    {
                        throw new TimeOutException(
                                "Did not get confirmation of registration of " + serviceInstance + " after waiting "
                                        + Values.PLATFORM_AGENT_SERVICE_REGISTRATION_TIMEOUT_MILLIS + "ms");
                    }
                }
                catch (InterruptedException e)
                {
                    throw new ExecutionException(
                            "Interrupted whilst waiting for registration confirmation of " + serviceInstance);
                }
            }
            finally
            {
                removeServiceInstanceAvailableListener(listener);
            }
        }
    }

    @Override
    public boolean destroyPlatformServiceInstance(String serviceFamily, String serviceMember)
    {
        this.createLock.lock();
        try
        {
            final String platformServiceInstanceID =
                    PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember);
            final PlatformServiceInstance service =
                    this.localPlatformServiceInstances.remove(platformServiceInstanceID);
            if (service != null)
            {
                try
                {
                    this.registryProxy.getRpc(PlatformRegistry.DEREGISTER).execute(
                            TextValue.valueOf(serviceFamily), TextValue.valueOf(serviceMember));
                }
                catch (Exception e)
                {
                    Log.log(this, "Could not deregister service " + platformServiceInstanceID
                            + ", continuing to destroy", e);
                }
                try
                {
                    service.destroy();
                }
                catch (Exception e)
                {
                    Log.log(this, "Could not destroy service " + platformServiceInstanceID, e);
                }
                return true;
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Could not destroy service " + serviceFamily + ":" + serviceMember, e);
        }
        finally
        {
            this.createLock.unlock();
        }
        return false;
    }

    @Override
    public IPlatformServiceInstance getPlatformServiceInstance(String serviceFamily, String serviceMember)
    {
        return this.localPlatformServiceInstances.get(
                PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember));
    }

    @Override
    public IPlatformServiceProxy getPlatformServiceProxy(final String serviceFamily)
    {
        this.createLock.lock();
        try
        {
            PlatformServiceProxy proxy = this.serviceProxies.get(serviceFamily);
            if (proxy == null || !proxy.isActive())
            {
                if (!this.serviceAvailableListeners.keySet().contains(serviceFamily))
                {
                    Log.log(this, "No service available for ", serviceFamily,
                            (proxy != null ? " (proxy is inactive)" : ""));
                    return null;
                }

                // NOTE: we cannot go directly to a local service because the platform registry
                // decides which instance is the active one.

                Map<String, IValue> serviceInfoRecord =
                        getPlatformServiceInstanceInfoRecordImageForService(serviceFamily);
                if (serviceInfoRecord == null)
                {
                    Log.log(this, "No service info record available for ", serviceFamily);
                    return null;
                }

                Log.log(this, "getPlatformServiceProxy serviceInfoRecord is ", serviceInfoRecord.toString());

                final ICodec<?> codec = PlatformUtils.getCodecFromServiceInfoRecord(serviceInfoRecord);
                final String host = PlatformUtils.getHostNameFromServiceInfoRecord(serviceInfoRecord);
                final int port = PlatformUtils.getPortFromServiceInfoRecord(serviceInfoRecord);
                final TransportTechnologyEnum transportTechnology =
                        PlatformUtils.getTransportTechnologyFromServiceInfoRecord(serviceInfoRecord);
                proxy = new PlatformServiceProxy(this, serviceFamily, codec, host, port, transportTechnology);
                this.serviceProxies.put(serviceFamily, proxy);
            }
            return proxy;
        }
        finally
        {
            this.createLock.unlock();
        }
    }

    @Override
    public IPlatformServiceProxy getPlatformServiceInstanceProxy(String serviceFamily, String serviceMember)
    {
        this.createLock.lock();
        try
        {
            final String serviceInstanceId =
                    PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember);

            PlatformServiceProxy proxy = this.serviceInstanceProxies.get(serviceInstanceId);
            if (proxy == null || !proxy.isActive())
            {
                if (!this.serviceInstanceAvailableListeners.keySet().contains(serviceInstanceId))
                {
                    Log.log(this, "No service instance available for ", serviceInstanceId,
                            (proxy != null ? " (proxy is inactive)" : ""));
                    return null;
                }
                Map<String, IValue> serviceInfoRecord = this.registryProxy.getRemoteRecordImage(
                        ServiceInfoRecordFields.SERVICE_INFO_RECORD_NAME_PREFIX + serviceInstanceId,
                        getRemoteRecordImageTimeoutMillis());
                if (serviceInfoRecord == null)
                {
                    Log.log(this, "No service info record available for ", serviceInstanceId);
                    return null;
                }
                final ICodec<?> codec = PlatformUtils.getCodecFromServiceInfoRecord(serviceInfoRecord);
                final String host = PlatformUtils.getHostNameFromServiceInfoRecord(serviceInfoRecord);
                final int port = PlatformUtils.getPortFromServiceInfoRecord(serviceInfoRecord);
                final TransportTechnologyEnum transportTechnology =
                        PlatformUtils.getTransportTechnologyFromServiceInfoRecord(serviceInfoRecord);
                proxy = new PlatformServiceProxy(this, serviceFamily, codec, host, port, transportTechnology);

                final IEndPointAddressFactory endPointAddressFactory = () -> {
                    Log.log(this, "Obtaining service info record for '", serviceInstanceId, "'");
                    final Map<String, IValue> serviceInfoRecord1 = this.registryProxy.getRemoteRecordImage(
                            ServiceInfoRecordFields.SERVICE_INFO_RECORD_NAME_PREFIX + serviceInstanceId,
                            getRemoteRecordImageTimeoutMillis());
                    if (serviceInfoRecord1 == null)
                    {
                        Log.log(this, "No service info record found for '", serviceInstanceId, "'");
                        return null;
                    }
                    final String node = PlatformUtils.getHostNameFromServiceInfoRecord(serviceInfoRecord1);
                    final int port1 = PlatformUtils.getPortFromServiceInfoRecord(serviceInfoRecord1);
                    final EndPointAddress endPointAddress = new EndPointAddress(node, port1);
                    Log.log(this, "Service instance '" + serviceInstanceId, "' ",
                            ObjectUtils.safeToString(endPointAddress));
                    return endPointAddress;
                };

                proxy.proxyContext.setTransportChannelBuilderFactory(
                        TransportChannelBuilderFactoryLoader.load(codec.getFrameEncodingFormat(),
                                endPointAddressFactory));

                this.serviceInstanceProxies.put(serviceInstanceId, proxy);
            }
            return proxy;
        }
        finally
        {
            this.createLock.unlock();
        }
    }

    @Override
    public boolean destroyPlatformServiceProxy(String serviceFamily)
    {
        return doDestroyProxy(serviceFamily, this.serviceProxies);
    }

    @Override
    public boolean destroyPlatformServiceInstanceProxy(String serviceInstanceId)
    {
        return doDestroyProxy(serviceInstanceId, this.serviceInstanceProxies);
    }

    final boolean doDestroyProxy(String serviceFamily, ConcurrentMap<String, PlatformServiceProxy> proxies)
    {
        this.createLock.lock();
        try
        {
            final PlatformServiceProxy proxy = proxies.remove(serviceFamily);
            if (proxy == null)
            {
                return false;
            }
            proxy.destroy();
            return true;
        }
        catch (Exception e)
        {
            Log.log(this, "Could not destroy proxy to " + serviceFamily, e);
            return false;
        }
        finally
        {
            this.createLock.unlock();
        }
    }

    @Override
    public void destroy()
    {
        if (this.destroyCalled.getAndSet(true))
        {
            return;
        }

        this.createLock.lock();
        try
        {
            Log.log(this, "Destroying ", ObjectUtils.safeToString(this));

            try
            {
                this.registryProxy.destroy();
            }
            catch (Exception e)
            {
                Log.log(this, "Could not destroy " + ObjectUtils.safeToString(this.registryProxy), e);
            }

            this.registryConnectionState.set(DISCONNECTED);

            this.registryConnectionMonitor.destroy();

            try
            {
                this.serviceAvailableListeners.destroy();
                this.serviceInstanceAvailableListeners.destroy();
                this.registryAvailableListeners.destroy();
            }
            catch (Exception e)
            {
                Log.log(this, "Could not destroy listener notifiers", e);
            }

            if (this.dynamicAttributeUpdateTask != null)
            {
                try
                {
                    this.dynamicAttributeUpdateTask.cancel(false);
                }
                catch (Exception e)
                {
                    Log.log(this, "Could not cancel dynamicAttributeUpdateTask", e);
                }
            }

            for (PlatformServiceInstance service : new HashSet<>(this.localPlatformServiceInstances.values()))
            {
                destroyPlatformServiceInstance(service.serviceFamily, service.serviceMember);
            }
            for (PlatformServiceProxy proxy : new HashSet<>(this.serviceProxies.values()))
            {
                destroyPlatformServiceProxy(proxy.serviceFamily);
            }
            for (String serviceInstanceId : new HashSet<>(this.serviceInstanceProxies.keySet()))
            {
                destroyPlatformServiceInstanceProxy(serviceInstanceId);
            }

            try
            {
                this.agentExecutor.shutdownNow();
            }
            catch (Exception e)
            {
                Log.log(this, "Could not shutdown executor", e);
            }
        }
        finally
        {
            this.createLock.unlock();
        }
    }

    /**
     * Make a remote call to the registry to obtain the record that describes the connection details
     * for the named service (the 'service info' record).
     * <p>
     * <b>This method makes a subscription to the registry over the network to get the record so is
     * not a cheap method.</b>
     *
     * @param serviceFamily the service name for the service info record to get
     * @return the service record image, <code>null</code> if the record could not be obtained
     * (either it doesn't exist or a network problem occurred)
     */
    IRecord getPlatformServiceInstanceInfoRecordImageForService(String serviceFamily)
    {
        try
        {
            final IRpcInstance rpc;
            if ((rpc = this.registryProxy.getRpc(PlatformRegistry.GET_SERVICE_INFO_RECORD_NAME_FOR_SERVICE))
                    == null)
            {
                Log.log(this, "Could not get RPC ", PlatformRegistry.GET_SERVICE_INFO_RECORD_NAME_FOR_SERVICE,
                        " to get connection record for ", serviceFamily, "'");
                return null;
            }

            final IValue instanceForService = rpc.execute(TextValue.valueOf(serviceFamily));
            if (instanceForService == null)
            {
                Log.log(this, "Registry has no service registered for '", serviceFamily, "'");
                return null;
            }
            return this.registryProxy.getRemoteRecordImage(instanceForService.textValue(),
                    getRemoteRecordImageTimeoutMillis());
        }
        catch (Exception e)
        {
            Log.log(this, "Could not get service instance to use for service '" + serviceFamily + "'", e);
            return null;
        }
    }

    static long getRemoteRecordImageTimeoutMillis()
    {
        return PlatformCoreProperties.Values.PLATFORM_AGENT_REMOTE_RECORD_IMAGE_TIMEOUT_MILLIS;
    }

    @Override
    public String getPlatformName()
    {
        return this.platformName;
    }

    @Override
    public String toString()
    {
        return "PlatformRegistryAgent [" + this.agentName + "] [" + this.platformName + "] "
                + this.registryProxy.getChannelString();
    }

    @Override
    public String getAgentName()
    {
        return this.hostQualifiedAgentName;
    }

    @Override
    public void addRegistryAvailableListener(IRegistryAvailableListener listener)
    {
        this.registryAvailableListeners.addListener(listener);
    }

    @Override
    public void removeRegistryAvailableListener(IRegistryAvailableListener listener)
    {
        this.registryAvailableListeners.removeListener(listener);
    }

    @Override
    public boolean addServiceInstanceAvailableListener(IServiceInstanceAvailableListener listener)
    {
        return this.serviceInstanceAvailableListeners.addListener(listener);
    }

    @Override
    public boolean removeServiceInstanceAvailableListener(IServiceInstanceAvailableListener listener)
    {
        return this.serviceInstanceAvailableListeners.removeListener(listener);
    }

    void setupRuntimeAttributePublishing()
    {
        // tell the registry about the runtime static attributes (one-time call)
        this.agentExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                final int timeoutMillis = PlatformRegistryAgent.this.registryProxy.getReconnectPeriodMillis();

                try
                {
                    final IRpcInstance rpc =
                            ContextUtils.getRpc(PlatformRegistryAgent.this.registryProxy, timeoutMillis,
                                    PlatformRegistry.RUNTIME_STATIC);
                    try
                    {
                        final String runtimeDescription =
                                System.getProperty("os.name") + " (" + System.getProperty("os.version")
                                        + "), " + System.getProperty("os.arch") + ", Java "
                                        + System.getProperty("java.version") + ", ClearConnect "
                                        + PlatformUtils.VERSION;
                        final String host = TcpChannelUtils.LOCALHOST_IP;
                        final Runtime runtime = Runtime.getRuntime();
                        final long cpuCount = runtime.availableProcessors();
                        final String user = System.getProperty("user.name");

                        rpc.executeNoResponse(TextValue.valueOf(PlatformRegistryAgent.this.agentName),
                                TextValue.valueOf(host), TextValue.valueOf(runtimeDescription),
                                TextValue.valueOf(user), LongValue.valueOf(cpuCount));
                    }
                    catch (Exception e)
                    {
                        Log.log(this, "Could not invoke " + PlatformRegistry.RUNTIME_STATIC, e);
                    }
                }
                catch (TimeOutException e1)
                {
                    Log.log(this,
                            "RPC not available from platform registry: " + PlatformRegistry.RUNTIME_STATIC
                                    + ", rescheduling in " + timeoutMillis + "ms", e1);

                    PlatformRegistryAgent.this.agentExecutor.schedule(this, timeoutMillis,
                            TimeUnit.MILLISECONDS);
                }
            }
        });

        // tell the registry about the runtime dynamic attributes (periodic call)
        if (this.dynamicAttributeUpdateTask != null)
        {
            try
            {
                this.dynamicAttributeUpdateTask.cancel(false);
            }
            catch (Exception e)
            {
                Log.log(this, "Could not cancel dynamicAttributeUpdateTask", e);
            }
        }

        final Runnable dynamicStatsTask = new Runnable()
        {
            final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
            IRpcInstance rpc;

            @Override
            public void run()
            {
                try
                {
                    if (this.rpc == null)
                    {
                        this.rpc = ContextUtils.getRpc(PlatformRegistryAgent.this.registryProxy,
                                PlatformRegistryAgent.this.registryProxy.getReconnectPeriodMillis(),
                                PlatformRegistry.RUNTIME_DYNAMIC);
                    }

                    final long[] stats = ContextUtils.getCoreStats();
                    final long qOverflow = stats[0];
                    final long qTotalSubmitted = stats[1];
                    final Runtime runtime = Runtime.getRuntime();
                    final double MB = 1d / (1024 * 1024);
                    final long freeMemory = runtime.freeMemory();
                    final long memUsed = (long) ((runtime.totalMemory() - freeMemory) * MB);
                    final long memAvailable = (long) (freeMemory * MB);
                    final long threadCount = this.threadMxBean.getThreadCount();
                    final double inverseLoggingPeriodSecs =
                            1d / DataFissionProperties.Values.STATS_LOGGING_PERIOD_SECS;
                    final long qTotalExecuted = stats[2];
                    final long eventsPerSec = (long) ((qTotalExecuted) * inverseLoggingPeriodSecs);
                    final long uptime =
                            (System.currentTimeMillis() - PlatformRegistryAgent.this.startTime) / 1000;
                    try
                    {
                        this.rpc.executeNoResponse(TextValue.valueOf(PlatformRegistryAgent.this.agentName),
                                LongValue.valueOf(qOverflow), LongValue.valueOf(qTotalSubmitted),
                                LongValue.valueOf(memUsed), LongValue.valueOf(memAvailable),
                                LongValue.valueOf(threadCount),
                                LongValue.valueOf(ContextUtils.getGcDutyCycle()),
                                LongValue.valueOf(eventsPerSec), LongValue.valueOf(uptime));
                    }
                    catch (Exception e)
                    {
                        Log.log(this, "Could not invoke " + this.rpc, e);
                    }
                }
                catch (TimeOutException e1)
                {
                    Log.log(this,
                            "RPC not available from platform registry: " + PlatformRegistry.RUNTIME_DYNAMIC,
                            e1);
                }
            }
        };
        this.dynamicAttributeUpdateTask = this.agentExecutor.scheduleWithFixedDelay(dynamicStatsTask,
                DataFissionProperties.Values.STATS_LOGGING_PERIOD_SECS,
                DataFissionProperties.Values.STATS_LOGGING_PERIOD_SECS, TimeUnit.SECONDS);
    }

    @Deprecated
    @Override
    public ScheduledExecutorService getUtilityExecutor()
    {
        return this.registryProxy.getUtilityExecutor();
    }

    @Override
    public Map<String, IPlatformServiceProxy> getActiveProxies()
    {
        return new HashMap<>(this.serviceProxies);
    }

    private void startRegistryConnectionMonitor()
    {
        if (this.registryConnectionMonitor == null)
            // listen for connection status changes in the registry service
            this.registryConnectionMonitor =
                    new PlatformServiceConnectionMonitor(this.registryProxy, PlatformRegistry.SERVICE_NAME)
                    {
                        @Override
                        protected void onPlatformServiceReconnecting()
                        {
                            onPlatformServiceDisconnected();
                        }

                        @Override
                        protected void onPlatformServiceDisconnected()
                        {
                            onRegistryDisconnected();
                        }

                        @Override
                        protected void onPlatformServiceConnected()
                        {
                            onRegistryConnected();
                        }
                    };
    }

    private void reRegisterServiceIfConnectedToRegistry(String serviceInstanceId)
    {
        if (this.localPlatformServiceInstances.get(serviceInstanceId) != null)
        {
            if (this.registryConnectionState.get() == CONNECTED)
            {
                this.agentExecutor.execute(() -> {
                    // check again in case its removed
                    final PlatformServiceInstance serviceInstance =
                            this.localPlatformServiceInstances.get(serviceInstanceId);
                    if (serviceInstance != null && serviceInstance.isActive())
                    {
                        Log.log(this, "Re-registering ", serviceInstanceId);
                        registerServiceWithRetry(serviceInstance, null);
                    }
                });
            }
            else
            {
                Log.log(this, "Not connected to registry, won't re-register ", serviceInstanceId);
            }
        }
    }
    
    void registerServiceWithRetry(PlatformServiceInstance platformServiceInstance, final Runnable failureTask)
    {
        final int maxTries = PlatformCoreProperties.Values.PLATFORM_AGENT_MAX_SERVICE_REGISTER_TRIES;
        int tries = 0;
        boolean registered = false;
        while (!registered && tries++ < maxTries && platformServiceInstance.isActive())
        {
            try
            {
                registerService(platformServiceInstance);
                registered = true;
            }
            catch (RegisterRpcNotAvailableException e)
            {
                Log.log(this,
                        "Register RPC not available (is the registry disconnected?), aborting registration of "
                                + ObjectUtils.safeToString(platformServiceInstance)
                                + ", if the registry reconnects this service will be re-registered", e);
                return;
            }
            catch (Exception e)
            {
                Log.log(this, " (" + tries + "/" + maxTries
                        + ") Failed attempt registering " + ObjectUtils.safeToString(platformServiceInstance)
                        + (tries < maxTries ? "...retrying" : "...MAX ATTEMPTS REACHED"), e);
            }
        }

        if (!registered)
        {
            Log.log(this, "*** ALERT *** Could not register ",
                    ObjectUtils.safeToString(platformServiceInstance));
            try
            {
                platformServiceInstance.destroy();
            }
            catch (Exception e)
            {
                Log.log(this, "*** ALERT *** Could not destroy " + ObjectUtils.safeToString(
                        platformServiceInstance), e);
            }
            finally
            {
                if (failureTask != null)
                {
                    failureTask.run();
                }
            }
        }
    }
}
