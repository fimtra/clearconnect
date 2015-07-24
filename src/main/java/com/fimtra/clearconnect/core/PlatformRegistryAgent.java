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

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointAddressFactory;
import com.fimtra.channel.TransportChannelBuilderFactoryLoader;
import com.fimtra.clearconnect.IDataRadar;
import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.IPlatformServiceProxy;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames;
import com.fimtra.clearconnect.core.PlatformRegistry.ServiceInfoRecordFields;
import com.fimtra.clearconnect.event.IDataRadarListener;
import com.fimtra.clearconnect.event.IDataRadarListener.SignatureMatch;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.clearconnect.event.IServiceAvailableListener;
import com.fimtra.clearconnect.event.IServiceInstanceAvailableListener;
import com.fimtra.clearconnect.expression.IExpression;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.core.ProxyContext.IRemoteSystemRecordNames;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.thimble.ThimbleExecutor;
import com.fimtra.util.FastDateFormat;
import com.fimtra.util.Log;
import com.fimtra.util.NotifyingCache;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.is;

/**
 * The primary implementation for interacting with the platform.
 * <p>
 * This has a round-robin approach to connecting to the registry service if it loses its connection.
 * 
 * @author Ramon Servadei, Paul Mackinlay
 */
public final class PlatformRegistryAgent implements IPlatformRegistryAgent
{
    static boolean platformRegistryRpcsAvailable(final Set<String> rpcNames)
    {
        return rpcNames.contains(PlatformRegistry.REGISTER) && rpcNames.contains(PlatformRegistry.DEREGISTER)
            && rpcNames.contains(PlatformRegistry.GET_PLATFORM_NAME)
            && rpcNames.contains(PlatformRegistry.GET_HEARTBEAT_CONFIG);
    }

    /**
     * A data radar.
     * <p>
     * Equal by object instance.
     * 
     * @author Ramon Servadei
     */
    private final class DataRadar implements IDataRadar, IRecordListener
    {
        final DataRadarSpecification dataRadarSpecification;
        final IDataRadarListener listener;

        DataRadar(DataRadarSpecification dataRadarSpec, IDataRadarListener listener)
        {
            this.dataRadarSpecification = dataRadarSpec;
            this.listener = listener;
        }

        @Override
        public IExpression getDataRadarSignatureExpression()
        {
            return this.dataRadarSpecification.getDataRadarSignatureExpression();
        }

        @Override
        public void onChange(IRecord radarRecord, IRecordChange atomicChange)
        {
            final Set<SignatureMatch> found = new HashSet<SignatureMatch>();
            final Set<SignatureMatch> lost = new HashSet<SignatureMatch>();

            Map.Entry<String, IValue> entry = null;
            String key = null;
            IValue value = null;
            SignatureMatch signatureMatch;
            for (Iterator<Map.Entry<String, IValue>> it = atomicChange.getPutEntries().entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                key = entry.getKey();
                value = entry.getValue();
                signatureMatch = new SignatureMatch(key, value.textValue());
                found.add(signatureMatch);
            }

            // process overwritten and removed entries as the same
            for (Iterator<Map.Entry<String, IValue>> it = atomicChange.getOverwrittenEntries().entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                key = entry.getKey();
                value = entry.getValue();
                signatureMatch = new SignatureMatch(key, value.textValue());
                found.remove(signatureMatch);
                lost.add(signatureMatch);
            }
            for (Iterator<Map.Entry<String, IValue>> it = atomicChange.getRemovedEntries().entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                key = entry.getKey();
                value = entry.getValue();
                signatureMatch = new SignatureMatch(key, value.textValue());
                found.remove(signatureMatch);
                lost.add(signatureMatch);
            }

            try
            {
                if (this.listener != null)
                {
                    this.listener.onRadarChange(this, found, lost);
                }
            }
            catch (Exception e)
            {
                Log.log(this, "Could not notify " + ObjectUtils.safeToString(this.listener) + " with radar results", e);
            }
        }

        void destroy()
        {
        }

        DataRadarSpecification getDataRadarSpecification()
        {
            return this.dataRadarSpecification;
        }

        @Override
        public String toString()
        {
            return "DataRadar [" + this.dataRadarSpecification + "]";
        }
    }

    final long startTime;
    final String agentName;
    final String hostQualifiedAgentName;
    volatile String platformName;
    final ProxyContext registryProxy;
    final Lock createLock;
    /**
     * The services registered through this agent. Key=function of {serviceFamily,serviceMember}
     */
    final ConcurrentMap<String, PlatformServiceInstance> localPlatformServiceInstances;
    private final ConcurrentMap<String, PlatformServiceProxy> serviceProxies;
    private final ConcurrentMap<String, PlatformServiceProxy> serviceInstanceProxies;
    final NotifyingCache<IServiceInstanceAvailableListener, String> serviceInstanceAvailableListeners;
    final NotifyingCache<IServiceAvailableListener, String> serviceAvailableListeners;
    final NotifyingCache<IRegistryAvailableListener, String> registryAvailableListeners;
    final Set<IDataRadar> radars;
    ScheduledFuture<?> dynamicAttributeUpdateTask;
    IPlatformServiceProxy radarStationProxy;
    boolean onPlatformServiceConnectedInvoked;

    /**
     * Construct the agent connecting to the registry service on the specified host and use the
     * default registry TCP port
     * <p>
     * <b>THIS CONSTRUCTOR PROVIDES NO REGISTRY CONNECTION REDUNDANCY.</b>
     * 
     * @param agentName
     *            the name of the agent
     * 
     * @see PlatformCoreProperties#REGISTRY_PORT
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
     * @param agentName
     *            the name of the agent
     */
    public PlatformRegistryAgent(String agentName, String registryNode, int registryPort) throws IOException
    {
        this(agentName, new EndPointAddress[] { new EndPointAddress(registryNode, registryPort) });
    }

    /**
     * Construct the agent connecting to one of the available registry servers in the
     * {@link InetSocketAddress} array.
     * 
     * @param agentName
     *            the name of the agent - this must be unique across all agents on the platform
     * @param registryAddresses
     *            the addresses of registry servers to use - this provides redundancy for registry
     *            connections
     * @throws RegistryNotAvailableException
     *             if the registry is not available
     */
    public PlatformRegistryAgent(String agentName, EndPointAddress... registryAddresses)
        throws RegistryNotAvailableException
    {
        this(agentName, DataFissionProperties.Values.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS, registryAddresses);
    }

    /**
     * Construct the agent connecting to one of the available registry servers in the
     * {@link InetSocketAddress} array.
     * 
     * @param agentName
     *            the name of the agent - this must be unique across all agents on the platform
     * @param registryReconnectPeriodMillis
     *            the registry reconnection period in milliseconds
     * @param registryAddresses
     *            the addresses of registry servers to use - this provides redundancy for registry
     *            connections
     * @throws RegistryNotAvailableException
     *             if the registry is not available
     */
    @SuppressWarnings({ "unused" })
    public PlatformRegistryAgent(final String agentName, int registryReconnectPeriodMillis,
        EndPointAddress... registryAddresses) throws RegistryNotAvailableException
    {
        this.startTime = System.currentTimeMillis();
        this.agentName = agentName + "-" + new FastDateFormat().yyyyMMddHHmmssSSS(System.currentTimeMillis());
        this.hostQualifiedAgentName = PlatformUtils.composeHostQualifiedName(this.agentName);
        this.createLock = new ReentrantLock();
        this.radars = new CopyOnWriteArraySet<IDataRadar>();
        this.localPlatformServiceInstances = new ConcurrentHashMap<String, PlatformServiceInstance>();
        this.serviceProxies = new ConcurrentHashMap<String, PlatformServiceProxy>();
        this.serviceInstanceProxies = new ConcurrentHashMap<String, PlatformServiceProxy>();

        // make the actual connection to the registry
        this.registryProxy =
            new ProxyContext(PlatformUtils.composeProxyName(PlatformRegistry.SERVICE_NAME, this.agentName),
                PlatformRegistry.CODEC, TransportChannelBuilderFactoryLoader.load(
                    PlatformRegistry.CODEC.getFrameEncodingFormat(), registryAddresses));

        this.registryProxy.setReconnectPeriodMillis(registryReconnectPeriodMillis);

        this.registryAvailableListeners =
            new NotifyingCache<IRegistryAvailableListener, String>(this.registryProxy.getUtilityExecutor())
            {
                @Override
                protected void notifyListenerDataAdded(IRegistryAvailableListener listener, String key, String data)
                {
                    listener.onRegistryConnected();
                }

                @Override
                protected void notifyListenerDataRemoved(IRegistryAvailableListener listener, String key, String data)
                {
                    listener.onRegistryDisconnected();
                }
            };

        this.serviceAvailableListeners =
            PlatformUtils.createServiceAvailableNotifyingCache(this.registryProxy, IRegistryRecordNames.SERVICES, this);

        this.serviceInstanceAvailableListeners =
            PlatformUtils.createServiceInstanceAvailableNotifyingCache(this.registryProxy,
                IRegistryRecordNames.SERVICE_INSTANCES_PER_SERVICE_FAMILY, this);

        this.registryProxy.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                if (platformRegistryRpcsAvailable(imageCopy.keySet()))
                {
                    onRegistryConnected(false);
                }
            }
        }, IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);

        // listen for connection status changes in the registry service
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
                onRegistryConnected(true);
            }
        };

        this.serviceAvailableListeners.addListener(new IServiceAvailableListener()
        {
            @Override
            public void onServiceUnavailable(String serviceFamily)
            {
                // when we lose the radar station, clear scans
                if (DataRadarScanManager.RADAR_STATION.equals(serviceFamily))
                {
                    for (PlatformServiceInstance serviceInstance : PlatformRegistryAgent.this.localPlatformServiceInstances.values())
                    {
                        serviceInstance.dataRadarScanManager.clearDataRadarScans();
                    }
                }
            }

            @Override
            public void onServiceAvailable(String serviceFamily)
            {
                if (DataRadarScanManager.RADAR_STATION.equals(serviceFamily))
                {
                    registerAllDataRadars();
                }
            }
        });

        // wait for the registry name to be received...
        synchronized (this.createLock)
        {
            try
            {
                this.createLock.wait(PlatformCoreProperties.Values.PLATFORM_AGENT_INITIALISATION_TIMEOUT_MILLIS);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Interrupted whilst waiting for registry name from " + registryAddresses[0],
                    e);
            }
        }
        if (this.platformName == null)
        {
            throw new RegistryNotAvailableException("Registry name has not been received from " + registryAddresses[0]);
        }

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this));
    }

    void onRegistryConnected(boolean calledFromPlatformServiceConnectionMonitor)
    {
        this.createLock.lock();
        try
        {
            if (this.platformName == null)
            {
                if (calledFromPlatformServiceConnectionMonitor)
                {
                    this.onPlatformServiceConnectedInvoked = true;
                }
                else
                {
                    if (!this.onPlatformServiceConnectedInvoked)
                    {
                        Log.log(PlatformRegistryAgent.this, "Waiting for registry service connection...");
                        return;
                    }
                }

                // NOTE: the RPC record may be updated whilst we check it
                // remember; record access via "getRecord" is not thread safe
                final IRecord remoteRpcs = this.registryProxy.getRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
                if (remoteRpcs == null)
                {
                    Log.log(this, "No registry RPCs available");
                    return;
                }
                final HashSet<String> rpcNames = new HashSet<String>(remoteRpcs.keySet());
                if (!platformRegistryRpcsAvailable(rpcNames))
                {
                    Log.log(this, "Waiting for registry RPCs, currently have: ", ObjectUtils.safeToString(rpcNames));
                    return;
                }

                this.registryProxy.getUtilityExecutor().execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        PlatformRegistryAgent.this.createLock.lock();
                        try
                        {
                            try
                            {
                                Log.log(PlatformRegistryAgent.this, "Completing registry connection activities...");

                                PlatformRegistryAgent.this.platformName =
                                    PlatformRegistryAgent.this.registryProxy.getRpc(PlatformRegistry.GET_PLATFORM_NAME).execute().textValue();

                                // configure the channel watchdog heartbeat
                                String heartbeatConfig =
                                    PlatformRegistryAgent.this.registryProxy.getRpc(
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
                                        Log.log(PlatformRegistryAgent.this,
                                            "Could not configure heartbeat for channel watchdog", e);
                                    }
                                }

                                synchronized (PlatformRegistryAgent.this.createLock)
                                {
                                    PlatformRegistryAgent.this.createLock.notifyAll();
                                }
                                PlatformRegistryAgent.this.registryAvailableListeners.notifyListenersDataAdded(
                                    PlatformRegistryAgent.this.platformName, PlatformRegistryAgent.this.platformName);
                            }
                            catch (Exception e)
                            {
                                Log.log(PlatformRegistryAgent.this, "Could not get platform name!");
                            }
                            // (re)publish any service instances managed by
                            // this agent
                            PlatformServiceInstance platformServiceInstance = null;
                            for (Iterator<Map.Entry<String, PlatformServiceInstance>> it =
                                PlatformRegistryAgent.this.localPlatformServiceInstances.entrySet().iterator(); it.hasNext();)
                            {
                                platformServiceInstance = it.next().getValue();
                                try
                                {
                                    Log.log(PlatformRegistryAgent.this, "Registering ",
                                        ObjectUtils.safeToString(platformServiceInstance));
                                    registerService(platformServiceInstance);
                                }
                                catch (Exception e)
                                {
                                    Log.log(PlatformRegistryAgent.this,
                                        "Could not register " + ObjectUtils.safeToString(platformServiceInstance), e);
                                }
                            }

                            // reset to prepare for a disconnect-reconnect sequence
                            PlatformRegistryAgent.this.onPlatformServiceConnectedInvoked = false;
                            Log.log(PlatformRegistryAgent.this, "*** REGISTRY CONNECTED ***");

                            setupRuntimeAttributePublishing();
                        }
                        finally
                        {
                            PlatformRegistryAgent.this.createLock.unlock();
                        }
                    }
                });
            }
        }
        finally
        {
            this.createLock.unlock();
        }
    }

    void onRegistryDisconnected()
    {
        this.createLock.lock();
        try
        {
            if (this.platformName != null)
            {
                Log.log(PlatformRegistryAgent.this, "*** REGISTRY DISCONNECTED ***");
                for (String serviceFamily : this.serviceAvailableListeners.getCacheSnapshot().keySet())
                {
                    if (this.serviceAvailableListeners.notifyListenersDataRemoved(serviceFamily, serviceFamily))
                    {
                        Log.log(PlatformRegistryAgent.this, "Dropped service: '", serviceFamily, "'");
                    }
                }
                this.registryAvailableListeners.notifyListenersDataRemoved(this.platformName, this.platformName);
                this.platformName = null;
            }
        }
        finally
        {
            this.createLock.unlock();
        }
    }

    @Override
    public void waitForPlatformService(final String serviceFamily)
    {
        final CountDownLatch servicesAvailable = new CountDownLatch(1);
        final IServiceAvailableListener listener = new IServiceAvailableListener()
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
        };
        addServiceAvailableListener(listener);
        Log.log(this, "Waiting for availability of service '", serviceFamily, "' ...");
        try
        {
            try
            {
                if (!servicesAvailable.await(60, TimeUnit.SECONDS))
                {
                    throw new RuntimeException("Service '" + serviceFamily + "' is not available");
                }
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Interrupted whilst waiting for " + serviceFamily + " to be available", e);
            }
        }
        finally
        {
            removeServiceAvailableListener(listener);
        }
        Log.log(this, "Service available '",
            ObjectUtils.safeToString(this.serviceAvailableListeners.getCacheSnapshot()), "'");
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
        WireProtocolEnum wireProtocol, RedundancyModeEnum redundacyMode)
    {
        return createPlatformServiceInstance(serviceFamily, serviceMember, host,
            PlatformUtils.getNextFreeDefaultTcpServerPort(host), wireProtocol, redundacyMode);
    }

    @Override
    public boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String host, int port,
        WireProtocolEnum wireProtocol, RedundancyModeEnum redundacyMode)
    {
        return createPlatformServiceInstance(serviceFamily, serviceMember, host, port, wireProtocol, redundacyMode,
            null, null, null);
    }

    @Override
    public boolean createPlatformServiceInstance(String serviceFamily, String serviceMember, String host, int port,
        WireProtocolEnum wireProtocol, RedundancyModeEnum redundacyMode, ThimbleExecutor coreExecutor,
        ThimbleExecutor rpcExecutor, ScheduledExecutorService utilityExecutor)
    {
        this.createLock.lock();
        try
        {
            PlatformServiceInstance platformServiceInstance =
                this.localPlatformServiceInstances.get(PlatformUtils.composePlatformServiceInstanceID(serviceFamily,
                    serviceMember));
            if (platformServiceInstance != null && platformServiceInstance.isActive())
            {
                return false;
            }
            try
            {
                platformServiceInstance =
                    new PlatformServiceInstance(this.platformName, serviceFamily, serviceMember, wireProtocol,
                        redundacyMode, host, port, coreExecutor, rpcExecutor, utilityExecutor);
                registerService(platformServiceInstance);
            }
            catch (Exception e)
            {
                Log.log(PlatformRegistryAgent.this, "Could not create service " + serviceFamily + " at " + host + ":"
                    + port, e);
                return false;
            }
            this.localPlatformServiceInstances.put(
                PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember), platformServiceInstance);
            return true;
        }
        finally
        {
            this.createLock.unlock();
        }
    }

    void registerService(PlatformServiceInstance serviceInstance) throws TimeOutException, ExecutionException
    {
        this.registryProxy.getRpc(PlatformRegistry.REGISTER).execute(
            new TextValue(serviceInstance.getPlatformServiceFamily()),
            new TextValue(serviceInstance.getWireProtocol().toString()),
            new TextValue(serviceInstance.getEndPointAddress().getNode()),
            LongValue.valueOf(serviceInstance.getEndPointAddress().getPort()),
            new TextValue(serviceInstance.getPlatformServiceMemberName()),
            new TextValue(serviceInstance.getRedundancyMode().toString()), new TextValue(this.agentName));
    }

    @Override
    public boolean destroyPlatformServiceInstance(String serviceFamily, String serviceMember)
    {
        PlatformServiceInstance service =
            this.localPlatformServiceInstances.get(PlatformUtils.composePlatformServiceInstanceID(serviceFamily,
                serviceMember));
        if (service != null)
        {
            try
            {
                this.registryProxy.getRpc(PlatformRegistry.DEREGISTER).execute(new TextValue(serviceFamily),
                    new TextValue(serviceMember));
                this.localPlatformServiceInstances.remove(PlatformUtils.composePlatformServiceInstanceID(serviceFamily,
                    serviceMember));
                service.destroy();
                return true;
            }
            catch (Exception e)
            {
                Log.log(PlatformRegistryAgent.this, "Could not destroy service " + serviceFamily, e);
                return false;
            }
        }
        return false;
    }

    @Override
    public IPlatformServiceInstance getPlatformServiceInstance(String serviceFamily, String serviceMember)
    {
        return this.localPlatformServiceInstances.get(PlatformUtils.composePlatformServiceInstanceID(serviceFamily,
            serviceMember));
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
                if (!this.serviceAvailableListeners.getCacheSnapshot().keySet().contains(serviceFamily))
                {
                    Log.log(PlatformRegistryAgent.this, "No service available for ", serviceFamily, (proxy != null
                        ? " (proxy is inactive)" : ""));
                    return null;
                }

                // NOTE: we cannot go directly to a local service because the platform registry
                // decides which instance is the active one.

                Map<String, IValue> serviceInfoRecord =
                    getPlatformServiceInstanceInfoRecordImageForService(serviceFamily);
                if (serviceInfoRecord == null)
                {
                    Log.log(PlatformRegistryAgent.this, "No service info record available for ", serviceFamily);
                    return null;
                }
                final ICodec<?> codec = PlatformUtils.getCodecFromServiceInfoRecord(serviceInfoRecord);
                final String host = PlatformUtils.getHostNameFromServiceInfoRecord(serviceInfoRecord);
                final int port = PlatformUtils.getPortFromServiceInfoRecord(serviceInfoRecord);
                try
                {
                    proxy = new PlatformServiceProxy(this, serviceFamily, codec, host, port);
                    this.serviceProxies.put(serviceFamily, proxy);
                }
                catch (IOException e)
                {
                    Log.log(PlatformRegistryAgent.this, "Could not create proxy to service " + serviceFamily, e);
                }
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
                if (!this.serviceInstanceAvailableListeners.getCacheSnapshot().keySet().contains(serviceInstanceId))
                {
                    Log.log(PlatformRegistryAgent.this, "No service instance available for ", serviceInstanceId,
                        (proxy != null ? " (proxy is inactive)" : ""));
                    return null;
                }
                Map<String, IValue> serviceInfoRecord =
                    this.registryProxy.getRemoteRecordImage(ServiceInfoRecordFields.SERVICE_INFO_RECORD_NAME_PREFIX
                        + serviceInstanceId, getRegistryReconnectPeriodMillis());
                if (serviceInfoRecord == null)
                {
                    Log.log(PlatformRegistryAgent.this, "No service info record available for ", serviceInstanceId);
                    return null;
                }
                final ICodec<?> codec = PlatformUtils.getCodecFromServiceInfoRecord(serviceInfoRecord);
                final String host = PlatformUtils.getHostNameFromServiceInfoRecord(serviceInfoRecord);
                final int port = PlatformUtils.getPortFromServiceInfoRecord(serviceInfoRecord);
                try
                {
                    proxy = new PlatformServiceProxy(this, serviceInstanceId, codec, host, port);
                    proxy.proxyContext.setTransportChannelBuilderFactory(TransportChannelBuilderFactoryLoader.load(
                        PlatformRegistry.CODEC.getFrameEncodingFormat(), new IEndPointAddressFactory()
                        {
                            @Override
                            public EndPointAddress next()
                            {
                                Log.log(this, "Obtaining service info record for '", serviceInstanceId, "'");
                                Map<String, IValue> serviceInfoRecord =
                                    PlatformRegistryAgent.this.registryProxy.getRemoteRecordImage(
                                        ServiceInfoRecordFields.SERVICE_INFO_RECORD_NAME_PREFIX + serviceInstanceId,
                                        getRegistryReconnectPeriodMillis());
                                if (serviceInfoRecord == null)
                                {
                                    Log.log(this, "No service info record found for '", serviceInstanceId, "'");
                                    return null;
                                }
                                final String node = PlatformUtils.getHostNameFromServiceInfoRecord(serviceInfoRecord);
                                final int port = PlatformUtils.getPortFromServiceInfoRecord(serviceInfoRecord);
                                final EndPointAddress endPointAddress = new EndPointAddress(node, port);
                                Log.log(this, "Service instance '" + serviceInstanceId, "' ",
                                    ObjectUtils.safeToString(endPointAddress));
                                return endPointAddress;
                            }
                        }));
                    this.serviceInstanceProxies.put(serviceInstanceId, proxy);
                }
                catch (IOException e)
                {
                    Log.log(PlatformRegistryAgent.this, "Could not create proxy to service instance "
                        + serviceInstanceId, e);
                }
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
            PlatformServiceProxy proxy = proxies.remove(serviceFamily);
            if (proxy == null)
            {
                return false;
            }
            proxy.destroy();
            return true;
        }
        finally
        {
            this.createLock.unlock();
        }
    }

    @Override
    public void destroy()
    {
        this.createLock.lock();
        try
        {
            Log.log(this, "Destroying ", ObjectUtils.safeToString(this));
            if (this.dynamicAttributeUpdateTask != null)
            {
                this.dynamicAttributeUpdateTask.cancel(false);
            }
            for (IDataRadar dataRadar : new HashSet<IDataRadar>(this.radars))
            {
                try
                {
                    deleteDataRadar(dataRadar);
                }
                catch (Exception e)
                {
                    Log.log(PlatformRegistryAgent.this, "Could not destroy " + ObjectUtils.safeToString(dataRadar), e);
                }
            }
            try
            {
                this.registryProxy.destroy();
            }
            catch (Exception e)
            {
                Log.log(PlatformRegistryAgent.this,
                    "Could not destroy " + ObjectUtils.safeToString(this.registryProxy), e);
            }

            for (PlatformServiceInstance service : this.localPlatformServiceInstances.values())
            {
                try
                {
                    service.destroy();
                }
                catch (Exception e)
                {
                    Log.log(PlatformRegistryAgent.this, "Could not destroy " + ObjectUtils.safeToString(service), e);
                }
            }
            for (PlatformServiceProxy service : this.serviceProxies.values())
            {
                try
                {
                    service.destroy();
                }
                catch (Exception e)
                {
                    Log.log(PlatformRegistryAgent.this, "Could not destroy " + ObjectUtils.safeToString(service), e);
                }
            }
            for (PlatformServiceProxy service : this.serviceInstanceProxies.values())
            {
                try
                {
                    service.destroy();
                }
                catch (Exception e)
                {
                    Log.log(PlatformRegistryAgent.this, "Could not destroy " + ObjectUtils.safeToString(service), e);
                }
            }

            this.serviceAvailableListeners.destroy();
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
     * @param serviceFamily
     *            the service name for the service info record to get
     * @return the service record image, <code>null</code> if the record could not be obtained
     *         (either it doesn't exist or a network problem occurred)
     */
    IRecord getPlatformServiceInstanceInfoRecordImageForService(String serviceFamily)
    {
        try
        {
            String instanceForService =
                this.registryProxy.getRpc(PlatformRegistry.GET_SERVICE_INFO_RECORD_NAME_FOR_SERVICE).execute(
                    new TextValue(serviceFamily)).textValue();
            return this.registryProxy.getRemoteRecordImage(instanceForService, getRegistryReconnectPeriodMillis());
        }
        catch (Exception e)
        {
            Log.log(this, "Could not get service instance to use for service '" + serviceFamily + "'", e);
            return null;
        }
    }

    @Override
    public String getPlatformName()
    {
        return this.platformName;
    }

    @Override
    public String toString()
    {
        return "PlatformRegistryAgent [" + this.platformName + "] " + this.registryProxy.getChannelString();
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
    public IDataRadar registerDataRadar(String name, IExpression dataRadarSignatureExpression,
        IDataRadarListener listener) throws TimeOutException, ExecutionException
    {
        if (this.radarStationProxy == null)
        {
            this.radarStationProxy = getPlatformServiceProxy(DataRadarScanManager.RADAR_STATION);
            if (this.radarStationProxy == null)
            {
                throw new RuntimeException("No radar station exists on the platform");
            }
        }

        final DataRadarSpecification dataRadarSpecification =
            new DataRadarSpecification(name, dataRadarSignatureExpression);

        final DataRadar radar = new DataRadar(dataRadarSpecification, listener);

        this.radarStationProxy.addRecordListener(radar, dataRadarSpecification.getName());
        this.radars.add(radar);

        PlatformUtils.executeRpc(this.radarStationProxy, getRegistryReconnectPeriodMillis(),
            DataRadarScanManager.RPC_REGISTER_DATA_RADAR, TextValue.valueOf(dataRadarSpecification.toWireString()));

        return radar;
    }

    @Override
    public void deleteDataRadar(IDataRadar dataRadar) throws TimeOutException, ExecutionException
    {
        if (this.radars.remove(dataRadar))
        {
            final DataRadar dataRadarImpl = (DataRadar) dataRadar;

            dataRadarImpl.destroy();

            PlatformUtils.executeRpc(this.radarStationProxy, getRegistryReconnectPeriodMillis(),
                DataRadarScanManager.RPC_DEREGISTER_DATA_RADAR,
                TextValue.valueOf(dataRadarImpl.getDataRadarSpecification().toWireString()));
        }

    }

    void registerAllDataRadars()
    {
        if (this.radarStationProxy == null)
        {
            this.radarStationProxy = getPlatformServiceProxy(DataRadarScanManager.RADAR_STATION);
            if (this.radarStationProxy == null)
            {
                throw new RuntimeException("No radar station exists on the platform");
            }
        }
        if (this.radars.size() > 0)
        {
            Log.log(this, "Registering all data radars");
            for (IDataRadar radar : this.radars)
            {
                this.radarStationProxy.addRecordListener(((DataRadar) radar),
                    ((DataRadar) radar).dataRadarSpecification.getName());
                try
                {
                    PlatformUtils.executeRpc(this.radarStationProxy, getRegistryReconnectPeriodMillis(),
                        DataRadarScanManager.RPC_REGISTER_DATA_RADAR,
                        TextValue.valueOf(((DataRadar) radar).dataRadarSpecification.toWireString()));
                }
                catch (Exception e)
                {
                    Log.log(this, "Could not register " + radar, e);
                }
            }
        }
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
        this.registryProxy.getUtilityExecutor().execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    final IRpcInstance rpc =
                        ContextUtils.getRpc(PlatformRegistryAgent.this.registryProxy,
                            PlatformRegistryAgent.this.registryProxy.getReconnectPeriodMillis(),
                            PlatformRegistry.RUNTIME_STATIC);
                    try
                    {
                        final String runtimeDescription =
                            System.getProperty("os.name") + " (" + System.getProperty("os.version") + "), "
                                + System.getProperty("os.arch") + ", Java " + System.getProperty("java.version");
                        final String host = TcpChannelUtils.LOCALHOST_IP;
                        final Runtime runtime = Runtime.getRuntime();
                        final long cpuCount = runtime.availableProcessors();
                        final String user = System.getProperty("user.name");

                        rpc.executeNoResponse(TextValue.valueOf(PlatformRegistryAgent.this.agentName),
                            TextValue.valueOf(host), TextValue.valueOf(runtimeDescription), TextValue.valueOf(user),
                            LongValue.valueOf(cpuCount));
                    }
                    catch (Exception e)
                    {
                        Log.log(PlatformRegistryAgent.this, "Could not invoke " + PlatformRegistry.RUNTIME_STATIC, e);
                    }
                }
                catch (TimeOutException e1)
                {
                    Log.log(PlatformRegistryAgent.this, "RPC not available from platform registry: "
                        + PlatformRegistry.RUNTIME_STATIC, e1);
                }
            }
        });

        // tell the registry about the runtime dynamic attributes (periodic call)
        if (this.dynamicAttributeUpdateTask == null)
        {
            this.dynamicAttributeUpdateTask =
                this.registryProxy.getUtilityExecutor().scheduleWithFixedDelay(
                    new Runnable()
                    {
                        final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
                        long executedFromLastPeriod;
                        long gcTimeLastPeriod;

                        @Override
                        public void run()
                        {
                            try
                            {
                                final IRpcInstance rpc =
                                    ContextUtils.getRpc(PlatformRegistryAgent.this.registryProxy,
                                        PlatformRegistryAgent.this.registryProxy.getReconnectPeriodMillis(),
                                        PlatformRegistry.RUNTIME_DYNAMIC);

                                final long[] stats = ContextUtils.getCoreStats();
                                final long qOverflow = stats[0];
                                final long qTotalSubmitted = stats[1];
                                final Runtime runtime = Runtime.getRuntime();
                                final double MB = 1d / (1024 * 1024);
                                final long memUsed = (long) ((runtime.totalMemory() - runtime.freeMemory()) * MB);
                                final long memAvailable = (long) (runtime.freeMemory() * MB);
                                final long threadCount = this.threadMxBean.getThreadCount();

                                long gcMillisPerMin = 0;
                                long time = 0;
                                for (GarbageCollectorMXBean gcMxBean : ManagementFactory.getGarbageCollectorMXBeans())
                                {
                                    time = gcMxBean.getCollectionTime();
                                    if (time > -1)
                                    {
                                        gcMillisPerMin += time;
                                    }
                                }
                                // store and work out delta of gc times
                                time = this.gcTimeLastPeriod;
                                this.gcTimeLastPeriod = gcMillisPerMin;
                                gcMillisPerMin -= time;
                                final double perMin = 60d / DataFissionProperties.Values.STATS_LOGGING_PERIOD_SECS;
                                gcMillisPerMin *= perMin;
                                // this is now the "% GC duty cycle per minute"
                                gcMillisPerMin = (long) ((double) gcMillisPerMin / 600);

                                final long qTotalExecuted = stats[2];
                                final long eventsPerMin =
                                    (long) ((qTotalExecuted - this.executedFromLastPeriod) * perMin);
                                final long uptime =
                                    (System.currentTimeMillis() - PlatformRegistryAgent.this.startTime) / 1000;
                                this.executedFromLastPeriod = qTotalExecuted;
                                try
                                {
                                    rpc.executeNoResponse(TextValue.valueOf(PlatformRegistryAgent.this.agentName),
                                        LongValue.valueOf(qOverflow), LongValue.valueOf(qTotalSubmitted),
                                        LongValue.valueOf(memUsed), LongValue.valueOf(memAvailable),
                                        LongValue.valueOf(threadCount), LongValue.valueOf(gcMillisPerMin),
                                        LongValue.valueOf(eventsPerMin), LongValue.valueOf(uptime));
                                }
                                catch (Exception e)
                                {
                                    Log.log(PlatformRegistryAgent.this, "Could not invoke " + rpc, e);
                                }
                            }
                            catch (TimeOutException e1)
                            {
                                Log.log(PlatformRegistryAgent.this, "RPC not available from platform registry: "
                                    + PlatformRegistry.RUNTIME_DYNAMIC, e1);
                            }

                        }
                    }, DataFissionProperties.Values.STATS_LOGGING_PERIOD_SECS,
                    DataFissionProperties.Values.STATS_LOGGING_PERIOD_SECS, TimeUnit.SECONDS);
        }
    }

    @Override
    public ScheduledExecutorService getUtilityExecutor()
    {
        return this.registryProxy.getUtilityExecutor();
    }

    @Override
    public Map<String, IPlatformServiceProxy> getActiveProxies()
    {
        return new HashMap<String, IPlatformServiceProxy>(this.serviceProxies);
    }
}
