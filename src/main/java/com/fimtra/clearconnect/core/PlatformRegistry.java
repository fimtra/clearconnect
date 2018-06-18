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

import static com.fimtra.datafission.core.ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS;
import static com.fimtra.datafission.core.ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_RECORDS;
import static com.fimtra.datafission.core.ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.core.PlatformRegistry.IPlatformSummaryRecordFields;
import com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames;
import com.fimtra.clearconnect.core.PlatformRegistry.IRuntimeStatusRecordFields;
import com.fimtra.clearconnect.core.PlatformRegistry.IServiceRecordFields;
import com.fimtra.clearconnect.core.PlatformServiceInstance.IServiceStatsRecordFields;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.AtomicChange;
import com.fimtra.datafission.core.CoalescingRecordListener;
import com.fimtra.datafission.core.CoalescingRecordListener.CachePolicyEnum;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.GZipProtocolCodec;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.core.Publisher;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.datafission.core.StringProtocolCodec;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.thimble.ICoalescingRunnable;
import com.fimtra.thimble.ISequentialRunnable;
import com.fimtra.thimble.ThimbleExecutor;
import com.fimtra.util.FastDateFormat;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.RollingFileAppender;
import com.fimtra.util.SystemUtils;
import com.fimtra.util.ThreadUtils;
import com.fimtra.util.UtilProperties;
import com.fimtra.util.is;

/**
 * A service that maintains a registry of all other services in the platform. This should only be
 * accessed via a {@link PlatformRegistryAgent}.
 * <p>
 * The registry maintains a proxy to every service that is registered with it - this allows the
 * registry to keep a live view of what services are available.
 * <p>
 * All information in the registry is maintained in internal "registry records" that are published
 * at timed intervals when there is a change. The timed publishing is an efficiency feature to group
 * as many changes to each record together in a single update message.
 * <p>
 * The registry service uses a GZIP wire-protocol
 * 
 * @see IPlatformRegistryAgent
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public final class PlatformRegistry
{
    static final IValue BLANK_VALUE = TextValue.valueOf("");
    static final String GET_SERVICE_INFO_RECORD_NAME_FOR_SERVICE = "getServiceInfoForService";
    static final String GET_HEARTBEAT_CONFIG = "getHeartbeatConfig";
    static final String GET_PLATFORM_NAME = "getPlatformName";
    static final String DEREGISTER = "deregister";
    static final String REGISTER = "register";
    static final String RUNTIME_STATIC = "runtimeStatic";
    static final String RUNTIME_DYNAMIC = "runtimeDynamic";
    static final String AGENT_PROXY_ID_PREFIX = PlatformRegistry.SERVICE_NAME + PlatformUtils.SERVICE_CLIENT_DELIMITER;
    static final int AGENT_PROXY_ID_PREFIX_LEN = AGENT_PROXY_ID_PREFIX.length();

    /**
     * Access for starting a {@link PlatformRegistry} using command line.
     * 
     * @param args
     *            - the parameters used to start the {@link PlatformRegistry}.
     * 
     *            <pre>
     *  arg[0] is the platform name (mandatory)
     *  arg[1] is the host (mandatory)
     *  arg[2] is the port (optional)
     *            </pre>
     * 
     * @throws InterruptedException
     */
    @SuppressWarnings("unused")
    public static void main(String[] args) throws InterruptedException
    {
        try
        {
            switch(args.length)
            {
                case 2:
                    new PlatformRegistry(args[0], args[1]);
                    break;
                case 3:
                    new PlatformRegistry(args[0], args[1], Integer.parseInt(args[2]));
                    break;
                default :
                    throw new IllegalArgumentException("Incorrect number of arguments.");
            }
        }
        catch (RuntimeException e)
        {
            throw new RuntimeException(SystemUtils.lineSeparator() + "Usage: " + PlatformRegistry.class.getSimpleName()
                + " platformName hostName [tcpPort]" + SystemUtils.lineSeparator() + "    platformName is mandatory"
                + SystemUtils.lineSeparator() + "    hostName is mandatory and is either the hostname or IP address"
                + SystemUtils.lineSeparator() + "    tcpPort is optional", e);
        }
        synchronized (args)
        {
            args.wait();
        }
    }

    static final StringProtocolCodec CODEC = new GZipProtocolCodec();

    /**
     * Describes the structure of a service info record and provides the prefix for the canonical
     * name of all service info records
     * 
     * @author Ramon Servadei
     */
    static interface ServiceInfoRecordFields
    {
        String PORT_FIELD = "PORT";
        String HOST_NAME_FIELD = "HOST_NAME";
        String WIRE_PROTOCOL_FIELD = "WIRE_PROTOCOL";
        String REDUNDANCY_MODE_FIELD = "REDUNDANCY_MODE";
        String TRANSPORT_TECHNOLOGY_FIELD = "TRANSPORT_TECHNOLOGY";
        /** The prefix for the record that holds the service info for a service instance */
        String SERVICE_INFO_RECORD_NAME_PREFIX = "ServiceInfo:";
    }

    static interface IRuntimeStatusRecordFields
    {
        String RUNTIME_NAME = "Agent";
        String RUNTIME_HOST = "Host";
        String Q_OVERFLOW = "QOverflow";
        String Q_TOTAL_SUBMITTED = "QTotalSubmitted";
        String CPU_COUNT = "CPUcount";
        String MEM_USED_MB = "MemUsedMb";
        String MEM_AVAILABLE_MB = "MemAvailableMb";
        String THREAD_COUNT = "ThreadCount";
        String SYSTEM_LOAD = "SystemLoad";
        String RUNTIME = "Runtime";
        String USER = "User";
        String EPS = "EPM";
        String UPTIME_SECS = "Uptime";
    }

    static interface IPlatformSummaryRecordFields
    {
        String VERSION = "Version";
        String NODES = "Nodes";
        String SERVICES = "Services";
        String SERVICE_INSTANCES = "ServiceInstances";
        String CONNECTIONS = "Connections";
        String UPTIME = "Uptime";
        String AGENTS = "Agents";
    }

    static interface IServiceRecordFields
    {
        String RECORD_COUNT = "RecordCount";
        String RPC_COUNT = "RpcCount";
        String SERVICE_INSTANCE_COUNT = "ServiceInstancesCount";
        String MSGS_PER_SEC = "Msgs per sec";
        String KB_PER_SEC = "Kb per sec";
        String MESSAGE_COUNT = "Msgs published";
        String KB_COUNT = "Kb published";
        String SUBSCRIPTION_COUNT = "Subscriptions";
        String TX_QUEUE_SIZE = "TxQueueSize";
    }

    public static final String SERVICE_NAME = "PlatformRegistry";

    /**
     * Exposes the record names of internal records used in a registry service.
     * 
     * @author Ramon Servadei
     */
    static interface IRegistryRecordNames
    {
        /** Prefix for the records-per-service-family record */
        String PREFIX_RECORDS_PER_SERVICE = "Service_Records_";
        
        /** Prefix for the rpcs-per-service-family record */
        String PREFIX_RPCS_PER_SERVICE = "ServiceRpcs:";
        
        /** Prefix for the records-per-service-instance record */
        String PREFIX_RECORDS_PER_INSTANCE = "InstanceRecords:";
        
        /** Prefix for the rpcs-per-service-instance record */
        String PREFIX_RPCS_PER_INSTANCE = "InstanceRpcs:";
        
        /**
         * <pre>
         * key: serviceFamily, value: redundancy mode of the service (string value of {@link RedundancyModeEnum})
         * </pre>
         * 
         * Agents subscribe for this to have a live view of the services that exist, each service
         * identified by its service family name
         * 
         * @see IServiceRecordFields
         */
        String SERVICES = "Services";

        /**
         * The statistics per logical service family existing on the platform.
         * 
         * <pre>
         * sub-map key: serviceFamily
         * sub-map structure: {key=one of the fields in {@link IServiceRecordFields}, value=attribute value}
         * </pre>
         * 
         * @see IServiceRecordFields
         */
        String SERVICE_STATS = "Service Statistics";

        /**
         * The service instances per service family on the platform
         * 
         * <pre>
         * sub-map key: serviceFamily
         * sub-map structure: {key=service member name (NOT the service instance ID), value=sequence number when registered/last used}
         * </pre>
         */
        String SERVICE_INSTANCES_PER_SERVICE_FAMILY = "Service Instances Per Service Family";

        /**
         * The service instances created per agent on the platform
         * 
         * <pre>
         * sub-map key: agent name
         * sub-map structure: {key=service instance ID, value=blank}
         * </pre>
         */
        String SERVICE_INSTANCES_PER_AGENT = "Service Instances Per Agent";

        /**
         * The statistics per service instance created on the platform
         * 
         * <pre>
         * sub-map key: serviceInstanceID
         * sub-map structure: {key=one of the fields in {@link IServiceRecordFields}, value=attribute value}
         * </pre>
         * 
         * @see IServiceStatsRecordFields
         */
        String SERVICE_INSTANCE_STATS = "Service Instance Statistics";

        /**
         * All service instance connections. Connections are held as sub-maps.
         * 
         * <pre>
         * sub-map key: connection description (e.g. /127.0.0.1:31467->127.0.0.1:50132)
         * sub-map structure: {connection information, see {@link IContextConnectionsRecordFields} for the definition of the sub-map fields}
         * </pre>
         */
        String PLATFORM_CONNECTIONS = "Platform Connections";

        /**
         * The status of the runtime of each agent on the platform
         * 
         * <pre>
         * sub-map key: runtime (agent) name
         * sub-map structure: {key={one of the {@link IRuntimeStatusRecordFields}}, value={the value for the field}}
         * </pre>
         * 
         * @see IRuntimeStatusRecordFields
         */
        String RUNTIME_STATUS = "Runtime Status";

        /**
         * A summary of the hosts, services and connections on the platform
         * 
         * @see IPlatformSummaryRecordFields
         */
        String PLATFORM_SUMMARY = "Platform Summary";
    }

    final Context context;
    final String platformName;
    final Publisher publisher;
    int reconnectPeriodMillis = DataFissionProperties.Values.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS;
    /** @see IRegistryRecordNames#SERVICES */
    final IRecord services;
    /** @see IRegistryRecordNames#SERVICE_STATS */
    final IRecord serviceStats;
    /** @see IRegistryRecordNames#SERVICE_INSTANCES_PER_SERVICE_FAMILY */
    final IRecord serviceInstancesPerServiceFamily;
    /** @See {@link IRegistryRecordNames#SERVICE_INSTANCES_PER_AGENT */
    final IRecord serviceInstancesPerAgent;
    /** @See {@link IRegistryRecordNames#SERVICE_INSTANCE_STATS */
    final IRecord serviceInstanceStats;
    /** @see IRegistryRecordNames#PLATFORM_CONNECTIONS */
    final IRecord platformConnections;
    /** @see IRegistryRecordNames#RUNTIME_STATUS */
    final IRecord runtimeStatus;
    /** @see IRegistryRecordNames#PLATFORM_SUMMARY */
    final IRecord platformSummary;

    final ThimbleExecutor coalescingExecutor;
    final EventHandler eventHandler;
    final AtomicLong serviceSequence;
    final AtomicLong registrationTokenCounter;
    
    /**
     * Construct the platform registry using the default platform registry port.
     * 
     * @see #PlatformRegistry(String, String, int)
     * @see PlatformCoreProperties#REGISTRY_PORT
     * @param platformName
     *            the platform name
     * @param node
     *            the hostname or IP address to use when creating the end-point for the channel
     *            server
     */
    public PlatformRegistry(String platformName, String node)
    {
        this(platformName, node, PlatformCoreProperties.Values.REGISTRY_PORT);
    }

    /**
     * Construct the platform registry using the socket address to bind to.
     * 
     * @see #PlatformRegistry(String, String, int)
     * @param platformName
     *            the platform name
     * @param registryEndPoint
     *            the registry end-point to use
     */
    public PlatformRegistry(String platformName, EndPointAddress registryEndPoint)
    {
        this(platformName, registryEndPoint.getNode(), registryEndPoint.getPort());
    }

    /**
     * Construct the platform registry using the specified host and port.
     * 
     * @see #PlatformRegistry(String, String)
     * @param platformName
     *            the platform name
     * @param host
     *            the hostname or IP address to use when creating the TCP server socket
     * @param port
     *            the TCP port to use for the server socket
     */
    public PlatformRegistry(String platformName, String host, int port)
    {
        final String registryInstanceId = platformName + "@" + host + ":" + port;
        Log.log(this, "Creating ", registryInstanceId);
        this.serviceSequence = new AtomicLong(0);
        this.registrationTokenCounter = new AtomicLong();

        this.platformName = platformName;
        this.context = new Context(PlatformUtils.composeHostQualifiedName(SERVICE_NAME + "[" + platformName + "]"));
        this.publisher = new Publisher(this.context, CODEC, host, port);

        this.coalescingExecutor = new ThimbleExecutor("registry-status", 1);
        this.eventHandler = new EventHandler(this);

        this.services = this.context.createRecord(IRegistryRecordNames.SERVICES);
        this.serviceStats = this.context.createRecord(IRegistryRecordNames.SERVICE_STATS);
        this.serviceInstancesPerServiceFamily =
            this.context.createRecord(IRegistryRecordNames.SERVICE_INSTANCES_PER_SERVICE_FAMILY);
        this.serviceInstancesPerAgent = this.context.createRecord(IRegistryRecordNames.SERVICE_INSTANCES_PER_AGENT);
        this.serviceInstanceStats = this.context.createRecord(IRegistryRecordNames.SERVICE_INSTANCE_STATS);
        this.platformConnections = this.context.createRecord(IRegistryRecordNames.PLATFORM_CONNECTIONS);
        this.runtimeStatus = this.context.createRecord(IRegistryRecordNames.RUNTIME_STATUS);
        this.platformSummary = this.context.createRecord(IRegistryRecordNames.PLATFORM_SUMMARY);

        this.platformSummary.put(IPlatformSummaryRecordFields.VERSION, TextValue.valueOf(PlatformUtils.VERSION));

        // register the RegistryService as a service!
        this.services.put(SERVICE_NAME, RedundancyModeEnum.FAULT_TOLERANT.toString());
        this.serviceInstancesPerServiceFamily.getOrCreateSubMap(SERVICE_NAME).put(platformName,
            LongValue.valueOf(nextSequence()));
        this.context.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
            {
                PlatformRegistry.this.eventHandler.executeHandleRegistryRecordsUpdate(atomicChange);
            }
        }, ISystemRecordNames.CONTEXT_RECORDS);

        this.context.publishAtomicChange(this.services);

        // log when platform summary changes occur
        this.context.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, final IRecordChange atomicChange)
            {
                Log.log(PlatformRegistry.this, imageCopy.toString());
            }
        }, IRegistryRecordNames.PLATFORM_SUMMARY);

        // handle real-time updates for the platform summary
        final IRecordListener platformSummaryListener = new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, final IRecordChange atomicChange)
            {
                PlatformRegistry.this.coalescingExecutor.execute(new ICoalescingRunnable()
                {
                    @Override
                    public void run()
                    {
                        PlatformRegistry.this.eventHandler.executeComputePlatformSummary();
                    }

                    @Override
                    public Object context()
                    {
                        return IRegistryRecordNames.PLATFORM_SUMMARY;
                    }
                });
            }
        };
        this.context.addObserver(platformSummaryListener, IRegistryRecordNames.RUNTIME_STATUS);
        this.context.addObserver(platformSummaryListener, IRegistryRecordNames.SERVICES);
        this.context.addObserver(platformSummaryListener, IRegistryRecordNames.SERVICE_INSTANCES_PER_SERVICE_FAMILY);
        this.context.addObserver(platformSummaryListener, IRegistryRecordNames.PLATFORM_CONNECTIONS);

        // the registry's connections
        this.context.addObserver(new CoalescingRecordListener(this.coalescingExecutor, new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, final IRecordChange atomicChange)
            {
                PlatformRegistry.this.eventHandler.executeHandleRegistryConnectionsUpdate(atomicChange);
            }
        }, ISystemRecordNames.CONTEXT_CONNECTIONS, CachePolicyEnum.NO_IMAGE_NEEDED),
            ISystemRecordNames.CONTEXT_CONNECTIONS);

        createGetServiceInfoRecordNameForServiceRpc();
        createGetHeartbeatConfigRpc();
        createGetPlatformNameRpc();
        createRegisterRpc();
        createDeregisterRpc();
        createRuntimeStaticRpc();
        createRuntimeDynamicRpc();

        final Map<String, IValue> registryServiceSubMap = this.serviceStats.getOrCreateSubMap(SERVICE_NAME);
        registryServiceSubMap.put(IServiceRecordFields.RPC_COUNT,
            LongValue.valueOf(this.context.getRecord(ISystemRecordNames.CONTEXT_RPCS).size()));
        registryServiceSubMap.put(IServiceRecordFields.SERVICE_INSTANCE_COUNT, LongValue.valueOf(1));

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this));
    }

    long nextSequence()
    {
        return this.serviceSequence.incrementAndGet();
    }

    private void createGetServiceInfoRecordNameForServiceRpc()
    {
        final RpcInstance getServiceInfoRecordNameForServiceRpc =
            new RpcInstance(TypeEnum.TEXT, GET_SERVICE_INFO_RECORD_NAME_FOR_SERVICE, TypeEnum.TEXT);
        getServiceInfoRecordNameForServiceRpc.setHandler(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(final IValue... args) throws TimeOutException, ExecutionException
            {
                try
                {
                    final String nextInstance =
                        PlatformRegistry.this.eventHandler.executeSelectNextInstance(args[0].textValue()).get();

                    if (nextInstance == null)
                    {
                        return null;
                    }
                    else
                    {
                        return TextValue.valueOf(
                            ServiceInfoRecordFields.SERVICE_INFO_RECORD_NAME_PREFIX + nextInstance);
                    }
                }
                catch (Exception e)
                {
                    throw new ExecutionException(e);
                }
            }
        });
        this.context.createRpc(getServiceInfoRecordNameForServiceRpc);
    }

    private void createGetPlatformNameRpc()
    {
        final RpcInstance getPlatformName = new RpcInstance(TypeEnum.TEXT, GET_PLATFORM_NAME);
        getPlatformName.setHandler(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                return TextValue.valueOf(PlatformRegistry.this.platformName);
            }
        });
        this.context.createRpc(getPlatformName);
    }

    private void createGetHeartbeatConfigRpc()
    {
        final RpcInstance getPlatformName = new RpcInstance(TypeEnum.TEXT, GET_HEARTBEAT_CONFIG);
        getPlatformName.setHandler(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                return TextValue.valueOf(ChannelUtils.WATCHDOG.getHeartbeatPeriodMillis() + ":"
                    + ChannelUtils.WATCHDOG.getMissedHeartbeatCount());
            }
        });
        this.context.createRpc(getPlatformName);
    }

    private void createRegisterRpc()
    {
        // publish an RPC that allows registration
        // args: serviceFamily, objectWireProtocol, hostname, port, serviceMember,
        // redundancyMode, agentName, TransportTechnologyEnum
        final RpcInstance register = new RpcInstance(TypeEnum.TEXT, REGISTER, TypeEnum.TEXT, TypeEnum.TEXT,
            TypeEnum.TEXT, TypeEnum.LONG, TypeEnum.TEXT, TypeEnum.TEXT, TypeEnum.TEXT, TypeEnum.TEXT);
        register.setHandler(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(final IValue... args) throws TimeOutException, ExecutionException
            {
                int i = 0;
                final String serviceFamily = args[i++].textValue();
                final String wireProtocol = args[i++].textValue();
                final String host = args[i++].textValue();
                final int port = (int) args[i++].longValue();
                final String serviceMember = args[i++].textValue();
                final String redundancyMode = args[i++].textValue();
                final String agentName = args[i++].textValue();
                final String tte = args[i++].textValue();

                if (serviceFamily.startsWith(PlatformRegistry.SERVICE_NAME))
                {
                    throw new ExecutionException("Cannot create service with reserved name '" + SERVICE_NAME + "'");
                }
                if (serviceMember.startsWith(PlatformRegistry.SERVICE_NAME))
                {
                    throw new ExecutionException(
                        "Cannot create service instance with reserved name '" + SERVICE_NAME + "'");
                }

                final String serviceInstanceId =
                    PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember);
                final RedundancyModeEnum redundancyModeEnum = RedundancyModeEnum.valueOf(redundancyMode);
                final Map<String, IValue> serviceRecordStructure = new HashMap<String, IValue>();
                serviceRecordStructure.put(ServiceInfoRecordFields.WIRE_PROTOCOL_FIELD,
                    TextValue.valueOf(wireProtocol));
                serviceRecordStructure.put(ServiceInfoRecordFields.HOST_NAME_FIELD, TextValue.valueOf(host));
                serviceRecordStructure.put(ServiceInfoRecordFields.PORT_FIELD, LongValue.valueOf(port));
                serviceRecordStructure.put(ServiceInfoRecordFields.REDUNDANCY_MODE_FIELD,
                    TextValue.valueOf(redundancyMode));
                serviceRecordStructure.put(ServiceInfoRecordFields.TRANSPORT_TECHNOLOGY_FIELD, TextValue.valueOf(tte));

                try
                {
                    PlatformRegistry.this.eventHandler.executeRegisterServiceInstance(
                        new RegistrationToken(
                            "token#" + PlatformRegistry.this.registrationTokenCounter.incrementAndGet(),
                            serviceInstanceId),
                        serviceFamily, agentName,
                        serviceInstanceId, redundancyModeEnum, TransportTechnologyEnum.valueOf(tte),
                        serviceRecordStructure, args);
                }
                catch (Exception e)
                {
                    throw new ExecutionException(e);
                }

                return TextValue.valueOf("Registered " + serviceInstanceId);
            }
        });
        this.context.createRpc(register);
    }

    private void createDeregisterRpc()
    {
        final RpcInstance deregister = new RpcInstance(TypeEnum.TEXT, DEREGISTER, TypeEnum.TEXT, TypeEnum.TEXT);
        deregister.setHandler(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                int i = 0;
                final String serviceFamily = args[i++].textValue();
                final String serviceMember = args[i++].textValue();
                final String serviceInstanceId =
                    PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember);
                try
                {
                    PlatformRegistry.this.eventHandler.executeDeregisterPlatformServiceInstance(null, serviceFamily,
                        serviceInstanceId, "RPC call");
                }
                catch (Exception e)
                {
                    throw new ExecutionException(e);
                }

                return TextValue.valueOf("Deregistered " + serviceInstanceId);
            }
        });
        this.context.createRpc(deregister);
    }

    private void createRuntimeStaticRpc()
    {
        final String[] fields = new String[] { IRuntimeStatusRecordFields.RUNTIME_NAME,
            IRuntimeStatusRecordFields.RUNTIME_HOST, IRuntimeStatusRecordFields.RUNTIME,
            IRuntimeStatusRecordFields.USER, IRuntimeStatusRecordFields.CPU_COUNT };

        final RpcInstance runtimeStatus = new RpcInstance(TypeEnum.TEXT, RUNTIME_STATIC, fields, TypeEnum.TEXT,
            TypeEnum.TEXT, TypeEnum.TEXT, TypeEnum.TEXT, TypeEnum.LONG);

        runtimeStatus.setHandler(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(final IValue... args) throws TimeOutException, ExecutionException
            {
                PlatformRegistry.this.eventHandler.executeRpcRuntimeStatic(args);
                return PlatformUtils.OK;
            }
        });
        this.context.createRpc(runtimeStatus);
    }

    private void createRuntimeDynamicRpc()
    {
        final String[] fields = new String[] { IRuntimeStatusRecordFields.RUNTIME_NAME,
            IRuntimeStatusRecordFields.Q_OVERFLOW, IRuntimeStatusRecordFields.Q_TOTAL_SUBMITTED,
            IRuntimeStatusRecordFields.MEM_USED_MB, IRuntimeStatusRecordFields.MEM_AVAILABLE_MB,
            IRuntimeStatusRecordFields.THREAD_COUNT, IRuntimeStatusRecordFields.SYSTEM_LOAD,
            IRuntimeStatusRecordFields.EPS, IRuntimeStatusRecordFields.UPTIME_SECS };

        final RpcInstance runtimeStatus =
            new RpcInstance(TypeEnum.TEXT, RUNTIME_DYNAMIC, fields, TypeEnum.TEXT, TypeEnum.LONG, TypeEnum.LONG,
                TypeEnum.LONG, TypeEnum.LONG, TypeEnum.LONG, TypeEnum.LONG, TypeEnum.LONG, TypeEnum.LONG);

        runtimeStatus.setHandler(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(final IValue... args) throws TimeOutException, ExecutionException
            {
                PlatformRegistry.this.eventHandler.executeRpcRuntimeDynamic(args);
                return PlatformUtils.OK;
            }
        });
        this.context.createRpc(runtimeStatus);
    }

    public void destroy()
    {
        Log.log(this, "Destroying ", ObjectUtils.safeToString(this));
        this.publisher.destroy();
        this.context.destroy();

        // NOTE: destroy event handler AFTER destroying the publisher and context - we don't want to
        // tell the agents that the services are de-registered - doing this when the publisher is
        // dead means we can't send any messages to the agents.
        this.eventHandler.destroy();
    }

    /**
     * @return the period in milliseconds to wait before trying a reconnect to a lost service
     */
    public int getReconnectPeriodMillis()
    {
        return this.reconnectPeriodMillis;
    }

    /**
     * Set the period to wait before attempting to reconnect to a lost service after the TCP
     * connection has been unexpectedly broken
     * 
     * @param reconnectPeriodMillis
     *            the period in milliseconds to wait before trying a reconnect to a lost service
     */
    public void setReconnectPeriodMillis(int reconnectPeriodMillis)
    {
        this.reconnectPeriodMillis = reconnectPeriodMillis;
    }

    @Override
    public String toString()
    {
        return "PlatformRegistry [" + this.platformName + "] " + this.publisher.getEndPointAddress();
    }
}

/**
 * Encapsulates handling of events for the registry.
 * <p>
 * Events handling is split between record processing and IO processing, this is to ensure record
 * processing is not blocked by IO operations.
 * <p>
 * E.g. service registration (the most complex event handling) occurs as follows:
 * 
 * <pre>
 * [core thread] check registration details of service instance
 * [io thread] connect to service instance
 * [core thread] when connected, add service to registry records
 * [io thread] register listeners for the service instance
 * </pre>
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings("synthetic-access")
final class EventHandler
{
    private static final String RUNTIME_STATUS = "runtimeStatus";
    private static final String SLOW = "*** SLOW EVENT HANDLING *** ";
    private static final int SLOW_EVENT_MILLIS = 200;
    private static final ThreadFactory IO_EXECUTOR_THREAD_FACTORY = ThreadUtils.newDaemonThreadFactory("io-executor");
    private static final ThreadFactory PUBLISH_EXECUTOR_THREAD_FACTORY =
        ThreadUtils.newDaemonThreadFactory("publish-executor");

    private static final boolean SERVICES_LOG_DISABLED = Boolean.getBoolean("platform.servicesLogDisabled");
    private static final RollingFileAppender SERVICES_LOG = SERVICES_LOG_DISABLED ? null
        : RollingFileAppender.createStandardRollingFileAppender("services", UtilProperties.Values.LOG_DIR);
    private static final Executor SERVICES_LOG_EXECUTOR =
        SERVICES_LOG_DISABLED ? null : ThreadUtils.newSingleThreadExecutorService("services-log");
    private static final FastDateFormat fdf = new FastDateFormat();

    private static void banner(EventHandler eventHandler, final String message)
    {
        try
        {
            Log.banner(eventHandler, message);

            if (SERVICES_LOG_DISABLED)
            {
                return;
            }

            final long now = System.currentTimeMillis();
            SERVICES_LOG_EXECUTOR.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        SERVICES_LOG.append(fdf.yyyyMMddHHmmssSSS(now)).append("|").append(message).append(
                            SystemUtils.lineSeparator());
                        SERVICES_LOG.flush();
                    }
                    catch (IOException e)
                    {
                        Log.log(EventHandler.class, "Could not log service message", e);
                    }
                }
            });
        }
        catch (Exception e)
        {
            Log.log(eventHandler, "Could not log banner message: " + message, e);
        }
    }
    
    private final static IRecordListener NOOP_OBSERVER = new IRecordListener()
    {
        @Override
        public void onChange(IRecord image, IRecordChange atomicChange)
        {
            // noop
        }
    };
    
    private static interface IDescriptiveRunnable extends ISequentialRunnable
    {
        String getDescription();
    }

    static FutureTask<String> createStubFuture(final String serviceInstanceId)
    {
        final FutureTask<String> futureTask = new FutureTask<String>(new Runnable()
        {
            @Override
            public void run()
            {
                // noop
            }
        }, serviceInstanceId);
        futureTask.run();
        return futureTask;
    }

    final long startTimeMillis;
    final PlatformRegistry registry;
    final AtomicInteger eventCount;
    final Set<String> pendingPublish;
    final ScheduledExecutorService publishExecutor;
    final ExecutorService ioExecutor;
    /**
     * Tracks services that are pending registration completion
     * 
     * @see IRegistryRecordNames#SERVICES
     */
    final ConcurrentMap<String, IValue> pendingPlatformServices;
    /**
     * Key=service instance ID, Value=Registration token; tracks pending service instances to
     * prevent duplicates
     */
    final ConcurrentMap<String, RegistrationToken> registrationTokenPerInstance;
    /** Key=service family, Value=PENDING master service instance ID */
    final ConcurrentMap<String, String> pendingMasterInstancePerFtService;
    /** Key=service family, Value=Future with the actual master service instance ID */
    final ConcurrentMap<String, FutureTask<String>> confirmedMasterInstancePerFtService;
    final ConcurrentMap<RegistrationToken, ProxyContext> monitoredServiceInstances;
    final ConcurrentMap<RegistrationToken, PlatformServiceConnectionMonitor> connectionMonitors;
    final ConcurrentMap<String, Set<String>> connectionsPerServiceFamily;
    final AtomicInteger servicesRecordSize;

    final Object connectionsLock;
    
    EventHandler(final PlatformRegistry registry)
    {
        this.registry = registry;
        this.connectionsLock = new Object();
        this.startTimeMillis = System.currentTimeMillis();

        this.servicesRecordSize = new AtomicInteger(0);
        this.monitoredServiceInstances = new ConcurrentHashMap<RegistrationToken, ProxyContext>();
        this.connectionMonitors = new ConcurrentHashMap<RegistrationToken, PlatformServiceConnectionMonitor>();
        this.pendingMasterInstancePerFtService = new ConcurrentHashMap<String, String>();
        this.confirmedMasterInstancePerFtService = new ConcurrentHashMap<String, FutureTask<String>>();
        this.pendingPlatformServices = new ConcurrentHashMap<String, IValue>();
        this.registrationTokenPerInstance = new ConcurrentHashMap<String, RegistrationToken>();
        this.connectionsPerServiceFamily = new ConcurrentHashMap<String, Set<String>>();

        this.pendingPublish = new HashSet<String>();
        this.eventCount = new AtomicInteger(0);
        this.publishExecutor =
            new ScheduledThreadPoolExecutor(1, PUBLISH_EXECUTOR_THREAD_FACTORY, new ThreadPoolExecutor.DiscardPolicy());
        this.ioExecutor = new ThreadPoolExecutor(1, Integer.MAX_VALUE, 10, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(), IO_EXECUTOR_THREAD_FACTORY, new ThreadPoolExecutor.DiscardPolicy());
    }

    void execute(final IDescriptiveRunnable runnable)
    {
        if (this.eventCount.incrementAndGet() % 50 == 0)
        {
            Log.log(this, "*** Event queue: " + this.eventCount.get());
        }

        this.registry.context.executeSequentialCoreTask(new IDescriptiveRunnable()
        {
            @Override
            public void run()
            {
                EventHandler.this.eventCount.decrementAndGet();
                long time = System.nanoTime();
                try
                {
                    runnable.run();
                }
                catch (Exception e)
                {
                    Log.log(runnable,
                        "Could not execute " + runnable.getDescription() + " {" + runnable.context() + "}", e);
                }
                finally
                {
                    time = (long) ((System.nanoTime() - time) * 0.000001d);
                    if (time > SLOW_EVENT_MILLIS)
                    {
                        Log.log(EventHandler.this, SLOW, runnable.getDescription(), " {",
                            ObjectUtils.safeToString(runnable.context()), "} took ", Long.toString(time), "ms");
                    }
                }
            }

            @Override
            public Object context()
            {
                return runnable.context();
            }

            @Override
            public String getDescription()
            {
                return runnable.getDescription();
            }
        });
    }

    void destroy()
    {
        this.publishExecutor.shutdown();
        PlatformServiceConnectionMonitor monitor = null;
        for (RegistrationToken registrationToken : this.connectionMonitors.keySet())
        {
            try
            {
                monitor = this.connectionMonitors.remove(registrationToken);
                if (monitor != null)
                {
                    monitor.destroy();
                }
            }
            catch (Exception e)
            {
                Log.log(this, "Could not destroy " + ObjectUtils.safeToString(monitor), e);
            }
        }
        ProxyContext proxy = null;
        for (RegistrationToken registrationToken : this.monitoredServiceInstances.keySet())
        {
            try
            {
                proxy = this.monitoredServiceInstances.remove(registrationToken);
                if (proxy != null)
                {
                    proxy.destroy();
                }
            }
            catch (Exception e)
            {
                Log.log(this, "Could not destroy " + ObjectUtils.safeToString(proxy), e);
            }
        }
    }

    void executeRpcRuntimeDynamic(final IValue... args)
    {
        execute(new IDescriptiveRunnable()
        {
            @Override
            public String getDescription()
            {
                return context().toString();
            }

            @Override
            public Object context()
            {
                return RUNTIME_STATUS;
            }

            @Override
            public void run()
            {
                handleRpcRuntimeDynamic(args);
            }
        });
    }

    void executeRpcRuntimeStatic(final IValue... args)
    {
        execute(new IDescriptiveRunnable()
        {
            @Override
            public String getDescription()
            {
                return "handleRpcStaticRuntime";
            }

            @Override
            public Object context()
            {
                return RUNTIME_STATUS;
            }

            @Override
            public void run()
            {
                handleRpcStaticRuntime(args);
            }
        });
    }

    void executeRegisterServiceInstance(final RegistrationToken registrationToken, final String serviceFamily,
        final String agentName, final String serviceInstanceId, final RedundancyModeEnum redundancyModeEnum,
        final TransportTechnologyEnum transportTechnology, final Map<String, IValue> serviceRecordStructure,
        final IValue... args)
    {
        registerStep1_checkRegistrationDetailsForServiceInstance(registrationToken, serviceFamily, agentName,
            serviceRecordStructure, serviceInstanceId, redundancyModeEnum, transportTechnology, args);

        executeTaskWithIO(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    registerStep2_connectToServiceInstanceThenContinueRegistration(registrationToken, serviceFamily,
                        agentName, serviceRecordStructure, serviceInstanceId, redundancyModeEnum, transportTechnology);
                }
                catch (Exception e)
                {
                    logExceptionAndExecuteDeregister(registrationToken, serviceFamily, serviceInstanceId, e,
                        "Could not connect");
                }
            }
        });
    }

    void executeDeregisterPlatformServiceInstance(RegistrationToken registrationToken, final String serviceFamily,
        final String serviceInstanceId, String cause)
    {
        Log.log(this, "PREPARE deregister ", (registrationToken == null ? "'" + serviceInstanceId + "' token=null "
            : ObjectUtils.safeToString(registrationToken)), " (", cause, ")");

        final RegistrationToken _registrationToken;
        if (registrationToken == null)
        {
            _registrationToken = this.registrationTokenPerInstance.get(serviceInstanceId);
            Log.log(this, "Found ", ObjectUtils.safeToString(_registrationToken));
        }
        else
        {
            _registrationToken = registrationToken;
        }

        execute(new IDescriptiveRunnable()
        {
            @Override
            public String getDescription()
            {
                return "deregisterPlatformServiceInstance: " + serviceInstanceId;
            }

            @Override
            public Object context()
            {
                return serviceFamily;
            }

            @Override
            public void run()
            {
                deregisterPlatformServiceInstance_callInFamilyScope(_registrationToken, serviceInstanceId);
            }
        });
    }

    void executeHandleRegistryConnectionsUpdate(final IRecordChange atomicChange)
    {
        execute(new IDescriptiveRunnable()
        {
            @Override
            public String getDescription()
            {
                return "handleRegistryConnectionsUpdate";
            }

            @Override
            public Object context()
            {
                return RUNTIME_STATUS;
            }

            @Override
            public void run()
            {
                handleConnectionsUpdate_callInFamilyScope(atomicChange, PlatformRegistry.SERVICE_NAME,
                    EventHandler.this.registry.platformName);
            }
        });
    }

    void executeHandleRegistryRecordsUpdate(final IRecordChange atomicChange)
    {
        execute(new IDescriptiveRunnable()
        {
            @Override
            public String getDescription()
            {
                return "handleRegistryRecordsUpdate";
            }

            @Override
            public Object context()
            {
                return RUNTIME_STATUS;
            }

            @Override
            public void run()
            {
                handleRegistryRecordsUpdate(atomicChange);
            }
        });
    }

    Future<String> executeSelectNextInstance(final String serviceFamily)
    {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Future<String>> result = new AtomicReference<Future<String>>(null);
        execute(new IDescriptiveRunnable()
        {
            @Override
            public String getDescription()
            {
                return "selectNextInstance: " + serviceFamily;
            }

            @Override
            public void run()
            {
                try
                {
                    result.set(selectNextInstance_callInFamilyScope(serviceFamily, "RPC call"));
                }
                finally
                {
                    latch.countDown();
                }
            }

            @Override
            public Object context()
            {
                return serviceFamily;
            }
        });
        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            Log.log(this, "Interrupted waiting on latch for selectNextInstance: '" + serviceFamily + "'", e);
        }
        return result.get();
    }

    void executeComputePlatformSummary()
    {
        execute(new IDescriptiveRunnable()
        {
            @Override
            public String getDescription()
            {
                return "computePlatformSummary";
            }

            @Override
            public Object context()
            {
                return RUNTIME_STATUS;
            }

            @Override
            public void run()
            {
                computePlatformSummary();
            }
        });
    }

    private void computePlatformSummary()
    {
        final Set<String> hosts = new HashSet<String>();
        final Set<String> agentNames = this.registry.runtimeStatus.getSubMapKeys();
        for (String agentName : agentNames)
        {
            hosts.add(this.registry.runtimeStatus.getOrCreateSubMap(agentName).get(
                IRuntimeStatusRecordFields.RUNTIME_HOST).textValue());
        }
        this.registry.platformSummary.put(IPlatformSummaryRecordFields.NODES, LongValue.valueOf(hosts.size()));

        this.registry.platformSummary.put(IPlatformSummaryRecordFields.SERVICES,
            LongValue.valueOf(this.registry.services.size()));

        this.registry.platformSummary.put(IPlatformSummaryRecordFields.AGENTS,
            LongValue.valueOf(this.registry.runtimeStatus.getSubMapKeys().size()));

        int serviceInstanceCount = 0;
        for (String serviceName : this.registry.serviceInstancesPerServiceFamily.getSubMapKeys())
        {
            serviceInstanceCount +=
                this.registry.serviceInstancesPerServiceFamily.getOrCreateSubMap(serviceName).size();
        }
        this.registry.platformSummary.put(IPlatformSummaryRecordFields.SERVICE_INSTANCES,
            LongValue.valueOf(serviceInstanceCount));

        this.registry.platformSummary.put(IPlatformSummaryRecordFields.CONNECTIONS,
            LongValue.valueOf(this.registry.platformConnections.getSubMapKeys().size()));

        final long uptimeSecs = (long) ((System.currentTimeMillis() - this.startTimeMillis) * 0.001);
        final int minsPerDay = 3600 * 24;
        final long days = uptimeSecs / minsPerDay;
        final long hoursMinsLeft = uptimeSecs - (days * minsPerDay);
        final long hours = hoursMinsLeft / 3600;
        final double mins = (long) (((double) hoursMinsLeft % 3600) / 60);
        this.registry.platformSummary.put(IPlatformSummaryRecordFields.UPTIME,
            "" + days + "d:" + (hours < 10 ? "0" : "") + hours + "h:" + (mins < 10 ? "0" : "") + (long) mins + "m");
        publishTimed(this.registry.platformSummary);
    }

    /**
     * @return a future with the correct instance to use
     */
    private Future<String> selectNextInstance_callInFamilyScope(final String serviceFamily, String cause)
    {
        Log.log(this, "Select next instance '", serviceFamily, "' (", cause, ")");

        String activeServiceMemberName = null;
        final IValue redundancyModeValue = this.registry.services.get(serviceFamily);
        if (redundancyModeValue == null)
        {
            Log.log(this, "Ignoring select next instance for unregistered service: '", serviceFamily, "'");
            return createStubFuture(null);
        }
        if (this.registry.serviceInstancesPerServiceFamily.getSubMapKeys().contains(serviceFamily))
        {
            final Map<String, IValue> serviceInstances =
                this.registry.serviceInstancesPerServiceFamily.getOrCreateSubMap(serviceFamily);
            if (serviceInstances.size() > 0)
            {
                // find the instance with the earliest timestamp
                long earliest = Long.MAX_VALUE;
                Map.Entry<String, IValue> entry = null;
                String key = null;
                IValue value = null;
                for (Iterator<Map.Entry<String, IValue>> it = serviceInstances.entrySet().iterator(); it.hasNext();)
                {
                    entry = it.next();
                    key = entry.getKey();
                    value = entry.getValue();
                    if (value.longValue() < earliest)
                    {
                        earliest = value.longValue();
                        activeServiceMemberName = key;
                    }
                }

                final Future<String> result;
                final String serviceInstanceId =
                    PlatformUtils.composePlatformServiceInstanceID(serviceFamily, activeServiceMemberName);
                if (RedundancyModeEnum.valueOf(redundancyModeValue.textValue()) == RedundancyModeEnum.LOAD_BALANCED)
                {
                    /*
                     * for LB, the sequence is updated so the next instance selected will be one
                     * with an earlier sequence and thus produce a round-robin style selection
                     * policy
                     */
                    serviceInstances.put(activeServiceMemberName, LongValue.valueOf(this.registry.nextSequence()));
                    result = createStubFuture(serviceInstanceId);
                }
                else
                {
                    result = verifyNewMasterInstance(this.registrationTokenPerInstance.get(serviceInstanceId),
                        serviceFamily, serviceInstanceId);
                }

                return result;
            }
        }
        return createStubFuture(null);
    }

    private void deregisterPlatformServiceInstance_callInFamilyScope(final RegistrationToken registrationToken,
        final String serviceInstanceId)
    {
        Log.log(this, "Deregister '", serviceInstanceId, "', token=", ObjectUtils.safeToString(registrationToken));

        try
        {
            removeUnregisteredProxiesAndMonitors();
        }
        catch (Exception e)
        {
            Log.log(this,
                "Could not purge any unregistered connections, continuing with deregister of " + registrationToken, e);
        }

        if (!this.registrationTokenPerInstance.remove(serviceInstanceId, registrationToken))
        {
            Log.log(this, "Ignoring deregister for service instance '", serviceInstanceId,
                "' as the operation was completed earlier, registrationToken=",
                ObjectUtils.safeToString(registrationToken), " currentToken=",
                ObjectUtils.safeToString(this.registrationTokenPerInstance.get(serviceInstanceId)));
            return;
        }

        final PlatformServiceConnectionMonitor connectionMonitor = this.connectionMonitors.remove(registrationToken);
        if (connectionMonitor != null)
        {
            Log.log(this, "Destroying connection monitor for ", ObjectUtils.safeToString(registrationToken));
            connectionMonitor.destroy();
        }
        else
        {
            Log.log(this, "No connection monitor to destroy for ", ObjectUtils.safeToString(registrationToken));
        }
        final ProxyContext proxy = this.monitoredServiceInstances.remove(registrationToken);
        if (proxy != null)
        {
            destroyProxy(registrationToken, proxy);
        }
        else
        {
            Log.log(this, "No proxy to destroy for ", ObjectUtils.safeToString(registrationToken));
        }
    }

    /**
     * Ensures that there are no unregistered proxy or connection monitors.
     * <p>
     * This is a safety clean-up job.
     */
    void removeUnregisteredProxiesAndMonitors()
    {
        final Collection<RegistrationToken> registrationTokens = this.registrationTokenPerInstance.values();
        RegistrationToken registrationToken = null;

        {
            Map.Entry<RegistrationToken, ProxyContext> entry = null;
            for (Iterator<Map.Entry<RegistrationToken, ProxyContext>> it =
                this.monitoredServiceInstances.entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                registrationToken = entry.getKey();
                if (!registrationTokens.contains(registrationToken))
                {
                    Log.log(this, "Destroying UNREGISTERED proxy ", ObjectUtils.safeToString(registrationToken));
                    try
                    {
                        destroyProxy(registrationToken, entry.getValue());
                        it.remove();
                    }
                    catch (Exception e)
                    {
                        Log.log(this, "Could not destroy UNREGISTERED proxy " + registrationToken, e);
                    }
                }
            }
        }
        {
            Map.Entry<RegistrationToken, PlatformServiceConnectionMonitor> entry = null;
            for (Iterator<Map.Entry<RegistrationToken, PlatformServiceConnectionMonitor>> it =
                this.connectionMonitors.entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                registrationToken = entry.getKey();
                if (!registrationTokens.contains(registrationToken))
                {
                    Log.log(this, "Destroying UNREGISTERED connection monitor for ",
                        ObjectUtils.safeToString(registrationToken));
                    try
                    {
                        entry.getValue().destroy();
                        it.remove();
                    }
                    catch (Exception e)
                    {
                        Log.log(this, "Could not destroy UNREGISTERED connection monitor for " + registrationToken, e);
                    }
                }
            }
        }
    }

    void destroyProxy(final RegistrationToken registrationToken, final ProxyContext proxy)
    {
        final String serviceInstanceId = registrationToken.getServiceInstanceId();
        final String[] serviceParts = PlatformUtils.decomposePlatformServiceInstanceID(serviceInstanceId);
        final String serviceFamily = serviceParts[0];
        final String serviceMember = serviceParts[1];

        banner(this,
            "Deregistering " + registrationToken + " (was monitored with " + proxy.getChannelString() + ")");

        proxy.destroy();

        // remove the service instance info record
        this.registry.context.removeRecord(
            PlatformRegistry.ServiceInfoRecordFields.SERVICE_INFO_RECORD_NAME_PREFIX + serviceInstanceId);

        // remove connections - we need to scan the entire platform connections to find
        // matching connections from the publisher that is now dead
        synchronized(this.connectionsLock)
        {
            for (String connection : new HashSet<String>(this.registry.platformConnections.getSubMapKeys()))
            {
                final Map<String, IValue> subMap = this.registry.platformConnections.getOrCreateSubMap(connection);
                final IValue iValue = subMap.get(IContextConnectionsRecordFields.PUBLISHER_ID);
                if (iValue != null)
                {
                    if (serviceInstanceId.equals(iValue.textValue()))
                    {
                        this.registry.platformConnections.removeSubMap(connection);
                    }
                }
            }
        }
        publishTimed(this.registry.platformConnections);

        removeRecordsAndRpcsPerServiceInstance(serviceInstanceId, serviceFamily);

        // remove the service instance from the instances-per-service
        Map<String, IValue> serviceInstances = Collections.emptyMap();
        if (this.registry.serviceInstancesPerServiceFamily.getSubMapKeys().contains(serviceFamily))
        {
            serviceInstances = this.registry.serviceInstancesPerServiceFamily.getOrCreateSubMap(serviceFamily);
            serviceInstances.remove(serviceMember);
            if (serviceInstances.size() == 0)
            {
                this.registry.serviceInstancesPerServiceFamily.removeSubMap(serviceFamily);
                this.registry.services.removeSubMap(serviceFamily);
            }
            else
            {
                this.registry.serviceStats.getOrCreateSubMap(serviceFamily).put(IServiceRecordFields.SERVICE_INSTANCE_COUNT,
                    LongValue.valueOf(serviceInstances.size()));
                publishTimed(this.registry.serviceStats);
            }
        }

        // remove the service instance from the instances-per-agent
        Map<String, IValue> instancesPerAgent;
        Set<String> agentsWithNoServiceInstance = new HashSet<String>();
        for (String agentName : this.registry.serviceInstancesPerAgent.getSubMapKeys())
        {
            // we don't know which agent has it so scan them all
            instancesPerAgent = this.registry.serviceInstancesPerAgent.getOrCreateSubMap(agentName);
            instancesPerAgent.remove(serviceInstanceId);
            if (instancesPerAgent.size() == 0)
            {
                agentsWithNoServiceInstance.add(agentName);
            }
        }
        for (String agentName : agentsWithNoServiceInstance)
        {
            this.registry.serviceInstancesPerAgent.removeSubMap(agentName);
        }

        if (serviceInstances.size() == 0)
        {
            if (this.registry.services.remove(serviceFamily) != null)
            {
                Log.log(this, "Removing service '", serviceFamily, "' from registry");
                this.registry.services.removeSubMap(serviceFamily);
                this.pendingMasterInstancePerFtService.remove(serviceFamily);
                this.confirmedMasterInstancePerFtService.remove(serviceFamily);
                this.connectionsPerServiceFamily.remove(serviceFamily);
                publishTimed(this.registry.services);
            }
        }
        else
        {
            if (checkServiceType(serviceFamily, RedundancyModeEnum.FAULT_TOLERANT))
            {
                // this will ensure there is a valid master of the FT service
                selectNextInstance_callInFamilyScope(serviceFamily, "deregister " + registrationToken);
            }
        }

        publishTimed(this.registry.serviceInstancesPerServiceFamily);
        publishTimed(this.registry.serviceInstancesPerAgent);

        removeServiceStats(serviceInstanceId);
    }

    private void executeTaskWithIO(Runnable runnable)
    {
        this.ioExecutor.execute(new ThreadUtils.ExceptionLoggingRunnable(runnable));
    }

    /**
     * Checks that the parameters are valid for registering a new service.
     * 
     * @throws AlreadyRegisteredException
     *             if the service is already registered
     * @throws IllegalStateException
     *             if the registration parameters clash with any in-flight registration
     */
    @SuppressWarnings("unused")
    private void registerStep1_checkRegistrationDetailsForServiceInstance(final RegistrationToken registrationToken,
        final String serviceFamily, final String agentName, final Map<String, IValue> serviceRecordStructure,
        final String serviceInstanceId, final RedundancyModeEnum redundancyModeEnum,
        final TransportTechnologyEnum transportTechnology, final IValue... args)
    {
        Log.log(this, "CHECK ", ObjectUtils.safeToString(registrationToken));

        // check if already registered/being registered
        final RegistrationToken currentToken =
            this.registrationTokenPerInstance.putIfAbsent(serviceInstanceId, registrationToken);
        if (currentToken != null)
        {
            final ProxyContext proxy = this.monitoredServiceInstances.get(currentToken);

            if (proxy != null && proxy.isConnected())
            {
                throw new AlreadyRegisteredException(serviceInstanceId, agentName, proxy.getEndPointAddress().getNode(),
                    proxy.getEndPointAddress().getPort(),
                    checkServiceType(serviceFamily, RedundancyModeEnum.FAULT_TOLERANT)
                        ? RedundancyModeEnum.FAULT_TOLERANT.toString() : RedundancyModeEnum.LOAD_BALANCED.toString());
            }

            // no connection, so its an in-flight registration
            throw new IllegalStateException("[DUPLICATE INSTANCE] Platform service instance '" + serviceInstanceId
                + "' is currently being registered with " + currentToken);
        }

        switch(redundancyModeEnum)
        {
            case FAULT_TOLERANT:
                if (checkServiceType(serviceFamily, RedundancyModeEnum.LOAD_BALANCED))
                {
                    throw new IllegalArgumentException("Platform service '" + serviceFamily
                        + "' is already registered as " + RedundancyModeEnum.LOAD_BALANCED);
                }
                break;
            case LOAD_BALANCED:
                if (checkServiceType(serviceFamily, RedundancyModeEnum.FAULT_TOLERANT))
                {
                    throw new IllegalArgumentException("Platform service '" + serviceFamily
                        + "' is already registered as " + RedundancyModeEnum.FAULT_TOLERANT);
                }
                break;
            default :
                throw new IllegalArgumentException(
                    "Unhandled mode '" + redundancyModeEnum + "' for service '" + serviceFamily + "'");
        }

        // add to pending AFTER checking services
        final IValue current =
            this.pendingPlatformServices.putIfAbsent(serviceFamily, TextValue.valueOf(redundancyModeEnum.name()));
        if (current != null && RedundancyModeEnum.valueOf(current.textValue()) != redundancyModeEnum)
        {
            throw new IllegalArgumentException("Platform service '" + serviceFamily
                + "' is currently being registered as " + RedundancyModeEnum.valueOf(current.textValue()));
        }
    }

    private void registerStep2_connectToServiceInstanceThenContinueRegistration(
        final RegistrationToken registrationToken, final String serviceFamily, final String agentName,
        final Map<String, IValue> serviceRecordStructure, final String serviceInstanceId,
        final RedundancyModeEnum redundancyModeEnum, final TransportTechnologyEnum transportTechnology)
    {
        Log.log(this, "CONNECT ", ObjectUtils.safeToString(registrationToken));
        // NOTE: this is blocks for the TCP construction...
        // connect to the service using the service's transport technology
        final ProxyContext serviceProxy =
            new ProxyContext(PlatformUtils.composeProxyName(serviceInstanceId, this.registry.context.getName()),
                PlatformUtils.getCodecFromServiceInfoRecord(serviceRecordStructure),
                PlatformUtils.getHostNameFromServiceInfoRecord(serviceRecordStructure),
                PlatformUtils.getPortFromServiceInfoRecord(serviceRecordStructure), transportTechnology,
                PlatformRegistry.SERVICE_NAME);
        serviceProxy.setReconnectPeriodMillis(this.registry.reconnectPeriodMillis);

        this.monitoredServiceInstances.put(registrationToken, serviceProxy);
        
        // add a noop observer for the RPC record (stops subscribe-unsubscribe for RPCs)
        serviceProxy.addObserver(NOOP_OBSERVER, REMOTE_CONTEXT_RPCS);

        // setup monitoring of the service instance via the proxy
        final PlatformServiceConnectionMonitor monitor = createConnectionMonitor(registrationToken, serviceFamily,
            agentName, serviceRecordStructure, serviceInstanceId, redundancyModeEnum, serviceProxy);

        this.connectionMonitors.put(registrationToken, monitor);
    }

    private PlatformServiceConnectionMonitor createConnectionMonitor(final RegistrationToken registrationToken,
        final String serviceFamily, final String agentName, final Map<String, IValue> serviceRecordStructure,
        final String serviceInstanceId, final RedundancyModeEnum redundancyModeEnum, final ProxyContext serviceProxy)
    {
        return new PlatformServiceConnectionMonitor(serviceProxy, serviceInstanceId)
        {
            @Override
            protected void onPlatformServiceDisconnected()
            {
                executeDeregisterPlatformServiceInstance(registrationToken, serviceFamily, this.serviceInstanceId,
                    "connection lost");
            }

            @Override
            protected void onPlatformServiceConnected()
            {
                execute(new IDescriptiveRunnable()
                {
                    @SuppressWarnings("unqualified-field-access")
                    @Override
                    public String getDescription()
                    {
                        return "registerServiceInstanceWhenConnectionEstablished:" + serviceInstanceId;
                    }

                    @Override
                    public Object context()
                    {
                        return serviceFamily;
                    }

                    @SuppressWarnings("unqualified-field-access")
                    @Override
                    public void run()
                    {
                        try
                        {
                            final Object currentToken =
                                EventHandler.this.registrationTokenPerInstance.get(serviceInstanceId);
                            if (!is.eq(registrationToken, currentToken))
                            {
                                Log.log(EventHandler.this, "Registration token changed for '", serviceInstanceId,
                                    "'. Ignoring connect event, currentToken=", ObjectUtils.safeToString(currentToken),
                                    ", registrationToken=", ObjectUtils.safeToString(registrationToken));
                                return;
                            }

                            registerStep3_continueRegistrationWhenConnectionEstablished_callInFamilyScope(
                                registrationToken, agentName, serviceInstanceId, serviceRecordStructure,
                                redundancyModeEnum);
                        }
                        catch (Exception e)
                        {
                            logExceptionAndDeregister_familyScope(registrationToken, serviceInstanceId, e);
                        }
                    }
                });
            }
        };
    }

    private void registerStep3_continueRegistrationWhenConnectionEstablished_callInFamilyScope(
        final RegistrationToken registrationToken, final String agentName, final String serviceInstanceId,
        final Map<String, IValue> serviceRecordStructure, final RedundancyModeEnum redundancyModeEnum)
    {
        final String[] serviceParts = PlatformUtils.decomposePlatformServiceInstanceID(serviceInstanceId);
        final String serviceFamily = serviceParts[0];
        final String serviceMember = serviceParts[1];

        final ProxyContext serviceProxy = this.monitoredServiceInstances.get(registrationToken);

        if (RedundancyModeEnum.FAULT_TOLERANT == redundancyModeEnum)
        {
            executeTaskWithIO(new Runnable()
            {
                @Override
                public void run()
                {
                    // always trigger standby first
                    if (callFtServiceStatusRpc(registrationToken, serviceFamily, serviceInstanceId, false))
                    {
                        // after calling standby, continue with the rest of the registration
                        execute(new IDescriptiveRunnable()
                        {
                            @Override
                            public String getDescription()
                            {
                                return "publishServiceDetails:" + serviceInstanceId;
                            }

                            @Override
                            public Object context()
                            {
                                return serviceFamily;
                            }

                            @Override
                            public void run()
                            {
                                try
                                {
                                    registerStep4_publishServiceDetails(agentName, serviceInstanceId,
                                        serviceRecordStructure, redundancyModeEnum, serviceFamily, serviceMember);

                                    executeTaskWithIO(new Runnable()
                                    {
                                        @Override
                                        public void run()
                                        {
                                            try
                                            {
                                                registerStep5_registerListenersForServiceInstance(serviceFamily,
                                                    serviceMember, serviceInstanceId, serviceProxy);

                                                // this will ensure the service FT signals are
                                                // triggered
                                                execute(new IDescriptiveRunnable()
                                                {
                                                    @Override
                                                    public String getDescription()
                                                    {
                                                        return "ftService_selectNextInstance:" + serviceInstanceId;
                                                    }

                                                    @Override
                                                    public Object context()
                                                    {
                                                        return serviceFamily;
                                                    }

                                                    @Override
                                                    public void run()
                                                    {
                                                        try
                                                        {
                                                            selectNextInstance_callInFamilyScope(serviceFamily,
                                                                "register " + registrationToken);

                                                            banner(EventHandler.this,
                                                                "Registered " + registrationToken + " "
                                                                    + redundancyModeEnum + " (monitoring with "
                                                                    + serviceProxy.getChannelString() + ")");
                                                        }
                                                        catch (Exception e)
                                                        {
                                                            logExceptionAndDeregister_familyScope(registrationToken,
                                                                serviceInstanceId, e);
                                                        }
                                                    }
                                                });
                                            }
                                            catch (Exception e)
                                            {
                                                logExceptionAndExecuteDeregister(registrationToken, serviceFamily,
                                                    serviceInstanceId, e,
                                                    "Could not register service record listeners");
                                            }
                                        }
                                    });
                                }
                                catch (Exception e)
                                {
                                    logExceptionAndDeregister_familyScope(registrationToken, serviceInstanceId, e);
                                }
                            }
                        });
                    }
                }
            });
        }
        else
        {
            // load balanced services are simple to register, just 2 steps

            registerStep4_publishServiceDetails(agentName, serviceInstanceId, serviceRecordStructure,
                redundancyModeEnum, serviceFamily, serviceMember);

            executeTaskWithIO(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        registerStep5_registerListenersForServiceInstance(serviceFamily, serviceMember,
                            serviceInstanceId, serviceProxy);

                        banner(EventHandler.this, "Registered " + registrationToken + " " + redundancyModeEnum
                            + " (monitoring with " + serviceProxy.getChannelString() + ")");
                    }
                    catch (Exception e)
                    {
                        logExceptionAndExecuteDeregister(registrationToken, serviceFamily, serviceInstanceId, e,
                            "Could not register service record listeners");
                    }
                }
            });
        }
    }

    private void registerStep4_publishServiceDetails(final String agentName, final String serviceInstanceId,
        final Map<String, IValue> serviceRecordStructure, final RedundancyModeEnum redundancyModeEnum,
        final String serviceFamily, final String serviceMember)
    {
        this.registry.context.createRecord(
            PlatformRegistry.ServiceInfoRecordFields.SERVICE_INFO_RECORD_NAME_PREFIX + serviceInstanceId,
            serviceRecordStructure);

        // register the service member
        final Map<String, IValue> servicesInstancesForFamilySubMap =
            this.registry.serviceInstancesPerServiceFamily.getOrCreateSubMap(serviceFamily);
        servicesInstancesForFamilySubMap.put(serviceMember, LongValue.valueOf(this.registry.nextSequence()));
        publishTimed(this.registry.serviceInstancesPerServiceFamily);

        // register the service instance against the agent
        this.registry.serviceInstancesPerAgent.getOrCreateSubMap(agentName).put(serviceInstanceId,
            PlatformRegistry.BLANK_VALUE);
        publishTimed(this.registry.serviceInstancesPerAgent);

        this.registry.services.put(serviceFamily, redundancyModeEnum.name());
        this.registry.serviceStats.getOrCreateSubMap(serviceFamily).put(IServiceRecordFields.SERVICE_INSTANCE_COUNT,
            LongValue.valueOf(servicesInstancesForFamilySubMap.size()));

        // as the service has now been declared to be of a specific redundancy type, it does not
        // matter if duplicate instances of the same family are registering simultaneously
        this.pendingPlatformServices.remove(serviceFamily);

        publishTimed(this.registry.serviceStats);
        publishTimed(this.registry.services);
    }

    private void handleRpcStaticRuntime(final IValue... args)
    {
        final String agentName = args[0].textValue();
        Map<String, IValue> runtimeRecord = this.registry.runtimeStatus.getOrCreateSubMap(agentName);
        runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.RUNTIME_HOST, args[1]);
        runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.RUNTIME, args[2]);
        runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.USER, args[3]);
        runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.CPU_COUNT, args[4]);
        publishTimed(this.registry.runtimeStatus);
        banner(this, "Agent connected: " + agentName);
    }

    private void handleRpcRuntimeDynamic(final IValue... args)
    {
        final String agentName = args[0].textValue();
        if (this.registry.runtimeStatus.getSubMapKeys().contains(agentName))
        {
            Map<String, IValue> runtimeRecord = this.registry.runtimeStatus.getOrCreateSubMap(agentName);
            runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.Q_OVERFLOW, args[1]);
            runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.Q_TOTAL_SUBMITTED, args[2]);
            runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.MEM_USED_MB, args[3]);
            runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.MEM_AVAILABLE_MB, args[4]);
            runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.THREAD_COUNT, args[5]);
            runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.SYSTEM_LOAD, args[6]);
            runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.EPS, args[7]);
            runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.UPTIME_SECS, args[8]);
            publishTimed(this.registry.runtimeStatus);
        }
        else
        {
            Log.log(this, "WARNING: RPC call from unregistered agent ", agentName);
        }
    }
    
    private void handleConnectionsUpdate_callInFamilyScope(final IRecordChange atomicChange, String serviceFamily,
        String serviceMember)
    {
        // check if the service instance is still registered
        if (this.registry.serviceInstancesPerServiceFamily.getSubMapKeys().contains(serviceFamily))
        {
            if (!this.registry.serviceInstancesPerServiceFamily.getOrCreateSubMap(serviceFamily).containsKey(
                serviceMember))
            {
                // ignore this update - the service has been de-registered and the connections
                // update is from a late handled update
                Log.log(this, "Ignoring connection update from de-registered instance: '", serviceFamily, "[",
                    serviceMember, "]'");
                return;
            }
        }

        // get the connections for the family
        Set<String> connectionIds = this.connectionsPerServiceFamily.get(serviceFamily);
        if (connectionIds == null)
        {
            connectionIds = new HashSet<String>();
            this.connectionsPerServiceFamily.put(serviceFamily, connectionIds);
        }
        connectionIds.addAll(atomicChange.getSubMapKeys());

        long subscriptionCount = 0;
        long txQueue = 0;
        long msgsPublished = 0;
        long msgsPerSec = 0;
        double kbPerSec = 0;
        long kbPublished = 0;
        
        synchronized(this.connectionsLock)
        {
            IValue proxyId;
            String agent = null;
            IRecordChange subMapAtomicChange;
            Map<String, IValue> connection;
            for (String connectionId : atomicChange.getSubMapKeys())
            {
                subMapAtomicChange = atomicChange.getSubMapAtomicChange(connectionId);
                if ((proxyId = subMapAtomicChange.getRemovedEntries().get(
                    ISystemRecordNames.IContextConnectionsRecordFields.PROXY_ID)) != null)
                {
                    if (proxyId.textValue().startsWith(PlatformRegistry.AGENT_PROXY_ID_PREFIX))
                    {
                        agent = proxyId.textValue().substring(PlatformRegistry.AGENT_PROXY_ID_PREFIX_LEN);
                        // purge the runtimeStatus record
                        this.registry.runtimeStatus.removeSubMap(agent);
                        banner(this, "Agent disconnected: " + agent);
                    }
                }
    
                connection = this.registry.platformConnections.getOrCreateSubMap(connectionId);
                subMapAtomicChange.applyTo(connection);
                if (connection.isEmpty())
                {
                    // purge the connection
                    this.registry.platformConnections.removeSubMap(connectionId);
                    connectionIds.remove(connectionId);
                }
            }
    
            // aggregate stats at the service-level service, e.g. subscription, txQueue, msgs published
            for (String connectionId : connectionIds)
            {
                connection = this.registry.platformConnections.getOrCreateSubMap(connectionId);
                if(connection.isEmpty())
                {
                    this.registry.platformConnections.removeSubMap(connectionId);
                    continue;
                }
                subscriptionCount += getLong(connection.get(IContextConnectionsRecordFields.SUBSCRIPTION_COUNT));
                msgsPublished += getLong(connection.get(IContextConnectionsRecordFields.MESSAGE_COUNT));
                msgsPerSec += getLong(connection.get(IContextConnectionsRecordFields.MSGS_PER_SEC));
                kbPerSec += getDouble(connection.get(IContextConnectionsRecordFields.KB_PER_SEC));
                kbPublished += getLong(connection.get(IContextConnectionsRecordFields.KB_COUNT));
                txQueue += getLong(connection.get(IContextConnectionsRecordFields.TX_QUEUE_SIZE));
            }
        }
        
        final Map<String, IValue> familyStats = this.registry.serviceStats.getOrCreateSubMap(serviceFamily);
        familyStats.put(IServiceRecordFields.KB_COUNT, LongValue.valueOf(kbPublished));
        familyStats.put(IServiceRecordFields.KB_PER_SEC, DoubleValue.valueOf(((long) ((kbPerSec * 10d)) / 10d)));
        familyStats.put(IServiceRecordFields.MESSAGE_COUNT, LongValue.valueOf(msgsPublished));
        familyStats.put(IServiceRecordFields.MSGS_PER_SEC, LongValue.valueOf(msgsPerSec));
        familyStats.put(IServiceRecordFields.SUBSCRIPTION_COUNT, LongValue.valueOf(subscriptionCount));
        familyStats.put(IServiceRecordFields.TX_QUEUE_SIZE, LongValue.valueOf(txQueue));

        publishTimed(this.registry.serviceStats);
        publishTimed(this.registry.runtimeStatus);
        publishTimed(this.registry.platformConnections);
    }

    private static double getDouble(IValue iValue)
    {
        return iValue == null ? 0 : iValue.doubleValue();
    }

    private static long getLong(IValue iValue)
    {
        return iValue == null ? 0 : iValue.longValue();
    }

    /**
     * @return a future with the instance ID that is the confirmed master. The {@link Future#get()}
     *         may throw an exception which indicates the master instance selection failed.
     */
    private Future<String> verifyNewMasterInstance(final RegistrationToken registrationToken,
        final String serviceFamily, final String activeServiceInstanceId)
    {
        Log.log(this, "Verify MASTER '", serviceFamily, "' '", activeServiceInstanceId, "'");
        final String previousMasterInstance;

        // first check if the master instance changes
        if (!is.eq(
            (previousMasterInstance =
                this.pendingMasterInstancePerFtService.put(serviceFamily, activeServiceInstanceId)),
            activeServiceInstanceId))
        {
            final AtomicReference<FutureTask<String>> futureTaskRef = new AtomicReference<FutureTask<String>>();
            final FutureTask<String> futureTask = new FutureTask<String>(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        if (previousMasterInstance != null)
                        {
                            final RegistrationToken previousInstanceRegistrationToken =
                                EventHandler.this.registrationTokenPerInstance.get(previousMasterInstance);
                            if (previousInstanceRegistrationToken != null)
                            {
                                callFtServiceStatusRpc(previousInstanceRegistrationToken, serviceFamily,
                                    previousMasterInstance, false);
                            }
                            else
                            {
                                Log.log(EventHandler.this,
                                    "Not signalling STANDBY instance (no registration token found, it has probably been deregistered already): '",
                                    previousMasterInstance, "'");
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Log.log(EventHandler.this, "Could not signal STANDBY instance '" + previousMasterInstance + "'",
                            e);
                    }

                    boolean signalMasterInstanceResult = false;
                    try
                    {
                        signalMasterInstanceResult =
                            callFtServiceStatusRpc(registrationToken, serviceFamily, activeServiceInstanceId, true);
                    }
                    catch (Exception e)
                    {
                        Log.log(EventHandler.this, "Could not signal MASTER instance " + registrationToken, e);
                    }

                    if (!signalMasterInstanceResult)
                    {
                        EventHandler.this.pendingMasterInstancePerFtService.remove(serviceFamily,
                            activeServiceInstanceId);
                        EventHandler.this.confirmedMasterInstancePerFtService.remove(serviceFamily,
                            futureTaskRef.get());
                    }
                }
            }, activeServiceInstanceId);
            futureTaskRef.set(futureTask);

            this.confirmedMasterInstancePerFtService.put(serviceFamily, futureTask);

            executeTaskWithIO(futureTask);

            return futureTask;
        }

        return this.confirmedMasterInstancePerFtService.get(serviceFamily);
    }

    /**
     * Call the RPC to activate/deactivate the master instance of an FT service.
     * <p>
     * <b>If the RPC fails, the service is deregistered.</b>
     * 
     * @return <code>true</code> if the RPC call succeeded, <code>false</code> if not
     */
    private boolean callFtServiceStatusRpc(RegistrationToken registrationToken, String serviceFamily,
        String activeServiceInstanceId, boolean active)
    {
        Log.log(this, "Signalling ", (active ? "MASTER" : "STANDBY"), " FT service ",
            ObjectUtils.safeToString(registrationToken));

        final ProxyContext proxy = this.monitoredServiceInstances.get(registrationToken);

        // HOW CAN THE PROXY BE NULL?
        // ANSWER: if a FAULT_TOLERANT service is destroyed before the registry has called the
        // service's ftServiceInstanceStatus service status RPC then when the code reaches this
        // point, if the deregister has completed then there will be no active instance
        if (proxy == null)
        {
            Log.log(this, "No connection found for ", ObjectUtils.safeToString(registrationToken),
                ", cannot signal FT service");
            return true;
        }

        try
        {
            ContextUtils.getRpc(proxy, PlatformCoreProperties.Values.REGISTRY_RPC_FT_SERVICE_STATUS_TIMEOUT_MILLIS,
                PlatformServiceInstance.RPC_FT_SERVICE_STATUS).executeNoResponse(
                    TextValue.valueOf(Boolean.valueOf(active).toString()));
        }
        catch (Exception e)
        {
            Log.log(this,  "Could not execute FT service RPC", e);
            executeDeregisterPlatformServiceInstance(registrationToken, serviceFamily, activeServiceInstanceId,
                "could not signal " + (active ? "MASTER" : "STANDBY") + " FT status:" + e.toString());
            return false;
        }
        return true;
    }

    private boolean checkServiceType(final String serviceFamily, final RedundancyModeEnum type)
    {
        final IValue iValue = this.registry.services.get(serviceFamily);
        if (iValue == null)
        {
            return false;
        }
        return RedundancyModeEnum.valueOf(iValue.textValue()) == type;
    }

    private void handleChangeForObjectsPerServiceAndInstance(final String serviceFamily, final String serviceInstanceId,
        IRecordChange atomicChange, final IRecord serviceInstanceObjects, final IRecord serviceObjects,
        final boolean aggregateValuesAsLongs, String servicesObjectCountField)
    {
        try
        {
            // first handle updates to the object record for the service instance
            atomicChange.applyTo(serviceInstanceObjects);
            publishTimed(serviceInstanceObjects);

            /*
             * build up an array of the objects for each service instance of this service, we need
             * all service instance objects to work out if an object should be removed from the
             * service level; if it exists in ANY service instance, it cannot be removed from the
             * service level
             */
            final Map<String, IValue>[] objectsForEachServiceInstanceOfThisService =
                getObjectsForEachServiceInstanceOfThisServiceName(serviceFamily,
                    servicesObjectCountField == IServiceRecordFields.RECORD_COUNT);
            if (objectsForEachServiceInstanceOfThisService != null)
            {

                /*
                 * here we work out if, for any removed objects in the atomic change, there are no
                 * more occurrences of the object across all the service instances and thus we can
                 * remove the object from the service (serviceObjects) record
                 */
                Map<String, IValue> objectsPerServiceInstance;
                boolean existsForOneInstance = false;
                String objectName = null;
                for (Iterator<Map.Entry<String, IValue>> it =
                    atomicChange.getRemovedEntries().entrySet().iterator(); it.hasNext();)
                {
                    objectName = it.next().getKey();
                    for (int i = 0; i < objectsForEachServiceInstanceOfThisService.length; i++)
                    {
                        objectsPerServiceInstance = objectsForEachServiceInstanceOfThisService[i];
                        existsForOneInstance = objectsPerServiceInstance.containsKey(objectName);
                        if (existsForOneInstance)
                        {
                            break;
                        }
                    }
                    if (!existsForOneInstance)
                    {
                        serviceObjects.remove(objectName);
                    }
                }

                /*
                 * some objects need to show the aggregation across all service instances (e.g.
                 * subscription counts for a record, we need to see the counts for the same record
                 * across all service instances)
                 */
                if (aggregateValuesAsLongs)
                {
                    final Set<String> keysChanged = new HashSet<String>(atomicChange.getPutEntries().keySet());
                    keysChanged.addAll(atomicChange.getRemovedEntries().keySet());
                    Map<String, IValue> additions = new HashMap<String, IValue>();
                    IValue value;
                    for (Iterator<String> iterator = keysChanged.iterator(); iterator.hasNext();)
                    {
                        objectName = iterator.next();
                        additions.put(objectName, LongValue.valueOf(0));

                        // aggregate the long value of all objects of the same name across all
                        // instances of this service
                        for (int i = 0; i < objectsForEachServiceInstanceOfThisService.length; i++)
                        {
                            objectsPerServiceInstance = objectsForEachServiceInstanceOfThisService[i];
                            value = objectsPerServiceInstance.get(objectName);
                            if (value != null)
                            {
                                additions.put(objectName,
                                    LongValue.valueOf(additions.get(objectName).longValue() + value.longValue()));
                            }
                        }

                        if (additions.get(objectName).longValue() == 0
                            && atomicChange.getRemovedEntries().containsKey(objectName))
                        {
                            additions.remove(objectName);
                        }
                    }
                    serviceObjects.putAll(additions);
                }
                else
                {
                    serviceObjects.putAll(atomicChange.getPutEntries());
                }

                final int size = serviceObjects.size();
                if (size == 0)
                {
                    serviceObjects.removeSubMap(serviceFamily);
                }
                publishTimed(serviceObjects);

                EventHandler.this.registry.serviceStats.getOrCreateSubMap(serviceFamily).put(servicesObjectCountField,
                    LongValue.valueOf(serviceObjects.size()));
                publishTimed(EventHandler.this.registry.serviceStats);
            }
        }
        catch (Exception e)
        {
            Log.log(this,
                "Could not handle change for '" + serviceInstanceId + "', " + ObjectUtils.safeToString(atomicChange),
                e);
        }
    }
    
    /**
     * Publishes the record at a point in time in the future (in seconds). If there is a pending
     * publish for the record then the current call will do nothing and use the pending publish.
     */
    private void publishTimed(final IRecord record)
    {
        // only time publish records that are not "service" oriented - this prevents service
        // detection issues occurring due to "aliasing" (i.e. on-off-on being seen as just on)
        boolean publishServiceRecordImmediately = record.getName().startsWith(IRegistryRecordNames.SERVICES, 0);
        if (publishServiceRecordImmediately)
        {
            final int size = record.size();
            if (this.servicesRecordSize.getAndSet(size) == size)
            {
                // if the SERVICES record size is unchanged, skip an immediate publish and schedule
                // a pending publish (otherwise we start pumping out updates due to record changes
                // in the service which causes the registry to become quite chatty on the network to
                // its agents - agents subscribe for the SERVICES record by default) 
                publishServiceRecordImmediately = false;
            }
        }
        if (publishServiceRecordImmediately
            || record.getName().startsWith(IRegistryRecordNames.SERVICE_INSTANCES_PER_SERVICE_FAMILY, 0))
        {
            this.registry.context.publishAtomicChange(record);
        }
        else
        {
            synchronized (this.pendingPublish)
            {
                if (this.pendingPublish.add(record.getName()))
                {
                    this.publishExecutor.schedule(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            synchronized (EventHandler.this.pendingPublish)
                            {
                                EventHandler.this.pendingPublish.remove(record.getName());
                            }
                            EventHandler.this.registry.context.publishAtomicChange(record);
                        }
                    }, PlatformCoreProperties.Values.REGISTRY_RECORD_PUBLISH_PERIOD_SECS, TimeUnit.SECONDS);
                }
            }
        }
    }

    private boolean serviceInstanceNotRegistered(final String serviceFamily, String serviceMember)
    {
        return !this.registry.services.containsKey(serviceFamily)
            || (this.registry.serviceInstancesPerServiceFamily.getSubMapKeys().contains(serviceFamily)
                && !this.registry.serviceInstancesPerServiceFamily.getOrCreateSubMap(serviceFamily).containsKey(
                    serviceMember));
    }

    private void removeServiceStats(String serviceInstanceId)
    {
        this.registry.serviceInstanceStats.removeSubMap(serviceInstanceId);
        publishTimed(this.registry.serviceInstanceStats);
    }

    private void removeRecordsAndRpcsPerServiceInstance(String serviceInstanceId, final String serviceFamily)
    {
        // remove the records for this service instance
        final IRecord recordsPerServiceInstance = getRecordsPerServiceInstance(serviceInstanceId);
        AtomicChange removeAllAtomicChange = new AtomicChange(serviceInstanceId, ContextUtils.EMPTY_MAP,
            ContextUtils.EMPTY_MAP, new HashMap<String, IValue>(recordsPerServiceInstance));

        handleChangeForObjectsPerServiceAndInstance(serviceFamily, serviceInstanceId, removeAllAtomicChange,
            recordsPerServiceInstance, getRecordsPerServiceFamily(serviceFamily), true,
            IServiceRecordFields.RECORD_COUNT);

        this.registry.context.removeRecord(recordsPerServiceInstance.getName());

        // remove the rpcs for this service instance
        final IRecord rpcsPerServiceInstance = getRpcsPerServiceInstance(serviceInstanceId);
        removeAllAtomicChange = new AtomicChange(serviceInstanceId, ContextUtils.EMPTY_MAP, ContextUtils.EMPTY_MAP,
            new HashMap<String, IValue>(rpcsPerServiceInstance));

        handleChangeForObjectsPerServiceAndInstance(serviceFamily, serviceInstanceId, removeAllAtomicChange,
            rpcsPerServiceInstance, getRpcsPerServiceFamily(serviceFamily), false, IServiceRecordFields.RPC_COUNT);

        this.registry.context.removeRecord(rpcsPerServiceInstance.getName());
    }

    private void registerStep5_registerListenersForServiceInstance(final String serviceFamily,
        final String serviceMember, final String serviceInstanceId, final ProxyContext serviceProxy)
    {
        // add a listener to get the service-level statistics
        serviceProxy.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(final IRecord imageCopy, IRecordChange atomicChange)
            {
                EventHandler.this.execute(new IDescriptiveRunnable()
                {
                    @Override
                    public String getDescription()
                    {
                        return "handle service stats record change: " + serviceProxy.getName();
                    }

                    @Override
                    public Object context()
                    {
                        return serviceFamily;
                    }

                    @Override
                    public void run()
                    {
                        if (serviceInstanceNotRegistered(serviceFamily, serviceMember))
                        {
                            removeServiceStats(serviceInstanceId);
                        }
                        else
                        {
                            final Map<String, IValue> statsForService =
                                EventHandler.this.registry.serviceInstanceStats.getOrCreateSubMap(serviceInstanceId);
                            statsForService.putAll(imageCopy);
                            publishTimed(EventHandler.this.registry.serviceInstanceStats);
                        }
                    }
                });
            }
        }, PlatformServiceInstance.SERVICE_STATS_RECORD_NAME);

        // add a listener to cache the context connections record of the service locally in
        // the platformConnections record
        serviceProxy.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, final IRecordChange atomicChange)
            {
                EventHandler.this.execute(new IDescriptiveRunnable()
                {
                    @Override
                    public String getDescription()
                    {
                        return "handleConnectionsUpdate: " + serviceProxy.getName();
                    }

                    @Override
                    public Object context()
                    {
                        return serviceFamily;
                    }

                    @Override
                    public void run()
                    {
                        handleConnectionsUpdate_callInFamilyScope(atomicChange, serviceFamily, serviceMember);
                    }
                });
            }
        }, REMOTE_CONTEXT_CONNECTIONS);

        // add listeners to handle platform objects published by this instance
        serviceProxy.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, final IRecordChange atomicChange)
            {
                EventHandler.this.execute(new IDescriptiveRunnable()
                {
                    @Override
                    public String getDescription()
                    {
                        return "handle RemoteContextRecords record change: " + serviceProxy.getName();
                    }

                    @Override
                    public Object context()
                    {
                        return serviceFamily;
                    }

                    @Override
                    public void run()
                    {
                        if (serviceInstanceNotRegistered(serviceFamily, serviceMember))
                        {
                            removeRecordsAndRpcsPerServiceInstance(serviceInstanceId, serviceFamily);
                        }
                        else
                        {
                            final IRecord serviceInstanceObjectsRecord =
                                getRecordsPerServiceInstance(serviceInstanceId);
                            final IRecord serviceObjectsRecord = getRecordsPerServiceFamily(serviceFamily);
                            handleChangeForObjectsPerServiceAndInstance(serviceFamily, serviceInstanceId, atomicChange,
                                serviceInstanceObjectsRecord, serviceObjectsRecord, true,
                                IServiceRecordFields.RECORD_COUNT);
                        }
                    }
                });
            }
        }, REMOTE_CONTEXT_RECORDS);

        serviceProxy.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, final IRecordChange atomicChange)
            {
                EventHandler.this.execute(new IDescriptiveRunnable()
                {
                    @Override
                    public String getDescription()
                    {
                        return "handle RemoteContextRpcs record change: " + serviceProxy.getName();
                    }

                    @Override
                    public Object context()
                    {
                        return serviceFamily;
                    }

                    @Override
                    public void run()
                    {
                        if (serviceInstanceNotRegistered(serviceFamily, serviceMember))
                        {
                            removeRecordsAndRpcsPerServiceInstance(serviceInstanceId, serviceFamily);
                        }
                        else
                        {
                            final IRecord serviceInstanceObjectsRecord = getRpcsPerServiceInstance(serviceInstanceId);
                            final IRecord serviceObjectsRecord = getRpcsPerServiceFamily(serviceFamily);
                            handleChangeForObjectsPerServiceAndInstance(serviceFamily, serviceInstanceId, atomicChange,
                                serviceInstanceObjectsRecord, serviceObjectsRecord, false,
                                IServiceRecordFields.RPC_COUNT);
                        }
                    }
                });
            }
        }, REMOTE_CONTEXT_RPCS);
        // remove the NOOP observer as we have a real listener attached to the RPC record now
        serviceProxy.removeObserver(NOOP_OBSERVER, REMOTE_CONTEXT_RPCS);
    }

    @SuppressWarnings("unchecked")
    private Map<String, IValue>[] getObjectsForEachServiceInstanceOfThisServiceName(final String serviceFamily,
        boolean objectsAreRecords)
    {
        if (this.registry.serviceInstancesPerServiceFamily.getSubMapKeys().contains(serviceFamily))
        {
            final Map<String, IValue> serviceMembersForThisService =
                this.registry.serviceInstancesPerServiceFamily.getOrCreateSubMap(serviceFamily);

            final String[] serviceInstancesNamesForThisServiceArray =
                serviceMembersForThisService.keySet().toArray(new String[serviceMembersForThisService.keySet().size()]);

            final List<Map<String, IValue>> objectsForAllServiceInstancesOfThisService =
                new ArrayList<Map<String, IValue>>(serviceInstancesNamesForThisServiceArray.length);
            String serviceInstanceId;
            for (int i = 0; i < serviceInstancesNamesForThisServiceArray.length; i++)
            {
                serviceInstanceId = PlatformUtils.composePlatformServiceInstanceID(serviceFamily,
                    serviceInstancesNamesForThisServiceArray[i]);
                objectsForAllServiceInstancesOfThisService.add(objectsAreRecords
                    ? getRecordsPerServiceInstance(serviceInstanceId) : getRpcsPerServiceInstance(serviceInstanceId));
            }
            return objectsForAllServiceInstancesOfThisService.toArray(
                new Map[objectsForAllServiceInstancesOfThisService.size()]);
        }
        else
        {
            return null;
        }
    }

    private void handleRegistryRecordsUpdate(final IRecordChange atomicChange)
    {
        atomicChange.applyTo(getRecordsPerServiceFamily(PlatformRegistry.SERVICE_NAME));
        publishTimed(getRecordsPerServiceFamily(PlatformRegistry.SERVICE_NAME));

        final String registryInstanceName =
            PlatformUtils.composePlatformServiceInstanceID(PlatformRegistry.SERVICE_NAME, this.registry.platformName);
        atomicChange.applyTo(getRecordsPerServiceInstance(registryInstanceName));
        publishTimed(getRecordsPerServiceInstance(registryInstanceName));

        final Map<String, IValue> registryServiceSubMap =
            this.registry.serviceStats.getOrCreateSubMap(PlatformRegistry.SERVICE_NAME);
        final LongValue recordCounts =
            LongValue.valueOf(this.registry.context.getRecord(ISystemRecordNames.CONTEXT_RECORDS).size());
        if (!recordCounts.equals(registryServiceSubMap.put(IServiceRecordFields.RECORD_COUNT, recordCounts)))
        {
            publishTimed(this.registry.serviceStats);
        }
    }

    IRecord getRecordsPerServiceFamily(String serviceName)
    {
        return this.registry.context.getOrCreateRecord(IRegistryRecordNames.PREFIX_RECORDS_PER_SERVICE + serviceName);
    }

    IRecord getRecordsPerServiceInstance(String serviceInstanceId)
    {
        return this.registry.context.getOrCreateRecord(
            IRegistryRecordNames.PREFIX_RECORDS_PER_INSTANCE + serviceInstanceId);
    }

    IRecord getRpcsPerServiceFamily(String serviceName)
    {
        return this.registry.context.getOrCreateRecord(IRegistryRecordNames.PREFIX_RPCS_PER_SERVICE + serviceName);
    }

    IRecord getRpcsPerServiceInstance(String serviceInstanceId)
    {
        return this.registry.context.getOrCreateRecord(
            IRegistryRecordNames.PREFIX_RPCS_PER_INSTANCE + serviceInstanceId);
    }

    private void logExceptionAndDeregister_familyScope(final RegistrationToken registrationToken,
        final String serviceInstanceId, Exception e)
    {
        Log.log(EventHandler.this, "*** Error registering " + registrationToken + ", deregistering...", e);

        deregisterPlatformServiceInstance_callInFamilyScope(registrationToken, serviceInstanceId);
    }

    private void logExceptionAndExecuteDeregister(final RegistrationToken registrationToken, final String serviceFamily,
        final String serviceInstanceId, Exception e, final String reason)
    {
        Log.log(EventHandler.this, "*** " + reason + " " + registrationToken + ", deregistering...", e);

        executeDeregisterPlatformServiceInstance(registrationToken, serviceFamily, serviceInstanceId, reason);
    }

}

/**
 * Thrown when a service attempts a re-registration when it is still active
 * 
 * @author Ramon Servadei
 */
final class AlreadyRegisteredException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    final String serviceInstanceId;
    final String agentName;
    final String nodeName;
    final long port;
    final String redundancyMode;

    AlreadyRegisteredException(String serviceInstanceId, String agentName, String nodeName, long port,
        String redundancyMode)
    {
        super("[agentName=" + agentName + ", serviceInstanceId=" + serviceInstanceId + ", nodeName=" + nodeName
            + ", port=" + port + ", redundancyMode=" + redundancyMode + "]");

        this.serviceInstanceId = serviceInstanceId;
        this.agentName = agentName;
        this.nodeName = nodeName;
        this.port = port;
        this.redundancyMode = redundancyMode;
    }
}

/**
 * A token for recording distinct registration of a platform service instance.
 * 
 * @author Ramon Servadei
 */
final class RegistrationToken implements Serializable
{
    private static final long serialVersionUID = 1L;
    final Serializable token;
    final String serviceInstanceId;
    final int hashCode;

    RegistrationToken(Serializable token, String serviceInstanceId)
    {
        super();
        this.token = token;
        this.serviceInstanceId = serviceInstanceId;
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.token == null) ? 0 : this.token.hashCode());
        result = prime * result + ((this.serviceInstanceId == null) ? 0 : this.serviceInstanceId.hashCode());
        this.hashCode = result;
    }

    @Override
    public int hashCode()
    {
        return this.hashCode;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (is.same(this, obj))
        {
            return true;
        }
        if (is.differentClass(this, obj))
        {
            return false;
        }
        RegistrationToken other = (RegistrationToken) obj;
        return is.eq(this.token, other.token) && is.eq(this.serviceInstanceId, other.serviceInstanceId);
    }

    @Override
    public String toString()
    {
        return "[" + this.token + ", " + this.serviceInstanceId + "]";
    }

    Serializable getToken()
    {
        return this.token;
    }

    String getServiceInstanceId()
    {
        return this.serviceInstanceId;
    }

}