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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.EndPointAddress;
import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.core.PlatformRegistry.IPlatformSummaryRecordFields;
import com.fimtra.clearconnect.core.PlatformRegistry.IRuntimeStatusRecordFields;
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
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.core.Publisher;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.datafission.core.StringProtocolCodec;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.thimble.ICoalescingRunnable;
import com.fimtra.thimble.ThimbleExecutor;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SystemUtils;
import com.fimtra.util.ThreadUtils;
import com.fimtra.util.is;

/**
 * A service that maintains a registry of all other services in the platform. This should only be
 * accessed via a {@link PlatformRegistryAgent}.
 * <p>
 * The registry maintains a proxy to every service that is registered with it - this allows the
 * registry to keep a live view of what services are available.
 * <p>
 * The registry service uses a string wire-protocol
 * 
 * @see IPlatformRegistryAgent
 * @author Ramon Servadei, Paul Mackinlay
 */
public final class PlatformRegistry
{
    static final IValue BLANK_VALUE = new TextValue("");
    static final String GET_SERVICE_INFO_RECORD_NAME_FOR_SERVICE = "getServiceInfoForService";
    static final String GET_HEARTBEAT_CONFIG = "getHeartbeatConfig";
    static final String GET_PLATFORM_NAME = "getPlatformName";
    static final String DEREGISTER = "deregister";
    static final String REGISTER = "register";
    static final String RUNTIME_STATIC = "runtimeStatic";
    static final String RUNTIME_DYNAMIC = "runtimeDynamic";
    static final String AGENT_PROXY_ID_PREFIX = PlatformRegistry.SERVICE_NAME + PlatformUtils.SERVICE_CLIENT_DELIMITER;
    static final int AGENT_PROXY_ID_PREFIX_LEN = AGENT_PROXY_ID_PREFIX.length();

    // suppress logging of the the runtimeDynamic RPC inbound commands
    static
    {
        String current =
            (String) System.getProperties().get(DataFissionProperties.Names.IGNORE_LOGGING_RX_COMMANDS_WITH_PREFIX);
        final String ignoreRxRpc = "rpc|" + RUNTIME_DYNAMIC;
        if (current == null)
        {
            current = ignoreRxRpc;
        }
        else
        {
            current += "," + ignoreRxRpc;
        }
        Log.log(PlatformRegistry.class, "Setting ", DataFissionProperties.Names.IGNORE_LOGGING_RX_COMMANDS_WITH_PREFIX,
            "=", current);
        System.getProperties().setProperty(DataFissionProperties.Names.IGNORE_LOGGING_RX_COMMANDS_WITH_PREFIX, current);
    }

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
     * </pre>
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

    static final StringProtocolCodec CODEC = new StringProtocolCodec();

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
        String EPM = "EPM";
        String UPTIME_SECS = "Uptime";
    }

    static interface IPlatformSummaryRecordFields
    {
        String NODES = "Nodes";
        String SERVICES = "Services";
        String SERVICE_INSTANCES = "ServiceInstances";
        String CONNECTIONS = "Connections";
    }

    public static final String SERVICE_NAME = "PlatformRegistry";

    /**
     * Exposes the record names of internal records used in a registry service.
     * 
     * @author Ramon Servadei
     */
    static interface IRegistryRecordNames
    {

        /**
         * <pre>
         * key: serviceFamily, value: redundancy mode of the service (string value of {@link RedundancyModeEnum})
         * </pre>
         * 
         * Agents subscribe for this to have a live view of the services that exist, each service
         * identified by its service family name
         */
        String SERVICES = "Services";

        /**
         * The service instances per service family on the platform
         * 
         * <pre>
         * sub-map key: serviceFamily
         * sub-map structure: {key=service member name (NOT the service instance ID), value=system time when registered/last used}
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
         * sub-map structure: {key=statistic attribute, value=attribute value}
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
         * The records per service family on the platform
         * 
         * <pre>
         * sub-map key: serviceFamily
         * sub-map structure: {key=record name, value=number of subscriptions across all service instances}
         * </pre>
         */
        String RECORDS_PER_SERVICE_FAMILY = "Records Per Service Family";

        /**
         * The records per service instance on the platform
         * 
         * <pre>
         * sub-map key: serviceInstanceID (i.e. 'service[serviceInstance]' )
         * sub-map structure: {key=record name, value=number of subscriptions in this service instance}
         * </pre>
         */
        String RECORDS_PER_SERVICE_INSTANCE = "Records Per Service Instance";

        /**
         * The RPCs per service family on the platform
         * 
         * <pre>
         * sub-map key: serviceFamily
         * sub-map structure: {key=rpc name, value=rpc specification}
         * </pre>
         */
        String RPCS_PER_SERVICE_FAMILY = "RPCS Per Service Family";

        /**
         * The RPCs per service instance on the platform
         * 
         * <pre>
         * sub-map key: serviceInstanceID (i.e. 'service[serviceInstance]' )
         * sub-map structure: {key=rpc name, value=rpc specification}
         * </pre>
         */
        String RPCS_PER_SERVICE_INSTANCE = "RPCS Per Service Instance";

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
    final ConcurrentMap<String, ProxyContext> monitoredServiceInstances;
    /** Key=service family, Value=master service instance ID */
    final ConcurrentMap<String, String> masterInstancePerFtService;
    /** @see IRegistryRecordNames#SERVICES */
    final IRecord services;
    /** @see IRegistryRecordNames#SERVICE_INSTANCES_PER_SERVICE_FAMILY */
    final IRecord serviceInstancesPerServiceFamily;
    /** @See {@link IRegistryRecordNames#SERVICE_INSTANCES_PER_AGENT  */
    final IRecord serviceInstancesPerAgent;
    /** @See {@link IRegistryRecordNames#SERVICE_INSTANCE_STATS  */
    final IRecord serviceInstanceStats;
    /** @see IRegistryRecordNames#PLATFORM_CONNECTIONS */
    final IRecord platformConnections;
    /** @see IRegistryRecordNames#RECORDS_PER_SERVICE_INSTANCE */
    final IRecord recordsPerServiceInstance;
    /** @see IRegistryRecordNames#RPCS_PER_SERVICE_INSTANCE */
    final IRecord rpcsPerServiceInstance;
    /** @see IRegistryRecordNames#RECORDS_PER_SERVICE_FAMILY */
    final IRecord recordsPerServiceFamily;
    /** @see IRegistryRecordNames#RPCS_PER_SERVICE_FAMILY */
    final IRecord rpcsPerServiceFamily;
    /** @see IRegistryRecordNames#RUNTIME_STATUS */
    final IRecord runtimeStatus;
    /** @see IRegistryRecordNames#PLATFORM_SUMMARY */
    final IRecord platformSummary;

    /**
     * Tracks services that are pending registration completion
     * 
     * @see IRegistryRecordNames#SERVICES
     */
    final Map<String, IValue> pendingPlatformServices;
    final ThimbleExecutor coalescingExecutor;
    final EventHandler eventHandler;

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
        this.coalescingExecutor = new ThimbleExecutor("coalescing-eventExecutor-" + registryInstanceId, 1);
        this.eventHandler = new EventHandler(this);

        this.platformName = platformName;
        this.context = new Context(PlatformUtils.composeHostQualifiedName(SERVICE_NAME + "[" + platformName + "]"));
        this.publisher = new Publisher(this.context, CODEC, host, port);
        this.monitoredServiceInstances = new ConcurrentHashMap<String, ProxyContext>();
        this.masterInstancePerFtService = new ConcurrentHashMap<String, String>();
        this.pendingPlatformServices = new ConcurrentHashMap<String, IValue>();

        this.services = this.context.createRecord(IRegistryRecordNames.SERVICES);
        this.serviceInstancesPerServiceFamily =
            this.context.createRecord(IRegistryRecordNames.SERVICE_INSTANCES_PER_SERVICE_FAMILY);
        this.serviceInstancesPerAgent = this.context.createRecord(IRegistryRecordNames.SERVICE_INSTANCES_PER_AGENT);
        this.serviceInstanceStats = this.context.createRecord(IRegistryRecordNames.SERVICE_INSTANCE_STATS);
        this.platformConnections = this.context.createRecord(IRegistryRecordNames.PLATFORM_CONNECTIONS);
        this.recordsPerServiceInstance = this.context.createRecord(IRegistryRecordNames.RECORDS_PER_SERVICE_INSTANCE);
        this.rpcsPerServiceInstance = this.context.createRecord(IRegistryRecordNames.RPCS_PER_SERVICE_INSTANCE);
        this.recordsPerServiceFamily = this.context.createRecord(IRegistryRecordNames.RECORDS_PER_SERVICE_FAMILY);
        this.rpcsPerServiceFamily = this.context.createRecord(IRegistryRecordNames.RPCS_PER_SERVICE_FAMILY);
        this.runtimeStatus = this.context.createRecord(IRegistryRecordNames.RUNTIME_STATUS);
        this.platformSummary = this.context.createRecord(IRegistryRecordNames.PLATFORM_SUMMARY);

        // register the RegistryService as a service!
        this.services.put(SERVICE_NAME, RedundancyModeEnum.FAULT_TOLERANT.toString());
        this.serviceInstancesPerServiceFamily.getOrCreateSubMap(SERVICE_NAME).put(platformName,
            LongValue.valueOf(System.currentTimeMillis()));
        this.context.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
            {
                PlatformRegistry.this.eventHandler.executeHandleRecordsUpdate(atomicChange);
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
        final IRecordListener platformSummaryListener =
            new IRecordListener()
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
                PlatformRegistry.this.eventHandler.executeHandleConnectionsUpdate(atomicChange);
            }
        }, ISystemRecordNames.CONTEXT_CONNECTIONS), ISystemRecordNames.CONTEXT_CONNECTIONS);

        createGetServiceInfoRecordNameForServiceRpc();
        createGetHeartbeatConfigRpc();
        createGetPlatformNameRpc();
        createRegisterRpc();
        createDeregisterRpc();
        createRuntimeStaticRpc();
        createRuntimeDynamicRpc();

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this));
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
                        PlatformRegistry.this.eventHandler.executeSelectNextInstance(args[0].textValue());

                    return new TextValue(ServiceInfoRecordFields.SERVICE_INFO_RECORD_NAME_PREFIX + nextInstance);
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
                return new TextValue(PlatformRegistry.this.platformName);
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
                return new TextValue(ChannelUtils.WATCHDOG.getHeartbeatPeriodMillis() + ":"
                    + ChannelUtils.WATCHDOG.getMissedHeartbeatCount());
            }
        });
        this.context.createRpc(getPlatformName);
    }

    private void createRegisterRpc()
    {
        // publish an RPC that allows registration
        // args: serviceFamily, objectWireProtocol, hostname, port, serviceMember,
        // redundancyMode, agentName
        final RpcInstance register =
            new RpcInstance(TypeEnum.TEXT, REGISTER, TypeEnum.TEXT, TypeEnum.TEXT, TypeEnum.TEXT, TypeEnum.LONG,
                TypeEnum.TEXT, TypeEnum.TEXT, TypeEnum.TEXT);
        register.setHandler(new IRpcExecutionHandler()
        {
            @SuppressWarnings({ "rawtypes", "unchecked" })
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

                if (serviceFamily.startsWith(PlatformRegistry.SERVICE_NAME))
                {
                    throw new ExecutionException("Cannot create service with reserved name '" + SERVICE_NAME + "'");
                }
                if (serviceMember.startsWith(PlatformRegistry.SERVICE_NAME))
                {
                    throw new ExecutionException("Cannot create service instance with reserved name '" + SERVICE_NAME
                        + "'");
                }

                final String serviceInstanceId =
                    PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember);
                final RedundancyModeEnum redundancyModeEnum = RedundancyModeEnum.valueOf(redundancyMode);
                final Map<String, IValue> serviceRecordStructure = new HashMap();
                serviceRecordStructure.put(ServiceInfoRecordFields.WIRE_PROTOCOL_FIELD, TextValue.valueOf(wireProtocol));
                serviceRecordStructure.put(ServiceInfoRecordFields.HOST_NAME_FIELD, TextValue.valueOf(host));
                serviceRecordStructure.put(ServiceInfoRecordFields.PORT_FIELD, LongValue.valueOf(port));
                serviceRecordStructure.put(ServiceInfoRecordFields.REDUNDANCY_MODE_FIELD,
                    TextValue.valueOf(redundancyMode));

                try
                {
                    PlatformRegistry.this.eventHandler.executeRegisterPlatformServiceInstance(serviceFamily,
                        redundancyMode, agentName, serviceInstanceId, redundancyModeEnum, serviceRecordStructure, args);
                }
                catch (Exception e)
                {
                    throw new ExecutionException(e);
                }

                return new TextValue("Registered " + serviceInstanceId);
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
                    PlatformRegistry.this.eventHandler.executeDeregisterPlatformServiceInstance(serviceInstanceId);
                }
                catch (Exception e)
                {
                    throw new ExecutionException(e);
                }

                return new TextValue("Deregistered " + serviceInstanceId);
            }
        });
        this.context.createRpc(deregister);
    }

    private void createRuntimeStaticRpc()
    {
        final String[] fields =
            new String[] { IRuntimeStatusRecordFields.RUNTIME_NAME, IRuntimeStatusRecordFields.RUNTIME_HOST,
                IRuntimeStatusRecordFields.RUNTIME, IRuntimeStatusRecordFields.USER,
                IRuntimeStatusRecordFields.CPU_COUNT };

        final RpcInstance runtimeStatus =
            new RpcInstance(TypeEnum.TEXT, RUNTIME_STATIC, fields, TypeEnum.TEXT, TypeEnum.TEXT, TypeEnum.TEXT,
                TypeEnum.TEXT, TypeEnum.LONG);

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
        final String[] fields =
            new String[] { IRuntimeStatusRecordFields.RUNTIME_NAME, IRuntimeStatusRecordFields.Q_OVERFLOW,
                IRuntimeStatusRecordFields.Q_TOTAL_SUBMITTED, IRuntimeStatusRecordFields.MEM_USED_MB,
                IRuntimeStatusRecordFields.MEM_AVAILABLE_MB, IRuntimeStatusRecordFields.THREAD_COUNT,
                IRuntimeStatusRecordFields.SYSTEM_LOAD, IRuntimeStatusRecordFields.EPM,
                IRuntimeStatusRecordFields.UPTIME_SECS };

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
        this.eventHandler.destroy();
        this.publisher.destroy();
        this.context.destroy();
        // NOTE: destroy proxies AFTER destroying the publisher and context - we don't want to tell
        // the agents that the services are de-registered - doing this when the publisher is dead
        // means we can't send any messages to the agents.
        ProxyContext proxy = null;
        for (String serviceInstanceId : this.monitoredServiceInstances.keySet())
        {
            try
            {
                proxy = this.monitoredServiceInstances.remove(serviceInstanceId);
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
 * This executes in a single thread context.
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings("synthetic-access")
final class EventHandler
{
    final PlatformRegistry registry;
    private final ScheduledExecutorService eventExecutor;

    EventHandler(final PlatformRegistry registry)
    {
        this.registry = registry;
        this.eventExecutor = ThreadUtils.newScheduledExecutorService("event-executor", 1);
    }

    void destroy()
    {
        this.eventExecutor.shutdown();
    }

    void executeRpcRuntimeDynamic(final IValue... args)
    {
        this.eventExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                handleRpcRuntimeDynamic(args);
            }
        });
    }

    void executeRpcRuntimeStatic(final IValue... args)
    {
        this.eventExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                handleRpcStaticRuntime(args);
            }
        });
    }

    Void executeRegisterPlatformServiceInstance(final String serviceFamily, final String redundancyMode,
        final String agentName, final String serviceInstanceId, final RedundancyModeEnum redundancyModeEnum,
        final Map<String, IValue> serviceRecordStructure, final IValue... args) throws InterruptedException,
        java.util.concurrent.ExecutionException
    {
        return this.eventExecutor.submit(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                return registerPlatformServiceInstance(serviceFamily, redundancyMode, agentName,
                    serviceRecordStructure, serviceInstanceId, redundancyModeEnum, args);
            }
        }).get();
    }

    void executeDeregisterPlatformServiceInstance(final String serviceInstanceId)
    {
        this.eventExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                deregisterPlatformServiceInstance(serviceInstanceId);
            }
        });
    }

    void executeHandleConnectionsUpdate(final IRecordChange atomicChange)
    {
        this.eventExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                handleConnectionsUpdate(atomicChange);
            }
        });
    }

    void executeHandleRecordsUpdate(final IRecordChange atomicChange)
    {
        this.eventExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                handleRecordsUpdate(atomicChange);
            }
        });
    }

    String executeSelectNextInstance(final String serviceFamily) throws InterruptedException,
        java.util.concurrent.ExecutionException
    {
        final String nextInstance = this.eventExecutor.submit(new Callable<String>()
        {
            @Override
            public String call() throws Exception
            {
                return selectNextInstance(serviceFamily);
            }
        }).get();
        return nextInstance;
    }

    void executeComputePlatformSummary()
    {
        this.eventExecutor.execute(new Runnable()
        {
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
        this.registry.context.publishAtomicChange(this.registry.platformSummary);
    }

    private String selectNextInstance(final String serviceFamily)
    {
        String activeServiceMemberName = null;
        final IValue iValue = this.registry.services.get(serviceFamily);
        if (iValue == null)
        {
            return null;
        }
        RedundancyModeEnum redundancyModeEnum = RedundancyModeEnum.valueOf(iValue.textValue());
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

                final String serviceInstanceId =
                    PlatformUtils.composePlatformServiceInstanceID(serviceFamily, activeServiceMemberName);
                if (redundancyModeEnum == RedundancyModeEnum.LOAD_BALANCED)
                {
                    /*
                     * for LB, the timestamp is updated to be the last selected time so the next
                     * instance selected will be one with an earlier time and thus produce a
                     * round-robin style selection policy
                     */
                    serviceInstances.put(activeServiceMemberName, LongValue.valueOf(System.currentTimeMillis()));
                }
                else
                {
                    verifyMasterInstance(serviceFamily, serviceInstanceId);
                }

                Log.log(
                    this.registry,
                    "Selecting member '",
                    activeServiceMemberName,
                    "' for service '",
                    serviceFamily,
                    "' (service info details=",
                    ObjectUtils.safeToString(this.registry.context.getRecord(PlatformRegistry.ServiceInfoRecordFields.SERVICE_INFO_RECORD_NAME_PREFIX
                        + serviceInstanceId)), ")");
                return serviceInstanceId;
            }
        }
        return null;
    }

    private void deregisterPlatformServiceInstance(final String serviceInstanceId)
    {
        final String[] serviceParts = PlatformUtils.decomposePlatformServiceInstanceID(serviceInstanceId);
        final String serviceFamily = serviceParts[0];
        final String serviceMember = serviceParts[1];
        ProxyContext proxy = this.registry.monitoredServiceInstances.remove(serviceInstanceId);
        if (proxy != null)
        {
            Log.log(this.registry, "Deregistering service instance ", serviceInstanceId, " (monitored with ",
                proxy.getChannelString(), ")");
            proxy.destroy();

            // remove the service instance info record
            this.registry.context.removeRecord(PlatformRegistry.ServiceInfoRecordFields.SERVICE_INFO_RECORD_NAME_PREFIX
                + serviceInstanceId);

            // remove connections - we need to scan the entire platform connections to find
            // matching connections from the publisher that is now dead
            for (String connection : this.registry.platformConnections.getSubMapKeys())
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
            this.registry.context.publishAtomicChange(this.registry.platformConnections);

            removeRecordsAndRpcsPerServiceInstance(serviceInstanceId, serviceFamily);

            // remove the service instance from the instances-per-service
            final Map<String, IValue> serviceInstances =
                this.registry.serviceInstancesPerServiceFamily.getOrCreateSubMap(serviceFamily);
            serviceInstances.remove(serviceMember);
            if (serviceInstances.size() == 0)
            {
                this.registry.serviceInstancesPerServiceFamily.removeSubMap(serviceFamily);
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
                    Log.log(this.registry, "Removing service '", serviceFamily, "' from registry");
                    this.registry.masterInstancePerFtService.remove(serviceFamily);
                    this.registry.context.publishAtomicChange(this.registry.services);
                }
            }
            else
            {
                if (isFaultTolerantPlatformService(serviceFamily))
                {
                    // this will verify the current master of the FT service
                    selectNextInstance(serviceFamily);
                }
            }
            this.registry.context.publishAtomicChange(this.registry.serviceInstancesPerServiceFamily);
            this.registry.context.publishAtomicChange(this.registry.serviceInstancesPerAgent);

            removeServiceStats(serviceInstanceId);
        }
    }

    @SuppressWarnings("unused")
    private Void registerPlatformServiceInstance(final String serviceFamily, final String redundancyMode,
        final String agentName, final Map<String, IValue> serviceRecordStructure, final String serviceInstanceId,
        final RedundancyModeEnum redundancyModeEnum, final IValue... args) throws ExecutionException
    {
        if (this.registry.monitoredServiceInstances.containsKey(serviceInstanceId))
        {
            throw new IllegalStateException("Already registered: " + serviceInstanceId);
        }
        switch(redundancyModeEnum)
        {
            case FAULT_TOLERANT:
                if (isLoadBalancedPlatformService(serviceFamily))
                {
                    throw new IllegalArgumentException("Platform service '" + serviceFamily
                        + "' is already registered as load-balanced.");
                }
                break;
            case LOAD_BALANCED:
                if (isFaultTolerantPlatformService(serviceFamily))
                {
                    throw new IllegalArgumentException("Platform service '" + serviceFamily
                        + "' is already registered as fault-tolerant.");
                }
                break;
            default :
                throw new IllegalArgumentException("Unhandled mode '" + redundancyMode + "' for service '"
                    + serviceFamily + "'");
        }
        final ProxyContext serviceProxy;
        try
        {
            serviceProxy =
                new ProxyContext(PlatformUtils.composeProxyName(serviceInstanceId, this.registry.context.getName()),
                    PlatformUtils.getCodecFromServiceInfoRecord(serviceRecordStructure),
                    PlatformUtils.getHostNameFromServiceInfoRecord(serviceRecordStructure),
                    PlatformUtils.getPortFromServiceInfoRecord(serviceRecordStructure));
        }
        catch (IOException e)
        {
            Log.log(this.registry,
                "Could not construct service proxy with connection settings RPC args " + Arrays.toString(args), e);
            throw new ExecutionException("Could not register");
        }
        this.registry.pendingPlatformServices.put(serviceFamily, TextValue.valueOf(redundancyModeEnum.name()));
        this.registry.monitoredServiceInstances.put(serviceInstanceId, serviceProxy);
        serviceProxy.setReconnectPeriodMillis(this.registry.reconnectPeriodMillis);
        new PlatformServiceConnectionMonitor(serviceProxy, serviceInstanceId)
        {
            @Override
            protected void onPlatformServiceDisconnected()
            {
                EventHandler.this.eventExecutor.execute(new Runnable()
                {
                    @SuppressWarnings("unqualified-field-access")
                    @Override
                    public void run()
                    {
                        deregisterPlatformServiceInstance(serviceInstanceId);
                    }
                });
            }

            @Override
            protected void onPlatformServiceConnected()
            {
                EventHandler.this.eventExecutor.execute(new Runnable()
                {
                    @SuppressWarnings("unqualified-field-access")
                    @Override
                    public void run()
                    {
                        try
                        {
                            // register the service when the monitoring of the
                            // service is connected
                            registerPlatformServiceInstance(agentName, serviceInstanceId, serviceRecordStructure,
                                redundancyModeEnum);

                            Log.log(EventHandler.this.registry, "Registered ", redundancyMode, " service ",
                                serviceInstanceId, " (monitoring with " + serviceProxy.getChannelString(), ")");
                        }
                        catch (Exception e)
                        {
                            Log.log(EventHandler.this.registry, "Error registering service " + serviceInstanceId, e);
                            try
                            {
                                deregisterPlatformServiceInstance(serviceInstanceId);
                            }
                            catch (Exception e2)
                            {
                                Log.log(EventHandler.this.registry, "Error deregistering service " + serviceInstanceId,
                                    e2);
                            }
                        }
                    }
                });
            }
        };
        return null;
    }

    private void handleRpcStaticRuntime(final IValue... args)
    {
        final String agentName = args[0].textValue();
        Map<String, IValue> runtimeRecord = this.registry.runtimeStatus.getOrCreateSubMap(agentName);
        runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.RUNTIME_HOST, args[1]);
        runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.RUNTIME, args[2]);
        runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.USER, args[3]);
        runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.CPU_COUNT, args[4]);
        this.registry.context.publishAtomicChange(this.registry.runtimeStatus);
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
            runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.EPM, args[7]);
            runtimeRecord.put(PlatformRegistry.IRuntimeStatusRecordFields.UPTIME_SECS, args[8]);
            this.registry.context.publishAtomicChange(this.registry.runtimeStatus);
        }
    }

    private void handleConnectionsUpdate(final IRecordChange atomicChange)
    {
        IValue proxyId;
        String agent = null;
        IRecordChange subMapAtomicChange;
        Map<String, IValue> connection;
        for (String connectionId : atomicChange.getSubMapKeys())
        {
            subMapAtomicChange = atomicChange.getSubMapAtomicChange(connectionId);
            if ((proxyId =
                subMapAtomicChange.getRemovedEntries().get(ISystemRecordNames.IContextConnectionsRecordFields.PROXY_ID)) != null)
            {
                if (proxyId.textValue().startsWith(PlatformRegistry.AGENT_PROXY_ID_PREFIX))
                {
                    agent = proxyId.textValue().substring(PlatformRegistry.AGENT_PROXY_ID_PREFIX_LEN);
                    // purge the runtimeStatus record
                    this.registry.runtimeStatus.removeSubMap(agent);
                }
            }
            connection = this.registry.platformConnections.getOrCreateSubMap(connectionId);
            subMapAtomicChange.applyTo(connection);
            if (connection.isEmpty())
            {
                // purge the connection
                this.registry.platformConnections.removeSubMap(connectionId);
            }
        }
        this.registry.context.publishAtomicChange(this.registry.runtimeStatus);
        this.registry.context.publishAtomicChange(this.registry.platformConnections);
    }

    private void verifyMasterInstance(String serviceFamily, String activeServiceInstanceId)
    {
        final String previousMasterInstance;
        if (!is.eq(
            (previousMasterInstance =
                this.registry.masterInstancePerFtService.put(serviceFamily, activeServiceInstanceId)),
            activeServiceInstanceId))
        {
            if (previousMasterInstance != null)
            {
                callFtServiceStatusRpc(previousMasterInstance, false);
            }
            callFtServiceStatusRpc(activeServiceInstanceId, true);
        }
    }

    private void callFtServiceStatusRpc(String activeServiceInstanceId, boolean active)
    {
        try
        {
            final ProxyContext proxyContext = this.registry.monitoredServiceInstances.get(activeServiceInstanceId);
            ContextUtils.getRpc(proxyContext, proxyContext.getReconnectPeriodMillis(),
                PlatformServiceInstance.RPC_FT_SERVICE_STATUS).executeNoResponse(
                TextValue.valueOf(Boolean.valueOf(active).toString()));
        }
        catch (Exception e)
        {
            Log.log(this.registry, "Could not call RPC to " + (active ? "activate" : "deactivate OLD")
                + " master service: " + activeServiceInstanceId, e);
        }
    }

    private boolean isLoadBalancedPlatformService(final String serviceFamily)
    {
        IValue iValue = this.registry.pendingPlatformServices.get(serviceFamily);
        if (iValue != null)
        {
            return RedundancyModeEnum.valueOf(iValue.textValue()) == RedundancyModeEnum.LOAD_BALANCED;
        }
        // check registered services
        iValue = this.registry.services.get(serviceFamily);
        if (iValue == null)
        {
            return false;
        }
        return RedundancyModeEnum.valueOf(iValue.textValue()) == RedundancyModeEnum.LOAD_BALANCED;
    }

    private boolean isFaultTolerantPlatformService(final String serviceFamily)
    {
        IValue iValue = this.registry.pendingPlatformServices.get(serviceFamily);
        if (iValue != null)
        {
            return RedundancyModeEnum.valueOf(iValue.textValue()) == RedundancyModeEnum.FAULT_TOLERANT;
        }
        // check registered services
        iValue = this.registry.services.get(serviceFamily);
        if (iValue == null)
        {
            return false;
        }
        return RedundancyModeEnum.valueOf(iValue.textValue()) == RedundancyModeEnum.FAULT_TOLERANT;
    }

    private void handleChangeForObjectsPerServiceAndInstance(final String serviceFamily,
        final String serviceInstanceId, IRecordChange atomicChange,
        final IRecord objectsPerPlatformServiceInstanceRecord, final IRecord objectsPerPlatformServiceRecord,
        final boolean aggregateValuesAsLongs)
    {
        try
        {
            // first handle updates to the object record for the service instance
            Map<String, IValue> serviceInstanceObjects =
                objectsPerPlatformServiceInstanceRecord.getOrCreateSubMap(serviceInstanceId);
            atomicChange.applyTo(serviceInstanceObjects);
            if (serviceInstanceObjects.size() == 0)
            {
                objectsPerPlatformServiceInstanceRecord.removeSubMap(serviceInstanceId);
            }
            this.registry.context.publishAtomicChange(objectsPerPlatformServiceInstanceRecord);

            /*
             * build up an array of the objects for each service instance of this service, we need
             * all service instance objects to work out if an object should be removed from the
             * service level; if it exists in ANY service instance, it cannot be removed from the
             * service level
             */
            final Map<String, IValue>[] objectsForEachServiceInstanceOfThisService =
                getObjectsForEachServiceInstanceOfThisServiceName(serviceFamily,
                    objectsPerPlatformServiceInstanceRecord);
            if (objectsForEachServiceInstanceOfThisService != null)
            {

                /*
                 * here we work out if, for any removed objects in the atomic change, there are no
                 * more occurrences of the object across all the service instances and thus we can
                 * remove the object from the service (objects-per-service) record
                 */
                final Map<String, IValue> serviceObjects =
                    objectsPerPlatformServiceRecord.getOrCreateSubMap(serviceFamily);
                Map<String, IValue> objectsPerServiceInstance;
                boolean existsForOneInstance = false;
                String objectName = null;
                for (Iterator<Map.Entry<String, IValue>> it = atomicChange.getRemovedEntries().entrySet().iterator(); it.hasNext();)
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
                this.registry.context.publishAtomicChange(objectsPerPlatformServiceRecord);

                if (serviceObjects.size() == 0)
                {
                    objectsPerPlatformServiceRecord.removeSubMap(serviceFamily);
                }
            }
        }
        catch (Exception e)
        {
            Log.log(this.registry,
                "Could not handle change for " + serviceInstanceId + ", " + ObjectUtils.safeToString(atomicChange), e);
        }
    }

    private boolean serviceInstanceNotRegistered(final String serviceFamily, String serviceMember)
    {
        return !this.registry.services.containsKey(serviceFamily)
            || (this.registry.serviceInstancesPerServiceFamily.getSubMapKeys().contains(serviceFamily) && !this.registry.serviceInstancesPerServiceFamily.getOrCreateSubMap(
                serviceFamily).containsKey(serviceMember));
    }

    private void removeServiceStats(String serviceInstanceId)
    {
        this.registry.serviceInstanceStats.removeSubMap(serviceInstanceId);
        this.registry.context.publishAtomicChange(this.registry.serviceInstanceStats);
    }

    private void removeRecordsAndRpcsPerServiceInstance(String serviceInstanceId, final String serviceFamily)
    {
        // remove the records for this service instance
        handleChangeForObjectsPerServiceAndInstance(serviceFamily, serviceInstanceId, new AtomicChange(
            serviceInstanceId, ContextUtils.EMPTY_MAP, ContextUtils.EMPTY_MAP, new HashMap<String, IValue>(
                this.registry.recordsPerServiceInstance.getOrCreateSubMap(serviceInstanceId))),
            this.registry.recordsPerServiceInstance, this.registry.recordsPerServiceFamily, true);

        // remove the rpcs for this service instance
        handleChangeForObjectsPerServiceAndInstance(serviceFamily, serviceInstanceId, new AtomicChange(
            serviceInstanceId, ContextUtils.EMPTY_MAP, ContextUtils.EMPTY_MAP, new HashMap<String, IValue>(
                this.registry.rpcsPerServiceInstance.getOrCreateSubMap(serviceInstanceId))),
            this.registry.rpcsPerServiceInstance, this.registry.rpcsPerServiceFamily, false);
    }

    private void registerPlatformServiceInstance(final String agentName, String serviceInstanceId,
        final Map<String, IValue> serviceRecordStructure, final RedundancyModeEnum redundancyModeEnum)
    {
        final String[] serviceParts = PlatformUtils.decomposePlatformServiceInstanceID(serviceInstanceId);
        final String serviceFamily = serviceParts[0];
        final String serviceMember = serviceParts[1];

        if (redundancyModeEnum == RedundancyModeEnum.FAULT_TOLERANT)
        {
            if (this.registry.masterInstancePerFtService.get(serviceFamily) == null)
            {
                verifyMasterInstance(serviceFamily, serviceInstanceId);
            }
            else
            {
                callFtServiceStatusRpc(serviceInstanceId, false);
            }
        }

        this.registry.context.createRecord(PlatformRegistry.ServiceInfoRecordFields.SERVICE_INFO_RECORD_NAME_PREFIX
            + serviceInstanceId, serviceRecordStructure);

        // register the service member
        this.registry.serviceInstancesPerServiceFamily.getOrCreateSubMap(serviceFamily).put(serviceMember,
            LongValue.valueOf(System.currentTimeMillis()));
        this.registry.context.publishAtomicChange(this.registry.serviceInstancesPerServiceFamily);

        // register the service instance against the agent
        this.registry.serviceInstancesPerAgent.getOrCreateSubMap(agentName).put(serviceInstanceId,
            PlatformRegistry.BLANK_VALUE);
        this.registry.context.publishAtomicChange(this.registry.serviceInstancesPerAgent);

        this.registry.services.put(serviceFamily, redundancyModeEnum.name());
        // todo what if duplicate instances of the same family are registering simultaneously?
        this.registry.pendingPlatformServices.remove(serviceFamily);

        this.registry.context.publishAtomicChange(this.registry.services);

        ProxyContext serviceProxy = this.registry.monitoredServiceInstances.get(serviceInstanceId);
        registerListenersForServiceInstance(serviceFamily, serviceMember, serviceInstanceId, serviceProxy);
    }

    private void registerListenersForServiceInstance(final String serviceFamily, final String serviceMember,
        final String serviceInstanceId, final ProxyContext serviceProxy)
    {
        // add a listener to get the service-level statistics
        serviceProxy.addObserver(
            new CoalescingRecordListener(this.registry.coalescingExecutor, new IRecordListener()
            {
                @Override
                public void onChange(final IRecord imageCopy, IRecordChange atomicChange)
                {
                    EventHandler.this.eventExecutor.execute(new Runnable()
                    {
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
                                EventHandler.this.registry.context.publishAtomicChange(EventHandler.this.registry.serviceInstanceStats);
                            }
                        }
                    });
                }
            }, serviceInstanceId + "-" + PlatformServiceInstance.SERVICE_STATS_RECORD_NAME),
            PlatformServiceInstance.SERVICE_STATS_RECORD_NAME);

        // add a listener to cache the context connections record of the service locally in
        // the platformConnections record
        serviceProxy.addObserver(new CoalescingRecordListener(this.registry.coalescingExecutor, new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, final IRecordChange atomicChange)
            {
                EventHandler.this.eventExecutor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        handleConnectionsUpdate(atomicChange);
                    }
                });
            }
        }, serviceInstanceId + "-" + REMOTE_CONTEXT_CONNECTIONS), REMOTE_CONTEXT_CONNECTIONS);

        // add listeners to handle platform objects published by this instance
        serviceProxy.addObserver(new CoalescingRecordListener(this.registry.coalescingExecutor, new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, final IRecordChange atomicChange)
            {
                EventHandler.this.eventExecutor.execute(new Runnable()
                {
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
                                EventHandler.this.registry.recordsPerServiceInstance;
                            final IRecord serviceObjectsRecord = EventHandler.this.registry.recordsPerServiceFamily;
                            handleChangeForObjectsPerServiceAndInstance(serviceFamily, serviceInstanceId, atomicChange,
                                serviceInstanceObjectsRecord, serviceObjectsRecord, true);
                        }
                    }
                });
            }
        }, serviceInstanceId + "-" + REMOTE_CONTEXT_RECORDS), REMOTE_CONTEXT_RECORDS);

        serviceProxy.addObserver(new CoalescingRecordListener(this.registry.coalescingExecutor, new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, final IRecordChange atomicChange)
            {
                EventHandler.this.eventExecutor.execute(new Runnable()
                {
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
                                EventHandler.this.registry.rpcsPerServiceInstance;
                            final IRecord serviceObjectsRecord = EventHandler.this.registry.rpcsPerServiceFamily;
                            handleChangeForObjectsPerServiceAndInstance(serviceFamily, serviceInstanceId, atomicChange,
                                serviceInstanceObjectsRecord, serviceObjectsRecord, false);
                        }
                    }
                });
            }
        }, serviceInstanceId + "-" + REMOTE_CONTEXT_RPCS), REMOTE_CONTEXT_RPCS);
    }

    @SuppressWarnings("unchecked")
    private Map<String, IValue>[] getObjectsForEachServiceInstanceOfThisServiceName(final String serviceFamily,
        final IRecord objectsPerPlatformServiceInstanceRecord)
    {
        if (this.registry.serviceInstancesPerServiceFamily.getSubMapKeys().contains(serviceFamily))
        {
            final Map<String, IValue> serviceMembersForThisService =
                this.registry.serviceInstancesPerServiceFamily.getOrCreateSubMap(serviceFamily);

            final String[] serviceInstancesNamesForThisServiceArray =
                serviceMembersForThisService.keySet().toArray(new String[serviceMembersForThisService.keySet().size()]);

            // this block examines the subMapKeys (which are service instance IDs) and for all the
            // members of the serviceFamily, if the member is found in the subMapKeys, then the
            // associated objects (the submap) for the member is added to a return array
            final Set<String> allServiceInstanceIds = objectsPerPlatformServiceInstanceRecord.getSubMapKeys();
            final List<Map<String, IValue>> objectsForAllServiceInstancesOfThisService =
                new ArrayList<Map<String, IValue>>(serviceInstancesNamesForThisServiceArray.length);
            String serviceInstanceID;
            for (int i = 0; i < serviceInstancesNamesForThisServiceArray.length; i++)
            {
                serviceInstanceID =
                    PlatformUtils.composePlatformServiceInstanceID(serviceFamily,
                        serviceInstancesNamesForThisServiceArray[i]);
                if (allServiceInstanceIds.contains(serviceInstanceID))
                {
                    // Note: there is no leak than can occur from this call to getOrCreateSubMap
                    // because the serviceInstanceID is checked for existence in the
                    // allServiceInstanceIds set, which are the submap keys
                    objectsForAllServiceInstancesOfThisService.add(objectsPerPlatformServiceInstanceRecord.getOrCreateSubMap(serviceInstanceID));
                }
            }
            return objectsForAllServiceInstancesOfThisService.toArray(new Map[objectsForAllServiceInstancesOfThisService.size()]);
        }
        else
        {
            return null;
        }
    }

    void handleRecordsUpdate(final IRecordChange atomicChange)
    {
        atomicChange.applyTo(this.registry.recordsPerServiceFamily.getOrCreateSubMap(PlatformRegistry.SERVICE_NAME));
        this.registry.context.publishAtomicChange(this.registry.recordsPerServiceFamily);

        final String registryInstanceName =
            PlatformUtils.composePlatformServiceInstanceID(PlatformRegistry.SERVICE_NAME, this.registry.platformName);
        atomicChange.applyTo(this.registry.recordsPerServiceInstance.getOrCreateSubMap(registryInstanceName));
        this.registry.context.publishAtomicChange(this.registry.recordsPerServiceInstance);
    }
}