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
package com.fimtra.clearconnect.core;

import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.PLATFORM_CONNECTIONS;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.RECORDS_PER_SERVICE_FAMILY;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.RECORDS_PER_SERVICE_INSTANCE;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.RPCS_PER_SERVICE_FAMILY;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.RPCS_PER_SERVICE_INSTANCE;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.RUNTIME_STATUS;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.SERVICES;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.SERVICE_INSTANCES_PER_AGENT;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.SERVICE_INSTANCES_PER_SERVICE_FAMILY;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.SERVICE_INSTANCE_STATS;
import static com.fimtra.clearconnect.core.PlatformUtils.decomposeClientFromProxyName;
import static com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields.*;
import static com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields.MESSAGE_COUNT;
import static com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields.PROTOCOL;
import static com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields.PROXY_ENDPOINT;
import static com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields.PROXY_ID;
import static com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields.PUBLISHER_ID;
import static com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields.PUBLISHER_NODE;
import static com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields.PUBLISHER_PORT;
import static com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields.SUBSCRIPTION_COUNT;
import static com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields.TRANSPORT;
import static com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields.UPTIME;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.core.PlatformRegistry.IRuntimeStatusRecordFields;
import com.fimtra.clearconnect.core.PlatformServiceInstance.IServiceStatsRecordFields;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields;
import com.fimtra.datafission.IPublisherContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.CoalescingRecordListener;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.core.CoalescingRecordListener.CachePolicyEnum;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.thimble.ThimbleExecutor;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.is;

/**
 * The PlatformMetaDataModel exposes all platform connections and meta-data required by tooling
 * applications to provide support facilities for a platform. The PlatformMetaDataModel attaches to
 * the platform registry and builds up internal data structures held as {@link IObserverContext}
 * objects to present the entire state of the platform.
 * <p>
 * The PlatformMetaDataModel exposes contexts for specific aspects of the platform that can have
 * observers attached to. These contexts are obtained via:
 * <ul>
 * <li>{@link #getPlatformConnectionsContext()}
 * <li>{@link #getPlatformNodesContext()}
 * <li>{@link #getPlatformServicesContext()}
 * <li>{@link #getPlatformServiceInstancesContext()}
 * <li>{@link #getPlatformRegsitryAgentsContext()}
 * <li>{@link #getPlatformServiceProxiesContext()}
 * <li>{@link #getPlatformServiceRecordsContext(String)}
 * <li>{@link #getPlatformServiceRpcsContext(String)}
 * <li>{@link #getPlatformServiceInstanceRecordsContext(String)}
 * <li>{@link #getPlatformServiceInstanceRpcsContext(String)}
 * </ul>
 * Code can interact directly with platform services and platform service instances by using the
 * {@link IObserverContext} returned from one of these:
 * <ul>
 * <li> {@link #getProxyContextForPlatformService(String)}
 * <li> {@link #getProxyContextForPlatformServiceInstance(String)}
 * </ul>
 * A convenience method {@link #executeRpc(IObserverContext, String, IValue...)} exists to execute
 * an RPC on the proxy context returned from the above. The platform agent of the
 * PlatformMetaDataModel can also be used to interact with the platform via {@link #getAgent()}.
 * <p>
 * NOTE: the meta-data model operates using {@link IObserverContext} objects (the datafission
 * components) so deviates somewhat from the platform-core which it supports. Effectively the
 * lower-level objects (datafission) are exposed to introspect the higher-level components
 * (platform-core).
 * 
 * @author Ramon Servadei
 */
public final class PlatformMetaDataModel
{

    static final String RECORD_NAME_FIELD = "name";

    static final int PAUSE_BEFORE_REMOVING_SERVICE_MILLIS = 1000;

    /**
     * The fields for each record in the hosts context
     * 
     * @see PlatformMetaDataModel#getPlatformNodesContext()
     */
    public static enum NodesMetaDataRecordDefinition
    {
        InstanceCount
    }

    /**
     * The fields for each record in the agents context
     * 
     * @see PlatformMetaDataModel#getPlatformRegsitryAgentsContext()
     */
    public static enum AgentMetaDataRecordDefinition
    {
        Node, UpTimeSecs, QOverFlow, QTotalSubmitted, CPUCount, MemUsedMb, MemAvailableMb, ThreadCount, GcDutyCycle,
            Runtime, User, EPM
    }

    /**
     * The fields for each record in the service-proxies context
     * 
     * @see PlatformMetaDataModel#getPlatformServiceProxiesContext()
     * */
    public static enum ServiceProxyMetaDataRecordDefinition
    {
        EndPoint, SubscriptionCount, MessagesReceived, AvergeMessageSizeBytes, DataCountKb, ConnectionUptime, Service,
            ServiceInstance, ServiceEndPoint, MsgsPerSec, KbPerSec,
    }

    /**
     * The fields for each record in the services context
     * 
     * @see PlatformMetaDataModel#getPlatformServicesContext()
     * */
    public static enum ServiceMetaDataRecordDefinition
    {
        Mode, InstanceCount, RecordCount, RpcCount, ConnectionCount
    }

    /**
     * The fields for each record in the service instances context
     * 
     * @see PlatformMetaDataModel#getPlatformServiceInstancesContext()
     */
    public static enum ServiceInstanceMetaDataRecordDefinition
    {
        Service, Node, Port, RecordCount, RpcCount, ConnectionCount, UpTimeSecs, Codec, Agent, SubscriptionCount,
            MessagesSent, DataCountKb, MsgsPerMin, KbPerMin, Transport, Version
    }

    /**
     * The fields for each record in the records per service context
     * 
     * @see PlatformMetaDataModel#getPlatformServiceRecordsContext(String)
     */
    public static enum ServiceRecordMetaDataRecordDefinition
    {
        SubscriptionCount
    }

    /**
     * The fields for each record in the records per service instance context
     * 
     * @see PlatformMetaDataModel#getPlatformServiceInstanceRecordsContext(String)
     */
    public static enum ServiceInstanceRecordMetaDataRecordDefinition
    {
        SubscriptionCount
    }

    /**
     * The fields for each record in the rpcs per service context
     * 
     * @see PlatformMetaDataModel#getPlatformServiceRpcsContext(String)
     */
    public static enum ServiceRpcMetaDataRecordDefinition
    {
        Definition
    }

    /**
     * The fields for each record in the rpcs per service instance context
     * 
     * @see PlatformMetaDataModel#getPlatformServiceInstanceRpcsContext(String)
     */
    public static enum ServiceInstanceRpcMetaDataRecordDefinition
    {
        Definition
    }

    static void removeRecordsNotUpdated(final Set<String> updatedRecords, Context context)
    {
        final Set<String> previous = new HashSet<String>();
        for (String con : context.getRecordNames())
        {
            if (ContextUtils.isSystemRecordName(con))
            {
                continue;
            }
            previous.add(con);
        }
        // now remove
        previous.removeAll(updatedRecords);
        for (String toRemove : previous)
        {
            context.removeRecord(toRemove);
        }
    }

    static void handleRecordsForContext(String contextName, ConcurrentMap<String, Context> contextsPerName,
        final Map<String, IValue> recordsForContext, final Map<String, IValue> updatedRecordsForContext, String field)
    {
        Map.Entry<String, IValue> entry;
        String key;
        IValue value;
        IRecord record;
        Context context = safeGetContext(contextsPerName, contextName);
        for (Iterator<Map.Entry<String, IValue>> it = updatedRecordsForContext.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            key = entry.getKey();
            if (ContextUtils.isSystemRecordName(key))
            {
                continue;
            }
            value = entry.getValue();
            record = context.getOrCreateRecord(key);
            record.put(field, value.textValue());
            context.publishAtomicChange(record);
        }
        removeRecordsNotUpdated(recordsForContext.keySet(), context);
    }

    static void updateCountsForKey(final String key, final Map<String, AtomicInteger> countsPerKey)
    {
        AtomicInteger c = countsPerKey.get(key);
        if (c == null)
        {
            c = new AtomicInteger(0);
            countsPerKey.put(key, c);
        }
        c.getAndIncrement();
    }

    static void updateRecordWithCounts(final Map<String, AtomicInteger> countsPer, Context context, String countField)
    {
        Map.Entry<String, AtomicInteger> entry;
        IRecord record;
        for (Iterator<Map.Entry<String, AtomicInteger>> it = countsPer.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            record = context.getRecord(entry.getKey());
            if (record != null)
            {
                record.put(countField, entry.getValue().intValue());
            }
        }
    }

    static void updateRecordWithCountsAndPublish(String recordName, final Map<String, IValue> instancesToCount,
        Context context, String countField)
    {
        final int size = instancesToCount.size();
        // NOTE: its correct to always do get-or-create here
        final IRecord serviceRecord = context.getOrCreateRecord(recordName);
        serviceRecord.put(countField, LongValue.valueOf(size));
        context.publishAtomicChange(serviceRecord);
    }

    static void removeSystemRecords(Map<String, IValue> records)
    {
        for (Iterator<Map.Entry<String, IValue>> it = records.entrySet().iterator(); it.hasNext();)
        {
            if (ContextUtils.isSystemRecordName(it.next().getKey()))
            {
                it.remove();
            }
        }
    }

    static void publishAtomicChangeForAllRecords(Context context)
    {
        for (String recordName : context.getRecordNames())
        {
            if (!ContextUtils.isSystemRecordName(recordName))
            {
                context.publishAtomicChange(recordName);
            }
        }
    }

    static Context safeGetContext(ConcurrentMap<String, Context> map, String contextKey)
    {
        Context context = map.get(contextKey);
        if (context == null)
        {
            synchronized (contextKey.intern())
            {
                context = map.get(contextKey);
                if (context == null)
                {
                    context = new Context(contextKey);
                    map.put(contextKey, context);
                }
            }
        }
        return context;
    }

    static void reset(ConcurrentMap<String, ?> contexts)
    {
        for (Object object : contexts.values())
        {
            if (object instanceof IPublisherContext)
            {
                ContextUtils.clearNonSystemRecords((IPublisherContext) object);
            }
        }

        // NOTE: do not clear the contexts from this MAP - any views will then be disconnected and
        // not get updates!
    }

    static void reset(Context context)
    {
        if (context == null)
        {
            return;
        }
        ContextUtils.clearNonSystemRecords(context);
    }

    static final IValue BLANK_VALUE = TextValue.valueOf("");

    static IValue safeGetTextValue(IRecord record, String field)
    {
        final IValue iValue = record.get(field);
        if (iValue == null)
        {
            return BLANK_VALUE;
        }
        return iValue;
    }

    final PlatformRegistryAgent agent;

    final Context nodesContext;
    final Context agentsContext;
    final Context servicesContext;
    final Context connectionsContext;
    final Context serviceProxiesContext;
    final Context serviceInstancesContext;
    final ConcurrentMap<String, Context> serviceRpcsContext;
    final ConcurrentMap<String, Context> serviceRecordsContext;
    final ConcurrentMap<String, Context> serviceInstanceRpcsContext;
    final ConcurrentMap<String, Context> serviceInstanceRecordsContext;

    final Set<String> pendingRemoves;
    final ThimbleExecutor coalescingExecutor;

    boolean reset;

    public PlatformMetaDataModel(String registryNode, int registryPort) throws IOException
    {
        this.agent = new PlatformRegistryAgent(PlatformMetaDataModel.class.getSimpleName(), registryNode, registryPort);

        this.pendingRemoves = Collections.synchronizedSet(new HashSet<String>());
        this.coalescingExecutor = new ThimbleExecutor("meta-data-model-coalescing-executor", 1);

        this.nodesContext = new Context("nodes");
        this.agentsContext = new Context("agents");
        this.servicesContext = new Context("services");
        this.connectionsContext = new Context("connections");
        this.serviceProxiesContext = new Context("serviceProxies");
        this.serviceInstancesContext = new Context("serviceInstances");

        this.serviceRpcsContext = new ConcurrentHashMap<String, Context>();
        this.serviceRecordsContext = new ConcurrentHashMap<String, Context>();
        this.serviceInstanceRpcsContext = new ConcurrentHashMap<String, Context>();
        this.serviceInstanceRecordsContext = new ConcurrentHashMap<String, Context>();

        this.agent.addRegistryAvailableListener(new IRegistryAvailableListener()
        {
            @Override
            public void onRegistryDisconnected()
            {
                PlatformMetaDataModel.this.reset = true;
            }

            @Override
            public void onRegistryConnected()
            {
            }
        });

        this.agent.registryProxy.addObserver(new CoalescingRecordListener(this.coalescingExecutor,
            new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    checkReset();
                    handlePlatformServicesUpdate(imageCopy, atomicChange);
                }
            }, SERVICES), SERVICES);

        this.agent.registryProxy.addObserver(new CoalescingRecordListener(this.coalescingExecutor,
            new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    checkReset();
                    handlePlatformServiceInstancesPerAgentUpdate(atomicChange);
                }
            }, SERVICE_INSTANCES_PER_AGENT), SERVICE_INSTANCES_PER_AGENT);

        this.agent.registryProxy.addObserver(new CoalescingRecordListener(this.coalescingExecutor,
            new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    checkReset();
                    handleServiceInstanceStatsUpdate(atomicChange);
                }
            }, SERVICE_INSTANCE_STATS), SERVICE_INSTANCE_STATS);

        this.agent.registryProxy.addObserver(new CoalescingRecordListener(this.coalescingExecutor,
            new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    checkReset();
                    handlePlatformServiceInstancesUpdate(imageCopy);
                }
            }, SERVICE_INSTANCES_PER_SERVICE_FAMILY), SERVICE_INSTANCES_PER_SERVICE_FAMILY);

        this.agent.registryProxy.addObserver(new CoalescingRecordListener(this.coalescingExecutor,
            new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    checkReset();
                    handleRecordsPerServiceUpdate(imageCopy, atomicChange);
                }
            }, RECORDS_PER_SERVICE_FAMILY), RECORDS_PER_SERVICE_FAMILY);

        this.agent.registryProxy.addObserver(new CoalescingRecordListener(this.coalescingExecutor,
            new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    checkReset();
                    handleRecordsPerServiceInstanceUpdate(imageCopy, atomicChange);
                }
            }, RECORDS_PER_SERVICE_INSTANCE), RECORDS_PER_SERVICE_INSTANCE);

        this.agent.registryProxy.addObserver(new CoalescingRecordListener(this.coalescingExecutor,
            new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    checkReset();
                    handleRpcsPerServiceUpdate(imageCopy, atomicChange);
                }
            }, RPCS_PER_SERVICE_FAMILY), RPCS_PER_SERVICE_FAMILY);

        this.agent.registryProxy.addObserver(new CoalescingRecordListener(this.coalescingExecutor,
            new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    checkReset();
                    handleRpcsPerServiceInstanceUpdate(imageCopy, atomicChange);
                }
            }, RPCS_PER_SERVICE_INSTANCE), RPCS_PER_SERVICE_INSTANCE);

        this.agent.registryProxy.addObserver(new CoalescingRecordListener(this.coalescingExecutor,
            new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    checkReset();
                    handleConnectionsUpdate(imageCopy);
                }
            }, PLATFORM_CONNECTIONS), PLATFORM_CONNECTIONS);

        this.agent.registryProxy.addObserver(new CoalescingRecordListener(this.coalescingExecutor,
            new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    checkReset();
                    handleRuntimeStatusUpdate(imageCopy);
                }
            }, RUNTIME_STATUS), RUNTIME_STATUS);
    }

    void checkReset()
    {
        if (this.reset)
        {
            this.reset = false;
            reset(PlatformMetaDataModel.this.nodesContext);
            reset(PlatformMetaDataModel.this.agentsContext);
            reset(PlatformMetaDataModel.this.servicesContext);
            reset(PlatformMetaDataModel.this.connectionsContext);
            reset(PlatformMetaDataModel.this.serviceProxiesContext);
            reset(PlatformMetaDataModel.this.serviceInstancesContext);

            reset(PlatformMetaDataModel.this.serviceRpcsContext);
            reset(PlatformMetaDataModel.this.serviceRecordsContext);
            reset(PlatformMetaDataModel.this.serviceInstanceRpcsContext);
            reset(PlatformMetaDataModel.this.serviceInstanceRecordsContext);

            final IRecord record =
                PlatformMetaDataModel.this.servicesContext.getOrCreateRecord(PlatformRegistry.SERVICE_NAME);
            PlatformMetaDataModel.this.servicesContext.publishAtomicChange(record);
        }
    }

    /**
     * Get the agent used by the {@link PlatformMetaDataModel}. This should be used to interact with
     * the platform as needed.
     * 
     * @return the agent for this
     */
    public IPlatformRegistryAgent getAgent()
    {
        return this.agent;
    }

    /**
     * Get the connections context. Each record in this context represents a single connection on
     * the platform. Each record has the same structure as the fields defined in the
     * {@link IContextConnectionsRecordFields} interface.
     * 
     * @return a context for the connections on the platform
     */
    public IObserverContext getPlatformConnectionsContext()
    {
        return this.connectionsContext;
    }

    /**
     * Get the nodes context. Each record in this context represents a single node that is running
     * one or more platform service instances for the platform. The nodes are (generally) identified
     * by their IP address.
     * 
     * @see NodesMetaDataRecordDefinition
     * @return a context for the nodes on the platform
     */
    public IObserverContext getPlatformNodesContext()
    {
        return this.nodesContext;
    }

    /**
     * Get the agents context. Each record in this context represents a single platform registry
     * agent connected to the registry platform. The agents are located by their name.
     * 
     * @see AgentMetaDataRecordDefinition
     * @return a context for the agents on the platform
     */
    public IObserverContext getPlatformRegsitryAgentsContext()
    {
        return this.agentsContext;
    }

    /**
     * Get the platform services context. Each record in this context represents a single platform
     * service and is located by the service name.
     * 
     * @see ServiceMetaDataRecordDefinition
     * @return a context for the services on the platform
     */
    public IObserverContext getPlatformServicesContext()
    {
        return this.servicesContext;
    }

    /**
     * Get the platform service instances context. Each record in this context represents a single
     * platform service instance and is located by its service instance ID.
     * 
     * @see PlatformUtils#composePlatformServiceInstanceID(String, String)
     * @see ServiceInstanceMetaDataRecordDefinition
     * @return a context for the services instances on the platform
     */
    public IObserverContext getPlatformServiceInstancesContext()
    {
        return this.serviceInstancesContext;
    }

    /**
     * Get the platform service proxies context. Each record in this context represents a single
     * platform service proxy connected to a single platform service instance. The proxy name is the
     * name of each record.
     * 
     * @see ServiceProxyMetaDataRecordDefinition
     * @return a context for the service proxy instances on the platform
     */
    public IObserverContext getPlatformServiceProxiesContext()
    {
        return this.serviceProxiesContext;
    }

    /**
     * Get the records for a platform service. Each record in this context represents the record in
     * the platform service.
     * 
     * @see ServiceRecordMetaDataRecordDefinition
     * @param serviceFamily
     *            the platform service to get the records for
     * @return a context for the records for the platform service
     */
    public IObserverContext getPlatformServiceRecordsContext(String serviceFamily)
    {
        return safeGetContext(this.serviceRecordsContext, serviceFamily);
    }

    /**
     * Get the records for a platform service instance. Each record in this context represents the
     * record in the platform service instance.
     * 
     * @see PlatformUtils#composePlatformServiceInstanceID(String, String)
     * @see ServiceInstanceRecordMetaDataRecordDefinition
     * @param platformServiceInstanceID
     *            the platform service instance key to get the records for
     * @return a context for the records for the platform service
     */
    public IObserverContext getPlatformServiceInstanceRecordsContext(String platformServiceInstanceID)
    {
        return safeGetContext(this.serviceInstanceRecordsContext, platformServiceInstanceID);
    }

    /**
     * Get the RPCs for a platform service. Each record in this context represents the RPC in the
     * platform service.
     * 
     * @see ServiceRpcMetaDataRecordDefinition
     * @param serviceFamily
     *            the platform service to get the RPCs for
     * @return a context for the RPCs for the platform service
     */
    public IObserverContext getPlatformServiceRpcsContext(String serviceFamily)
    {
        return safeGetContext(this.serviceRpcsContext, serviceFamily);
    }

    /**
     * Get the RPCs for a platform service instance. Each record in this context represents the RPC
     * in the platform service instance.
     * 
     * @see PlatformUtils#composePlatformServiceInstanceID(String, String)
     * @see ServiceInstanceRpcMetaDataRecordDefinition
     * @param platformServiceInstanceID
     *            the platform service instance key to get the RPCs for
     * @return a context for the RPCs for the platform service
     */
    public IObserverContext getPlatformServiceInstanceRpcsContext(String platformServiceInstanceID)
    {
        return safeGetContext(this.serviceInstanceRpcsContext, platformServiceInstanceID);
    }

    /**
     * Get a remote {@link IObserverContext} to a platform service. <b>THIS WILL CREATE A NEW
     * CONNECTION TO THE SERVICE (ONE OF THE INSTANCES OF THE SERVICE). USE WITH CARE.</b>
     * 
     * @param serviceFamily
     *            the platform service name to connect to
     * @return an {@link IObserverContext} for the platform service (this will be connected to one
     *         of the platform service instances of the platform service)
     */
    public IObserverContext getProxyContextForPlatformService(String serviceFamily)
    {
        // NOTE: this is a small hack to work just for viewing the registry records...
        if (is.eq(serviceFamily, PlatformRegistry.SERVICE_NAME))
        {
            return this.agent.registryProxy;
        }
        return ((PlatformServiceProxy) getAgent().getPlatformServiceProxy(serviceFamily)).proxyContext;
    }

    /**
     * Get a remote {@link IObserverContext} to a platform service instance. <b>THIS WILL CREATE A
     * NEW CONNECTION TO THE SERVICE INSTANCE. USE WITH CARE.</b>
     * 
     * @param platformServiceInstanceID
     *            the platform service instance ID to connect to
     * @return an {@link IObserverContext} for the platform service instance
     */
    public IObserverContext getProxyContextForPlatformServiceInstance(String platformServiceInstanceID)
    {
        final String[] family_member = PlatformUtils.decomposePlatformServiceInstanceID(platformServiceInstanceID);
        // NOTE: another small hack to get the registry proxy
        if (PlatformRegistry.SERVICE_NAME.equals(family_member[0]))
        {
            return this.agent.registryProxy;
        }

        // todo this leaves a connection leak if the proxy is not destroyed when no more components
        // need it from the model
        return ((PlatformServiceProxy) this.agent.getPlatformServiceInstanceProxy(family_member[0], family_member[1])).proxyContext;
    }

    /**
     * Execute an RPC using a proxy context. This will wait for the RPC to become available before
     * executing it.
     * 
     * @param proxyContext
     *            the proxy context to invoke the RPC
     * @param rpcName
     *            the RPC name
     * @param rpcArgs
     *            the arguments for the RPC
     * @return the return value of the RPC execution
     * @throws TimeOutException
     *             if the RPC is not available within 5 seconds or if the RPC execution experiences
     *             an internal timeout
     * @throws ExecutionException
     */
    @SuppressWarnings("static-method")
    public IValue executeRpc(final IObserverContext proxyContext, final String rpcName, final IValue... rpcArgs)
        throws TimeOutException, ExecutionException
    {
        IRpcInstance rpc = proxyContext.getRpc(rpcName);
        if (rpc == null)
        {
            final CountDownLatch latch = new CountDownLatch(1);
            final IRecordListener observer = new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    if (imageCopy.keySet().contains(rpcName))
                    {
                        latch.countDown();
                    }
                }
            };
            proxyContext.addObserver(observer, ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
            try
            {
                latch.await(5000, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e)
            {
            }
            rpc = proxyContext.getRpc(rpcName);
        }
        if (rpc == null)
        {
            throw new TimeOutException("No RPC available");
        }
        return rpc.execute(rpcArgs);
    }

    void handlePlatformServicesUpdate(IRecord imageCopy, IRecordChange atomicChange)
    {
        Map.Entry<String, IValue> entry;
        String serviceFamilyName = null;
        IValue redundancyMode = null;
        for (Iterator<Map.Entry<String, IValue>> it = imageCopy.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            serviceFamilyName = entry.getKey();
            redundancyMode = entry.getValue();
            this.pendingRemoves.remove(serviceFamilyName);
            this.servicesContext.getOrCreateRecord(serviceFamilyName).put(
                ServiceMetaDataRecordDefinition.Mode.toString(), redundancyMode.textValue());
            this.servicesContext.publishAtomicChange(serviceFamilyName);
        }

        // handle removed services
        for (Iterator<Map.Entry<String, IValue>> it = atomicChange.getRemovedEntries().entrySet().iterator(); it.hasNext();)
        {
            removeService(it.next().getKey());
        }
    }

    void handlePlatformServiceInstancesPerAgentUpdate(IRecordChange atomicChange)
    {
        TextValue agentTextValue;
        for (String agentName : atomicChange.getSubMapKeys())
        {
            agentTextValue = TextValue.valueOf(agentName);
            for (String serviceInstanceID : atomicChange.getSubMapAtomicChange(agentName).getPutEntries().keySet())
            {
                this.serviceInstancesContext.getOrCreateRecord(serviceInstanceID).put(
                    ServiceInstanceMetaDataRecordDefinition.Agent.toString(), agentTextValue);
                this.serviceInstancesContext.publishAtomicChange(serviceInstanceID);
            }
        }
    }

    void handleServiceInstanceStatsUpdate(IRecordChange atomicChange)
    {
        // sub-map key: serviceInstanceId
        // values: fields in IServiceStatsRecordFields
        Map<String, IValue> stats;
        IRecord statsForServiceInstance;
        for (String serviceInstanceId : atomicChange.getSubMapKeys())
        {
            stats = atomicChange.getSubMapAtomicChange(serviceInstanceId).getPutEntries();
            statsForServiceInstance = this.serviceInstancesContext.getOrCreateRecord(serviceInstanceId);
            ContextUtils.fieldCopy(stats, IServiceStatsRecordFields.UPTIME, statsForServiceInstance,
                ServiceInstanceMetaDataRecordDefinition.UpTimeSecs.toString());
            ContextUtils.fieldCopy(stats, IServiceStatsRecordFields.SUBSCRIPTION_COUNT, statsForServiceInstance,
                ServiceInstanceMetaDataRecordDefinition.SubscriptionCount.toString());
            ContextUtils.fieldCopy(stats, IServiceStatsRecordFields.MESSAGE_COUNT, statsForServiceInstance,
                ServiceInstanceMetaDataRecordDefinition.MessagesSent.toString());
            ContextUtils.fieldCopy(stats, IServiceStatsRecordFields.KB_COUNT, statsForServiceInstance,
                ServiceInstanceMetaDataRecordDefinition.DataCountKb.toString());
            ContextUtils.fieldCopy(stats, IServiceStatsRecordFields.MSGS_PER_MIN, statsForServiceInstance,
                ServiceInstanceMetaDataRecordDefinition.MsgsPerMin.toString());
            ContextUtils.fieldCopy(stats, IServiceStatsRecordFields.KB_PER_MIN, statsForServiceInstance,
                ServiceInstanceMetaDataRecordDefinition.KbPerMin.toString());
            ContextUtils.fieldCopy(stats, IServiceStatsRecordFields.VERSION, statsForServiceInstance,
                ServiceInstanceMetaDataRecordDefinition.Version.toString());
            this.serviceInstancesContext.publishAtomicChange(statsForServiceInstance);
        }
    }

    void handlePlatformServiceInstancesUpdate(IRecord imageCopy)
    {
        final Set<String> serviceInstanceIDs = new HashSet<String>();
        final Set<String> serviceFamilys = imageCopy.getSubMapKeys();
        Map<String, IValue> instances;
        String serviceInstanceID;
        TextValue serviceFamilyTextValue;
        for (String serviceFamily : serviceFamilys)
        {
            serviceFamilyTextValue = TextValue.valueOf(serviceFamily);
            instances = imageCopy.getOrCreateSubMap(serviceFamily);

            // update the instance count per service
            updateRecordWithCountsAndPublish(serviceFamily, instances, this.servicesContext,
                ServiceMetaDataRecordDefinition.InstanceCount.toString());

            // add new instances
            for (Iterator<Map.Entry<String, IValue>> it = instances.entrySet().iterator(); it.hasNext();)
            {
                serviceInstanceID = PlatformUtils.composePlatformServiceInstanceID(serviceFamily, it.next().getKey());
                this.serviceInstancesContext.getOrCreateRecord(serviceInstanceID).put(
                    ServiceInstanceMetaDataRecordDefinition.Service.toString(), serviceFamilyTextValue);
                this.serviceInstancesContext.publishAtomicChange(serviceInstanceID);
                serviceInstanceIDs.add(serviceInstanceID);
                this.pendingRemoves.remove(serviceInstanceID);
            }
        }

        // remove instances
        Set<String> previousServiceInstances = new HashSet<String>(this.serviceInstancesContext.getRecordNames());
        previousServiceInstances.removeAll(serviceInstanceIDs);
        for (String removedServiceInstanceID : previousServiceInstances)
        {
            if (!ContextUtils.isSystemRecordName(removedServiceInstanceID))
            {
                removeServiceInstance(removedServiceInstanceID);
            }
        }
    }

    void handleRecordsPerServiceUpdate(IRecord imageCopy, IRecordChange change)
    {
        Map<String, IValue> records;
        for (String serviceFamily : imageCopy.getSubMapKeys())
        {
            records = new HashMap<String, IValue>(imageCopy.getOrCreateSubMap(serviceFamily));
            removeSystemRecords(records);
            updateRecordWithCountsAndPublish(serviceFamily, records, this.servicesContext,
                ServiceMetaDataRecordDefinition.RecordCount.toString());

            handleRecordsForContext(serviceFamily, this.serviceRecordsContext, records,
                change.getSubMapAtomicChange(serviceFamily).getPutEntries(),
                ServiceRecordMetaDataRecordDefinition.SubscriptionCount.toString());
        }
    }

    void handleRecordsPerServiceInstanceUpdate(IRecord imageCopy, IRecordChange change)
    {
        Map<String, IValue> records;
        for (String serviceInstanceID : imageCopy.getSubMapKeys())
        {
            records = new HashMap<String, IValue>(imageCopy.getOrCreateSubMap(serviceInstanceID));
            removeSystemRecords(records);
            updateRecordWithCountsAndPublish(serviceInstanceID, records, this.serviceInstancesContext,
                ServiceInstanceMetaDataRecordDefinition.RecordCount.toString());

            handleRecordsForContext(serviceInstanceID, this.serviceInstanceRecordsContext, records,
                change.getSubMapAtomicChange(serviceInstanceID).getPutEntries(),
                ServiceInstanceRecordMetaDataRecordDefinition.SubscriptionCount.toString());
        }
    }

    void handleRpcsPerServiceUpdate(IRecord imageCopy, IRecordChange change)
    {
        Map<String, IValue> rpcs;
        for (String serviceFamily : imageCopy.getSubMapKeys())
        {
            rpcs = imageCopy.getOrCreateSubMap(serviceFamily);
            updateRecordWithCountsAndPublish(serviceFamily, rpcs, this.servicesContext,
                ServiceMetaDataRecordDefinition.RpcCount.toString());

            handleRecordsForContext(serviceFamily, this.serviceRpcsContext, rpcs,
                change.getSubMapAtomicChange(serviceFamily).getPutEntries(),
                ServiceRpcMetaDataRecordDefinition.Definition.toString());
        }
    }

    void handleRpcsPerServiceInstanceUpdate(IRecord imageCopy, IRecordChange change)
    {
        Map<String, IValue> rpcs;
        for (String serviceInstanceID : imageCopy.getSubMapKeys())
        {
            rpcs = imageCopy.getOrCreateSubMap(serviceInstanceID);
            updateRecordWithCountsAndPublish(serviceInstanceID, rpcs, this.serviceInstancesContext,
                ServiceInstanceMetaDataRecordDefinition.RpcCount.toString());

            handleRecordsForContext(serviceInstanceID, this.serviceInstanceRpcsContext, rpcs,
                change.getSubMapAtomicChange(serviceInstanceID).getPutEntries(),
                ServiceInstanceRpcMetaDataRecordDefinition.Definition.toString());
        }
    }

    void handleRuntimeStatusUpdate(IRecord imageCopy)
    {
        IRecord agentRecord;
        Set<String> agentNames = imageCopy.getSubMapKeys();
        Map<String, IValue> subMap;
        for (String agentName : agentNames)
        {
            subMap = imageCopy.getOrCreateSubMap(agentName);
            agentRecord = this.agentsContext.getRecord(agentName);
            if (agentRecord != null)
            {
                agentRecord.put(AgentMetaDataRecordDefinition.QOverFlow.toString(),
                    subMap.get(IRuntimeStatusRecordFields.Q_OVERFLOW));
                agentRecord.put(AgentMetaDataRecordDefinition.QTotalSubmitted.toString(),
                    subMap.get(IRuntimeStatusRecordFields.Q_TOTAL_SUBMITTED));
                agentRecord.put(AgentMetaDataRecordDefinition.CPUCount.toString(),
                    subMap.get(IRuntimeStatusRecordFields.CPU_COUNT));
                agentRecord.put(AgentMetaDataRecordDefinition.MemUsedMb.toString(),
                    subMap.get(IRuntimeStatusRecordFields.MEM_USED_MB));
                agentRecord.put(AgentMetaDataRecordDefinition.MemAvailableMb.toString(),
                    subMap.get(IRuntimeStatusRecordFields.MEM_AVAILABLE_MB));
                agentRecord.put(AgentMetaDataRecordDefinition.ThreadCount.toString(),
                    subMap.get(IRuntimeStatusRecordFields.THREAD_COUNT));
                agentRecord.put(AgentMetaDataRecordDefinition.GcDutyCycle.toString(),
                    subMap.get(IRuntimeStatusRecordFields.SYSTEM_LOAD));
                agentRecord.put(AgentMetaDataRecordDefinition.Runtime.toString(),
                    subMap.get(IRuntimeStatusRecordFields.RUNTIME));
                agentRecord.put(AgentMetaDataRecordDefinition.User.toString(),
                    subMap.get(IRuntimeStatusRecordFields.USER));
                agentRecord.put(AgentMetaDataRecordDefinition.EPM.toString(),
                    subMap.get(IRuntimeStatusRecordFields.EPM));
                agentRecord.put(AgentMetaDataRecordDefinition.UpTimeSecs.toString(),
                    subMap.get(IRuntimeStatusRecordFields.UPTIME_SECS));
                this.agentsContext.publishAtomicChange(agentRecord);
            }
        }
    }

    void handleConnectionsUpdate(IRecord imageCopy)
    {
        final Set<String> nodesUpdated = new HashSet<String>();
        final Set<String> agentsUpdated = new HashSet<String>();
        final Set<String> serviceProxiesUpdated = new HashSet<String>();
        final Map<String, Set<String>> instancesPerNode = new HashMap<String, Set<String>>();
        final Map<String, AtomicInteger> connectionsPerService = new HashMap<String, AtomicInteger>();
        final Map<String, AtomicInteger> connectionsPerServiceInstance = new HashMap<String, AtomicInteger>();
        final Set<String> connectionKeys = imageCopy.getSubMapKeys();

        IRecord connectionRecord;
        String platformServiceInstanceID;
        String remoteId;
        String clientName;
        String[] decomposeServiceInstanceID;
        String serviceFamily;
        TextValue proxyEndPoint;
        TextValue codec;
        TextValue transport;
        LongValue publisherPort;
        TextValue publisherNode;
        LongValue messageCount;
        LongValue avgMsgSize;
        LongValue msgPerSec;
        DoubleValue kbPerSec;
        LongValue subscriptionCount;
        LongValue kbCount;
        LongValue connectionUptime;
        Map.Entry<String, Set<java.lang.String>> entry;
        IRecord hostRecord;
        Set<String> set;

        for (String connection : connectionKeys)
        {
            try
            {
                connectionRecord = this.connectionsContext.getOrCreateRecord(connection);
                connectionRecord.putAll(imageCopy.getOrCreateSubMap(connection));

                this.connectionsContext.publishAtomicChange(connection);

                // now work out what other data we can extract out of the connection update and
                // apply to the correct meta-data model

                platformServiceInstanceID = safeGetTextValue(connectionRecord, PUBLISHER_ID).textValue();
                remoteId = safeGetTextValue(connectionRecord, PROXY_ID).textValue();
                clientName = decomposeClientFromProxyName(remoteId);

                decomposeServiceInstanceID =
                    PlatformUtils.decomposePlatformServiceInstanceID(platformServiceInstanceID);
                if (decomposeServiceInstanceID == null)
                {
                    serviceFamily = platformServiceInstanceID;
                }
                else
                {
                    serviceFamily = decomposeServiceInstanceID[0];
                }

                // get the client part
                proxyEndPoint = connectionRecord.get(PROXY_ENDPOINT);
                publisherPort = connectionRecord.get(PUBLISHER_PORT);
                publisherNode = connectionRecord.get(PUBLISHER_NODE);
                messageCount = connectionRecord.get(MESSAGE_COUNT);
                avgMsgSize = connectionRecord.get(AVG_MSG_SIZE);
                msgPerSec = connectionRecord.get(MSGS_PER_SEC);
                kbPerSec = connectionRecord.get(KB_PER_SEC);
                subscriptionCount = connectionRecord.get(SUBSCRIPTION_COUNT);
                kbCount = connectionRecord.get(KB_COUNT);
                connectionUptime = connectionRecord.get(UPTIME);
                codec = connectionRecord.get(PROTOCOL);
                transport = connectionRecord.get(TRANSPORT);

                if (publisherNode == null)
                {
                    Log.log(this, "No data for ", ObjectUtils.safeToString(connectionRecord));
                    continue;
                }
                nodesUpdated.add(publisherNode.textValue());

                updateCountsForKey(serviceFamily, connectionsPerService);
                updateCountsForKey(platformServiceInstanceID, connectionsPerServiceInstance);

                if (serviceFamily.startsWith(PlatformRegistry.SERVICE_NAME))
                {
                    // its an agent connection
                    IRecord agentRecord = this.agentsContext.getOrCreateRecord(clientName);
                    agentRecord.put(AgentMetaDataRecordDefinition.Node.toString(), proxyEndPoint);
                    agentsUpdated.add(clientName);
                }
                else
                {
                    // its a service proxy connection flavour

                    // if its a service proxy for a registry (i.e. when the registry monitors a
                    // service instance) e.g.
                    // MARKET_DATA_SERVICE[mds_1382211729926]->PlatformRegistry[ExamplePlatform]@EVS1
                    if (decomposeServiceInstanceID != null && clientName.startsWith(PlatformRegistry.SERVICE_NAME))
                    {
                        IRecord serviceInstanceRecord =
                            this.serviceInstancesContext.getRecord(platformServiceInstanceID);
                        if (serviceInstanceRecord != null)
                        {
                            serviceInstanceRecord.put(ServiceInstanceMetaDataRecordDefinition.Node.toString(),
                                publisherNode);
                            serviceInstanceRecord.put(ServiceInstanceMetaDataRecordDefinition.Port.toString(),
                                publisherPort);
                            serviceInstanceRecord.put(ServiceInstanceMetaDataRecordDefinition.Codec.toString(), codec);
                            serviceInstanceRecord.put(ServiceInstanceMetaDataRecordDefinition.Transport.toString(),
                                transport);
                        }
                    }

                    serviceProxiesUpdated.add(remoteId);
                    IRecord serviceProxyRecord = this.serviceProxiesContext.getOrCreateRecord(remoteId);
                    serviceProxyRecord.put(ServiceProxyMetaDataRecordDefinition.EndPoint.toString(), proxyEndPoint);
                    serviceProxyRecord.put(ServiceProxyMetaDataRecordDefinition.MessagesReceived.toString(),
                        messageCount);
                    serviceProxyRecord.put(ServiceProxyMetaDataRecordDefinition.AvergeMessageSizeBytes.toString(),
                        avgMsgSize);
                    serviceProxyRecord.put(ServiceProxyMetaDataRecordDefinition.MsgsPerSec.toString(),
                        msgPerSec);
                    serviceProxyRecord.put(ServiceProxyMetaDataRecordDefinition.KbPerSec.toString(),
                        kbPerSec);
                    serviceProxyRecord.put(ServiceProxyMetaDataRecordDefinition.DataCountKb.toString(), kbCount);
                    serviceProxyRecord.put(ServiceProxyMetaDataRecordDefinition.SubscriptionCount.toString(),
                        subscriptionCount);
                    serviceProxyRecord.put(ServiceProxyMetaDataRecordDefinition.ConnectionUptime.toString(),
                        connectionUptime);
                    serviceProxyRecord.put(ServiceProxyMetaDataRecordDefinition.Service.toString(), serviceFamily);
                    serviceProxyRecord.put(ServiceProxyMetaDataRecordDefinition.ServiceInstance.toString(),
                        platformServiceInstanceID);
                    serviceProxyRecord.put(
                        ServiceProxyMetaDataRecordDefinition.ServiceEndPoint.toString(),
                        publisherNode.textValue()
                            + (TransportTechnologyEnum.valueOf(transport.textValue()).getNodePortDelimiter())
                            + publisherPort.textValue());

                }

                set = instancesPerNode.get(publisherNode.textValue());
                if (set == null)
                {
                    set = new HashSet<String>();
                    instancesPerNode.put(publisherNode.textValue(), set);
                }
                set.add(platformServiceInstanceID);
            }
            catch (Exception e)
            {
                Log.log(this, "Could not process connection: " + connection, e);
            }
        }

        updateRecordWithCounts(connectionsPerService, this.servicesContext,
            ServiceMetaDataRecordDefinition.ConnectionCount.toString());
        updateRecordWithCounts(connectionsPerServiceInstance, this.serviceInstancesContext,
            ServiceInstanceMetaDataRecordDefinition.ConnectionCount.toString());

        Map<String, IValue> instancesPerNodeSubMap = null;
        for (Iterator<Map.Entry<String, Set<String>>> it = instancesPerNode.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            set = entry.getValue();
            hostRecord = this.nodesContext.getOrCreateRecord(entry.getKey());
            instancesPerNodeSubMap = hostRecord.getOrCreateSubMap("Instances");
            for (String serviceInstanceId : set)
            {
                instancesPerNodeSubMap.put(serviceInstanceId, BLANK_VALUE);
            }
            hostRecord.put(NodesMetaDataRecordDefinition.InstanceCount.toString(), instancesPerNodeSubMap.size());
        }

        // handle removed services and instances
        removeRecordsNotUpdated(nodesUpdated, this.nodesContext);
        removeRecordsNotUpdated(agentsUpdated, this.agentsContext);
        removeRecordsNotUpdated(connectionKeys, this.connectionsContext);
        removeRecordsNotUpdated(serviceProxiesUpdated, this.serviceProxiesContext);

        publishAtomicChangeForAllRecords(this.connectionsContext);
        publishAtomicChangeForAllRecords(this.nodesContext);
        publishAtomicChangeForAllRecords(this.agentsContext);
        publishAtomicChangeForAllRecords(this.servicesContext);
        publishAtomicChangeForAllRecords(this.serviceInstancesContext);
        publishAtomicChangeForAllRecords(this.serviceProxiesContext);
    }

    void removeService(final String serviceFamily)
    {
        this.pendingRemoves.add(serviceFamily);
        this.agent.getUtilityExecutor().schedule(new Runnable()
        {
            @Override
            public void run()
            {
                PlatformMetaDataModel.this.coalescingExecutor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        if (!PlatformMetaDataModel.this.pendingRemoves.remove(serviceFamily))
                        {
                            return;
                        }
                        if (PlatformMetaDataModel.this.servicesContext.removeRecord(serviceFamily) != null)
                        {
                            Log.log(PlatformMetaDataModel.this, "Removing service '", serviceFamily, "'");
                            removeRecords(PlatformMetaDataModel.this.serviceRecordsContext.get(serviceFamily));
                            removeRecords(PlatformMetaDataModel.this.serviceRpcsContext.get(serviceFamily));
                            PlatformMetaDataModel.this.servicesContext.publishAtomicChange(serviceFamily);
                        }
                        // remove instances linked to this service
                        final Set<String> serviceInstanceIds =
                            PlatformMetaDataModel.this.serviceInstancesContext.getRecordNames();
                        IRecord serviceInstanceRecord;
                        IValue service;
                        for (String serviceInstanceId : serviceInstanceIds)
                        {
                            serviceInstanceRecord =
                                PlatformMetaDataModel.this.serviceInstancesContext.getRecord(serviceInstanceId);
                            service =
                                serviceInstanceRecord.get(ServiceInstanceMetaDataRecordDefinition.Service.toString());
                            if (service != null && is.eq(serviceFamily, service.textValue()))
                            {
                                removeServiceInstance(serviceInstanceId);
                            }
                        }
                    }
                });
            }
        }, PAUSE_BEFORE_REMOVING_SERVICE_MILLIS, TimeUnit.MILLISECONDS);
    }

    void removeServiceInstance(final String platformServiceInstanceID)
    {
        this.pendingRemoves.add(platformServiceInstanceID);
        this.agent.getUtilityExecutor().schedule(new Runnable()
        {
            @Override
            public void run()
            {
                PlatformMetaDataModel.this.coalescingExecutor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        if (!PlatformMetaDataModel.this.pendingRemoves.remove(platformServiceInstanceID))
                        {
                            return;
                        }
                        if (PlatformMetaDataModel.this.serviceInstancesContext.removeRecord(platformServiceInstanceID) != null)
                        {
                            Log.log(PlatformMetaDataModel.this, "Removing serviceInstance '",
                                platformServiceInstanceID, "'");
                            removeRecords(PlatformMetaDataModel.this.serviceInstanceRecordsContext.get(platformServiceInstanceID));
                            removeRecords(PlatformMetaDataModel.this.serviceInstanceRpcsContext.get(platformServiceInstanceID));
                            PlatformMetaDataModel.this.serviceInstancesContext.publishAtomicChange(platformServiceInstanceID);

                            // remove the service instance from the nodes
                            IRecord hostRecord = null;
                            Map<String, IValue> instancesPerNodeSubMap = null;
                            for (String hostNode : PlatformMetaDataModel.this.nodesContext.getRecordNames())
                            {
                                hostRecord = PlatformMetaDataModel.this.nodesContext.getRecord(hostNode);
                                if (hostRecord != null)
                                {
                                    instancesPerNodeSubMap = hostRecord.getOrCreateSubMap("Instances");
                                    if (instancesPerNodeSubMap.remove(platformServiceInstanceID) != null)
                                    {
                                        if (instancesPerNodeSubMap.size() == 0)
                                        {
                                            PlatformMetaDataModel.this.nodesContext.removeRecord(hostNode);
                                        }
                                        else
                                        {
                                            PlatformMetaDataModel.this.nodesContext.publishAtomicChange(hostRecord);
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }, PAUSE_BEFORE_REMOVING_SERVICE_MILLIS, TimeUnit.MILLISECONDS);
    }

    static void removeRecords(final Context context)
    {
        ContextUtils.removeRecords(context);
    }
}
