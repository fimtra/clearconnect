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

import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.RUNTIME_STATUS;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.SERVICES;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.SERVICE_INSTANCE_STATS;
import static com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames.SERVICE_STATS;

import java.awt.*;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.swing.*;

import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.core.PlatformDesktop.ParametersPanel;
import com.fimtra.clearconnect.event.EventListenerUtils;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IPublisherContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.CoalescingRecordListener;
import com.fimtra.datafission.core.CoalescingRecordListener.CachePolicyEnum;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.IStatusAttribute;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.core.session.ISessionAttributesProvider;
import com.fimtra.datafission.core.session.ISessionListener;
import com.fimtra.datafission.core.session.SessionContexts;
import com.fimtra.executors.ContextExecutorFactory;
import com.fimtra.executors.IContextExecutor;
import com.fimtra.util.Log;
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
 * <li>{@link #getPlatformNodesContext()}
 * <li>{@link #getPlatformServicesContext()}
 * <li>{@link #getPlatformServiceInstancesContext()}
 * <li>{@link #getPlatformRegistryAgentsContext()}
 * <li>{@link #getPlatformServiceRpcsContext(String)}
 * <li>{@link #getPlatformServiceRecordsContext(String)}
 * <li>{@link #getPlatformServiceConnectionsContext(String)}
 * <li>{@link #getPlatformServiceInstanceRpcsContext(String)}
 * <li>{@link #getPlatformServiceInstanceRecordsContext(String)}
 * <li>{@link #getPlatformServiceInstanceConnectionsContext(String)}
 * </ul>
 * Code can interact directly with platform services and platform service instances by using the
 * {@link IObserverContext} returned from one of these:
 * <ul>
 * <li>{@link #getProxyContextForPlatformService(String)}
 * <li>{@link #getProxyContextForPlatformServiceInstance(String)}
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

    final IContextExecutor coalescingExecutor =
            ContextExecutorFactory.create("meta-data-model-coalescing-executor", 1);

    final CoalescingRecordListener _servicesRecordListener =
            new CoalescingRecordListener(this.coalescingExecutor, (imageCopy, atomicChange) -> {
                checkReset();
                handlePlatformServicesUpdate(imageCopy, atomicChange);
            }, SERVICES);

    final CoalescingRecordListener _serviceStatsRecordListener =
            new CoalescingRecordListener(this.coalescingExecutor, (imageCopy, atomicChange) -> {
                checkReset();
                handlePlatformServiceStatsUpdate(imageCopy, atomicChange);
            }, SERVICE_STATS);

    final CoalescingRecordListener _serviceInstanceStatsRecordListener =
            new CoalescingRecordListener(this.coalescingExecutor, (imageCopy, atomicChange) -> {
                checkReset();
                handleServiceInstanceStatsUpdate(atomicChange);
            }, SERVICE_INSTANCE_STATS, CachePolicyEnum.NO_IMAGE_NEEDED);

    final CoalescingRecordListener _runtimeStatusRecordListener =
            new CoalescingRecordListener(this.coalescingExecutor, (imageCopy, atomicChange) -> {
                checkReset();
                handleRuntimeStatusUpdate(imageCopy);
            }, RUNTIME_STATUS);

    final PlatformRegistryAgent agent;

    final Context nodesContext;
    final Context agentsContext;
    final Context servicesContext;
    final Context serviceInstancesContext;
    final ConcurrentMap<String, Context> serviceRpcsContext;
    final ConcurrentMap<String, Context> serviceRecordsContext;
    final ConcurrentMap<String, Context> serviceConnectionsContext;
    final ConcurrentMap<String, Context> serviceInstanceRpcsContext;
    final ConcurrentMap<String, Context> serviceInstanceRecordsContext;
    final ConcurrentMap<String, Context> serviceInstanceConnectionsContext;

    boolean reset;

    public PlatformMetaDataModel(String registryNode, int registryPort) throws IOException
    {
        this.agent = new PlatformRegistryAgent(PlatformMetaDataModel.class.getSimpleName(), registryNode,
                registryPort);

        this.nodesContext = new Context("nodes");
        this.agentsContext = new Context("agents");
        this.servicesContext = new Context("services");
        this.serviceInstancesContext = new Context("serviceInstances");

        this.serviceRpcsContext = new ConcurrentHashMap<>();
        this.serviceRecordsContext = new ConcurrentHashMap<>();
        this.serviceConnectionsContext = new ConcurrentHashMap<>();
        this.serviceInstanceRpcsContext = new ConcurrentHashMap<>();
        this.serviceInstanceRecordsContext = new ConcurrentHashMap<>();
        this.serviceInstanceConnectionsContext = new ConcurrentHashMap<>();

        this.agent.addRegistryAvailableListener(
                EventListenerUtils.synchronizedListener(new IRegistryAvailableListener()
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
                }));
    }

    void registerListener_RUNTIME_STATUS()
    {
        this.agent.registryProxy.addObserver(this._runtimeStatusRecordListener, RUNTIME_STATUS);
    }

    void registerListener_SERVICE_INSTANCE_STATS()
    {
        this.agent.registryProxy.addObserver(this._serviceInstanceStatsRecordListener,
                SERVICE_INSTANCE_STATS);
    }

    void registerListener_SERVICES()
    {
        this.agent.registryProxy.addObserver(this._servicesRecordListener, SERVICES);
    }

    void registerListener_SERVICE_STATS()
    {
        this.agent.registryProxy.addObserver(this._serviceStatsRecordListener, SERVICE_STATS);
    }

    void checkReset()
    {
        if (this.reset)
        {
            this.reset = false;
            reset(PlatformMetaDataModel.this.nodesContext);
            reset(PlatformMetaDataModel.this.agentsContext);
            reset(PlatformMetaDataModel.this.servicesContext);
            reset(PlatformMetaDataModel.this.serviceInstancesContext);

            reset(PlatformMetaDataModel.this.serviceRpcsContext);
            reset(PlatformMetaDataModel.this.serviceRecordsContext);
            reset(PlatformMetaDataModel.this.serviceConnectionsContext);
            reset(PlatformMetaDataModel.this.serviceInstanceRpcsContext);
            reset(PlatformMetaDataModel.this.serviceInstanceRecordsContext);
            reset(PlatformMetaDataModel.this.serviceInstanceConnectionsContext);

            final IRecord record = PlatformMetaDataModel.this.servicesContext.getOrCreateRecord(
                    PlatformRegistry.SERVICE_NAME);
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
     * Get the nodes context. Each record in this context represents a single node that is running
     * one or more platform service instances for the platform. The nodes are (generally) identified
     * by their IP address.
     *
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
     * @return a context for the agents on the platform
     */
    public IObserverContext getPlatformRegistryAgentsContext()
    {
        registerListener_RUNTIME_STATUS();
        return this.agentsContext;
    }

    /**
     * Get the platform services context. Each record in this context represents a single platform
     * service and is located by the service name.
     *
     * @return a context for the services on the platform
     */
    public IObserverContext getPlatformServicesContext()
    {
        registerListener_SERVICES();
        registerListener_SERVICE_STATS();
        return this.servicesContext;
    }

    /**
     * Get the platform service instances context. Each record in this context represents a single
     * platform service instance and is located by its service instance ID.
     *
     * @return a context for the services instances on the platform
     * @see PlatformUtils#composePlatformServiceInstanceID(String, String)
     */
    public IObserverContext getPlatformServiceInstancesContext()
    {
        registerListener_SERVICE_INSTANCE_STATS();
        return this.serviceInstancesContext;
    }

    /**
     * Get the connections for a platform service. Each record in this context represents a connection in
     * the platform service.
     *
     * @param serviceFamily the platform service to get the connections for
     * @return a context for the connections for the platform service
     */
    public IObserverContext getPlatformServiceConnectionsContext(String serviceFamily)
    {
        return getFromRemoteContextRecord(this.serviceConnectionsContext, serviceFamily,
                ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS, null);
    }

    /**
     * Get the connections for a platform service. Each record in this context represents a connection in
     * the platform service.
     *
     * @param platformServiceInstanceID the platform service instance key to get the connections for
     * @return a context for the connections for the platform service instance
     * @see PlatformUtils#composePlatformServiceInstanceID(String, String)
     */
    public IObserverContext getPlatformServiceInstanceConnectionsContext(String platformServiceInstanceID)
    {
        return getFromRemoteContextRecord(this.serviceInstanceConnectionsContext, platformServiceInstanceID,
                ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS, null);
    }

    /**
     * Get the records for a platform service. Each record in this context represents the record in
     * the platform service.
     *
     * @param serviceFamily the platform service to get the records for
     * @return a context for the records for the platform service
     */
    public IObserverContext getPlatformServiceRecordsContext(String serviceFamily)
    {
        return getFromRemoteContextRecord(this.serviceRecordsContext, serviceFamily,
                ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_RECORDS, "subscriptions");
    }

    /**
     * Get the records for a platform service instance. Each record in this context represents the
     * record in the platform service instance.
     *
     * @param platformServiceInstanceID the platform service instance key to get the records for
     * @return a context for the records for the platform service
     * @see PlatformUtils#composePlatformServiceInstanceID(String, String)
     */
    public IObserverContext getPlatformServiceInstanceRecordsContext(String platformServiceInstanceID)
    {
        return getFromRemoteContextRecord(this.serviceInstanceRecordsContext, platformServiceInstanceID,
                ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_RECORDS, "subscriptions");
    }

    /**
     * Get the RPCs for a platform service. Each record in this context represents the RPC in the
     * platform service.
     *
     * @param serviceFamily the platform service to get the RPCs for
     * @return a context for the RPCs for the platform service
     */
    public IObserverContext getPlatformServiceRpcsContext(String serviceFamily)
    {
        return getFromRemoteContextRecord(this.serviceRpcsContext, serviceFamily,
                ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS, "args");
    }

    /**
     * Get the RPCs for a platform service instance. Each record in this context represents the RPC
     * in the platform service instance.
     *
     * @param platformServiceInstanceID the platform service instance key to get the RPCs for
     * @return a context for the RPCs for the platform service
     * @see PlatformUtils#composePlatformServiceInstanceID(String, String)
     */
    public IObserverContext getPlatformServiceInstanceRpcsContext(String platformServiceInstanceID)
    {
        return getFromRemoteContextRecord(this.serviceInstanceRpcsContext, platformServiceInstanceID,
                ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS, "args");
    }

    /**
     * Get a remote {@link IObserverContext} to a platform service. <b>THIS WILL CREATE A NEW
     * CONNECTION TO THE SERVICE (ONE OF THE INSTANCES OF THE SERVICE). USE WITH CARE.</b>
     *
     * @param serviceFamily the platform service name to connect to
     * @return an {@link IObserverContext} for the platform service (this will be connected to one
     * of the platform service instances of the platform service)
     */
    public IObserverContext getProxyContextForPlatformService(String serviceFamily)
    {
        // NOTE: this is a small hack to work just for viewing the registry records...
        if (is.eq(serviceFamily, PlatformRegistry.SERVICE_NAME))
        {
            return this.agent.registryProxy;
        }
        registerSessionProvider(serviceFamily);
        final ProxyContext proxyContext =
                ((PlatformServiceProxy) getAgent().getPlatformServiceProxy(serviceFamily)).proxyContext;
        waitForSessionResponse(proxyContext, serviceFamily);
        return proxyContext;
    }

    public String getSessionIdForPlatformService(String serviceFamily)
    {
        return this.sessionIds.get(serviceFamily);
    }

    /**
     * Get a remote {@link IObserverContext} to a platform service instance. <b>THIS WILL CREATE A
     * NEW CONNECTION TO THE SERVICE INSTANCE. USE WITH CARE.</b>
     *
     * @param platformServiceInstanceID the platform service instance ID to connect to
     * @return an {@link IObserverContext} for the platform service instance
     */
    public IObserverContext getProxyContextForPlatformServiceInstance(String platformServiceInstanceID)
    {
        final String[] family_member =
                PlatformUtils.decomposePlatformServiceInstanceID(platformServiceInstanceID);
        // NOTE: another small hack to get the registry proxy
        if (PlatformRegistry.SERVICE_NAME.equals(family_member[0]))
        {
            return this.agent.registryProxy;
        }

        // NOTE: this leaves a connection leak if the proxy is not destroyed when no more components
        // need it from the model
        registerSessionProvider(family_member[0]);
        final ProxyContext proxyContext =
                ((PlatformServiceProxy) this.agent.getPlatformServiceInstanceProxy(family_member[0],
                        family_member[1])).proxyContext;
        waitForSessionResponse(proxyContext, family_member[0]);
        return proxyContext;
    }

    public String getSessionIdForPlatformServiceInstance(String platformServiceInstanceID)
    {
        final String[] family_member =
                PlatformUtils.decomposePlatformServiceInstanceID(platformServiceInstanceID);
        return this.sessionIds.get(family_member[0]);
    }

    private void waitForSessionResponse(final ProxyContext proxyContext, final String serviceFamily)
    {
        final CountDownLatch latch = new CountDownLatch(1);
        proxyContext.addSessionListener(new ISessionListener()
        {
            @Override
            public void onSessionOpen(String sessionContext, String sessionId)
            {
                if (is.eq(serviceFamily, sessionContext))
                {
                    PlatformMetaDataModel.this.sessionIds.put(serviceFamily, sessionId);
                    proxyContext.removeSessionListener(this);
                    latch.countDown();
                }
            }

            @Override
            public void onSessionClosed(String sessionContext, String sessionId)
            {
                if (is.eq(serviceFamily, sessionContext))
                {
                    PlatformMetaDataModel.this.sessionAttributes.remove(serviceFamily);
                    proxyContext.removeSessionListener(this);
                    latch.countDown();
                }
            }
        });
        try
        {
            latch.await(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    final Set<String> sessionAttributes = Collections.synchronizedSet(new HashSet<>());
    final Map<String, String> sessionIds = Collections.synchronizedMap(new HashMap<>());

    private void registerSessionProvider(String serviceFamily)
    {
        if (this.sessionAttributes.add(serviceFamily))
        {
            final ParametersPanel parameters = new ParametersPanel();
            parameters.addParameter("attributes", null);

            final String title = "Session attributes for " + serviceFamily;
            final JDialog dialog = new JDialog((Frame) null, title, true);
            final Point location = MouseInfo.getPointerInfo().getLocation();
            dialog.setLocation((int) location.getX(), (int) location.getY());
            dialog.setIconImage(PlatformDesktop.createIcon());
            parameters.setOkButtonActionListener(e -> dialog.dispose());
            dialog.getRootPane().setDefaultButton(parameters.ok);
            final Font font = parameters.getFont();
            final Graphics graphics = dialog.getGraphics();
            final FontMetrics fontMetrics = dialog.getFontMetrics(font);
            final double width = fontMetrics.getStringBounds(title, graphics).getWidth() + 64;
            parameters.setPreferredSize(
                    new Dimension((int) width, (int) parameters.getPreferredSize().getHeight()));
            dialog.getContentPane().add(parameters);
            dialog.pack();
            dialog.setVisible(true);

            final String[] sessionAttributes = parameters.get().get("attributes").split(",");
            final ISessionAttributesProvider provider = () -> sessionAttributes;
            SessionContexts.registerSessionProvider(serviceFamily, provider);
        }
    }

    /**
     * Execute an RPC using a proxy context. This will wait for the RPC to become available before
     * executing it.
     *
     * @param proxyContext the proxy context to invoke the RPC
     * @param rpcName      the RPC name
     * @param rpcArgs      the arguments for the RPC
     * @return the return value of the RPC execution
     * @throws TimeOutException if the RPC is not available within 5 seconds or if the RPC execution experiences
     *                          an internal timeout
     */
    @SuppressWarnings("static-method")
    public IValue executeRpc(final IObserverContext proxyContext, final String rpcName,
            final IValue... rpcArgs) throws TimeOutException, ExecutionException
    {
        IRpcInstance rpc = proxyContext.getRpc(rpcName);
        if (rpc == null)
        {
            final CountDownLatch latch = new CountDownLatch(1);
            final IRecordListener observer = (imageCopy, atomicChange) -> {
                if (imageCopy.containsKey(rpcName))
                {
                    latch.countDown();
                }
            };
            proxyContext.addObserver(observer, ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
            try
            {
                latch.await(5000, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e)
            {
                // don't care
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
        String serviceFamilyName;
        IValue redundancyMode;
        IRecord serviceRecord;
        for (Map.Entry<String, IValue> iValueEntry : imageCopy.entrySet())
        {
            entry = iValueEntry;
            serviceFamilyName = entry.getKey();
            redundancyMode = entry.getValue();
            serviceRecord = this.servicesContext.getOrCreateRecord(serviceFamilyName);
            serviceRecord.put("Mode", redundancyMode.textValue());

            this.servicesContext.publishAtomicChange(serviceFamilyName);
        }

        // handle removed services
        for (Map.Entry<String, IValue> stringIValueEntry : atomicChange.getRemovedEntries().entrySet())
        {
            removeService(stringIValueEntry.getKey());
        }
    }

    void handlePlatformServiceStatsUpdate(IRecord imageCopy, IRecordChange atomicChange)
    {
        final Set<String> serviceNames = atomicChange.getSubMapKeys();
        IRecord serviceRecord;
        Map<String, IValue> serviceStats;
        for (String serviceFamilyName : serviceNames)
        {
            // note: only SERVICE record updates cause a servicesContext record to be created
            serviceRecord = this.servicesContext.getRecord(serviceFamilyName);
            if (serviceRecord != null)
            {
                serviceStats = imageCopy.getOrCreateSubMap(serviceFamilyName);
                serviceRecord.putAll(serviceStats);
                this.servicesContext.publishAtomicChange(serviceFamilyName);
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
            statsForServiceInstance = this.serviceInstancesContext.getOrCreateRecord(serviceInstanceId);

            stats = atomicChange.getSubMapAtomicChange(serviceInstanceId).getPutEntries();
            if (stats.size() > 0)
            {
                // this field needed for filtering
                statsForServiceInstance.put("Service",
                        PlatformUtils.decomposePlatformServiceInstanceID(serviceInstanceId)[0]);
                statsForServiceInstance.putAll(stats);
                this.serviceInstancesContext.publishAtomicChange(statsForServiceInstance);
            }

            stats = atomicChange.getSubMapAtomicChange(serviceInstanceId).getRemovedEntries();
            if (stats.size() > 0)
            {
                // NOTE: fields are never removed, so any remove means the entire submap has been
                // removed
                removeServiceInstance(serviceInstanceId);
            }
        }
    }

    void handleRuntimeStatusUpdate(IRecord imageCopy)
    {
        // find agents that have disconnected
        final Set<String> toRemove = this.agentsContext.getRecordNames().stream().filter(
                s -> !ContextUtils.isSystemRecordName(s)).collect(Collectors.toSet());
        toRemove.removeAll(imageCopy.getSubMapKeys());
        if (toRemove.size() > 0)
        {
            toRemove.forEach(this.agentsContext::removeRecord);
        }

        final Set<String> agentNames = imageCopy.getSubMapKeys();
        Map<String, IValue> subMap;
        for (String agentName : agentNames)
        {
            subMap = imageCopy.getOrCreateSubMap(agentName);
            this.agentsContext.getOrCreateRecord(agentName).putAll(subMap);
            this.agentsContext.publishAtomicChange(agentName);
        }
    }

    void removeService(final String serviceFamily)
    {
        if (this.servicesContext.removeRecord(serviceFamily) != null)
        {
            Log.log(PlatformMetaDataModel.this, "Removing service '", serviceFamily, "'");
            this.servicesContext.publishAtomicChange(serviceFamily);
        }
    }

    void removeServiceInstance(final String platformServiceInstanceID)
    {
        if (this.serviceInstancesContext.removeRecord(platformServiceInstanceID) != null)
        {
            Log.log(PlatformMetaDataModel.this, "Removing serviceInstance '", platformServiceInstanceID, "'");
            this.serviceInstancesContext.publishAtomicChange(platformServiceInstanceID);

            // remove the service instance from the nodes
            IRecord hostRecord;
            Map<String, IValue> instancesPerNodeSubMap;
            for (String hostNode : this.nodesContext.getRecordNames())
            {
                hostRecord = this.nodesContext.getRecord(hostNode);
                if (hostRecord != null)
                {
                    instancesPerNodeSubMap = hostRecord.getOrCreateSubMap("Instances");
                    if (instancesPerNodeSubMap.remove(platformServiceInstanceID) != null)
                    {
                        if (instancesPerNodeSubMap.size() == 0)
                        {
                            this.nodesContext.removeRecord(hostNode);
                        }
                        else
                        {
                            this.nodesContext.publishAtomicChange(hostRecord);
                        }
                        break;
                    }
                }
            }
        }
    }

    /**
     * Constructs a context from the passed in record - converts fields of the record into records of the context
     */
    Context getFromRemoteContextRecord(ConcurrentMap<String, Context> map, String contextKey,
            String recordName, String fieldName)
    {
        return map.computeIfAbsent(contextKey, (c) -> {

            final boolean isServiceInstance =
                    map == this.serviceInstanceRecordsContext || map == this.serviceInstanceRpcsContext
                            || map == this.serviceInstanceConnectionsContext;
            final IObserverContext proxyContext =
                    isServiceInstance ? getProxyContextForPlatformServiceInstance(contextKey) :
                            getProxyContextForPlatformService(contextKey);
            final Context innerContext = new Context(recordName + "-" + contextKey);
            final IRecordListener converter = (image, atomicChange) -> {
                if (image.getSubMapKeys().size() > 0)
                {
                    final Set<String> subMapKeys = image.getSubMapKeys();
                    for (String subMapKey : subMapKeys)
                    {
                        IRecord flatRecord = innerContext.getOrCreateRecord(subMapKey);
                        final Map<String, IValue> sub = image.getOrCreateSubMap(subMapKey);
                        flatRecord.putAll(sub);

                        innerContext.publishAtomicChange(flatRecord);
                    }

                    // for any update, we need to scan to see if there was anything removed
                    // full scan because the size may still be the same but equal numbers removed then added
                    final Set<String> keysToRemove = innerContext.getRecordNames().stream().filter(
                            k -> !ContextUtils.isSystemRecordName(k)).collect(Collectors.toSet());
                    keysToRemove.removeAll(image.getSubMapKeys());
                    if (keysToRemove.size() > 0)
                    {
                        keysToRemove.forEach(innerContext::removeRecord);
                    }
                }
                else
                {
                    final Set<String> keys = new HashSet<>(image.keySet());
                    for (String key : keys)
                    {
                        if (!ContextUtils.isSystemRecordName(key))
                        {
                            final IRecord record = innerContext.getOrCreateRecord(key);
                            final IValue value = image.get(key);
                            record.put(fieldName, value);
                            innerContext.publishAtomicChange(key);
                        }
                    }

                    // for any update, we need to scan to see if there was anything removed
                    // full scan because the size may still be the same but equal numbers removed then added
                    final Set<String> keysToRemove = innerContext.getRecordNames().stream().filter(
                            k -> !ContextUtils.isSystemRecordName(k)).collect(Collectors.toSet());
                    keysToRemove.removeAll(image.keySet());
                    if (keysToRemove.size() > 0)
                    {
                        keysToRemove.forEach(innerContext::removeRecord);
                    }
                }
            };

            final IRecordListener statusListener = (imageValidInCallingThreadOnly, atomicChange) -> {
                // pass on the connection status to the inner context
                final IStatusAttribute.Connection status =
                        IStatusAttribute.Utils.getStatus(IStatusAttribute.Connection.class,
                                imageValidInCallingThreadOnly);
                System.err.println("Status=" + status + " for " + contextKey + " " + recordName);
                try
                {
                    final Method method =
                            innerContext.getClass().getDeclaredMethod("updateContextStatusAndPublishChange",
                                    IStatusAttribute.class);
                    method.setAccessible(true);
                    method.invoke(innerContext, status);
                }
                catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e)
                {
                    Log.log(PlatformMetaDataModel.this, "Could not pass on " + status + " to " + innerContext,
                            e);
                }
            };
            proxyContext.addObserver(statusListener, IObserverContext.ISystemRecordNames.CONTEXT_STATUS);

            proxyContext.addObserver(converter, recordName);
            return innerContext;
        });
    }

}
