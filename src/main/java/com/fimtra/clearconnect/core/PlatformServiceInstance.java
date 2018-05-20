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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.event.IFtStatusListener;
import com.fimtra.clearconnect.event.IProxyConnectionListener;
import com.fimtra.clearconnect.event.IRecordAvailableListener;
import com.fimtra.clearconnect.event.IRecordSubscriptionListener;
import com.fimtra.clearconnect.event.IRecordSubscriptionListener.SubscriptionInfo;
import com.fimtra.clearconnect.event.IRpcAvailableListener;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields;
import com.fimtra.datafission.IPermissionFilter;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValidator;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.Publisher;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.thimble.ISequentialRunnable;
import com.fimtra.thimble.ThimbleExecutor;
import com.fimtra.util.Log;
import com.fimtra.util.NotifyingCache;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.is;

/**
 * The standard platform service. By default all instances are fault-tolerant unless specified
 * otherwise via the secondary constructor.
 * 
 * @author Ramon Servadei
 */
final class PlatformServiceInstance implements IPlatformServiceInstance
{
    /** The name of the service stats record */
    static final String SERVICE_STATS_RECORD_NAME = "Service Stats";

    /**
     * Defines the fields for the service stats record.
     * <p>
     * This is different to the statistics in the {@link IContextConnectionsRecordFields}. The
     * context connects record shows statistics about the individual connection between a Context
     * (service) and ProxyContext (service proxy). The Service stats record shows the <b>overall</b>
     * statistics for the service (Context).
     * 
     * @author Ramon Servadei
     */
    static interface IServiceStatsRecordFields
    {
        String SUBSCRIPTION_COUNT = "Subscriptions";
        String MESSAGE_COUNT = "Msgs published";
        String AVG_MSG_SIZE = "Avg msg size (bytes)";
        String MSGS_PER_SEC = "Msgs per sec";
        String KB_COUNT = "Kb published";
        String KB_PER_SEC = "Kb per sec";
        String UPTIME = "Uptime(sec)";
        String VERSION = "Version";
        String RECORD_COUNT = "RecordCount";
        String RPC_COUNT = "RpcCount";
    }

    static final String RPC_FT_SERVICE_STATUS = "ftServiceInstanceStatus";

    boolean active;
    final long startTimeMillis;
    final Context context;
    Boolean isFtMasterInstance;
    final String platformName;
    final String serviceFamily;
    final String serviceMember;
    final Publisher publisher;
    final WireProtocolEnum wireProtocol;
    final RedundancyModeEnum redundancyMode;
    final List<IFtStatusListener> ftStatusListeners;
    final NotifyingCache<IRecordAvailableListener, String> recordAvailableNotifyingCache;
    final NotifyingCache<IRpcAvailableListener, IRpcInstance> rpcAvailableNotifyingCache;
    final NotifyingCache<IRecordSubscriptionListener, SubscriptionInfo> subscriptionNotifyingCache;
    final NotifyingCache<IProxyConnectionListener, IValue> proxyConnectionListenerCache;
    final IRecord stats;
    final EndPointAddress endPointAddress;
    final ScheduledFuture<?> statsUpdateTask;

    @SuppressWarnings({ "unchecked" })
    PlatformServiceInstance(String platformName, String serviceFamily, String serviceMember,
        WireProtocolEnum wireProtocol, RedundancyModeEnum redundancyMode, String host, int port,
        ThimbleExecutor coreExecutor, ThimbleExecutor rpcExecutor, ScheduledExecutorService utilityExecutor,
        TransportTechnologyEnum transportTechnology)
    {
        this.startTimeMillis = System.currentTimeMillis();
        
        this.platformName = platformName;
        this.serviceFamily = serviceFamily;
        this.serviceMember = serviceMember;
        this.wireProtocol = wireProtocol;
        this.redundancyMode = redundancyMode;

        if (redundancyMode == RedundancyModeEnum.FAULT_TOLERANT)
        {
            this.ftStatusListeners = new CopyOnWriteArrayList<IFtStatusListener>();
        }
        else
        {
            this.ftStatusListeners = Collections.EMPTY_LIST;
        }
        this.context =
            new Context(PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember), coreExecutor,
                rpcExecutor, utilityExecutor);
        this.stats = this.context.getOrCreateRecord(SERVICE_STATS_RECORD_NAME);
        this.stats.put(IServiceStatsRecordFields.VERSION, TextValue.valueOf(PlatformUtils.VERSION));
        
        // update service stats periodically
        this.statsUpdateTask = this.context.getUtilityExecutor().scheduleWithFixedDelay(
            new Runnable()
            {
                long lastMessagesPublished = 0;
                long lastBytesPublished = 0;

                @Override
                public void run()
                {
                    IRecord subscriptions =
                        PlatformServiceInstance.this.context.getRecord(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);

                    int subscriptionCount = 0;
                    for (Iterator<Map.Entry<String, IValue>> it = subscriptions.entrySet().iterator(); it.hasNext();)
                    {
                        subscriptionCount += it.next().getValue().longValue();
                    }

                    final double inverse_1K = 1 / 1024d;
                    // can be null on the first call
                    final long messagesPublished = PlatformServiceInstance.this.publisher == null ? 0
                        : PlatformServiceInstance.this.publisher.getMessagesPublished();
                    final long bytesPublished = PlatformServiceInstance.this.publisher == null ? 0
                        : PlatformServiceInstance.this.publisher.getBytesPublished();
                    final double perSec = 1d / (DataFissionProperties.Values.STATS_LOGGING_PERIOD_SECS);

                    PlatformServiceInstance.this.stats.put(
                        IServiceStatsRecordFields.MSGS_PER_SEC,
                        DoubleValue.valueOf(((long) (((messagesPublished - this.lastMessagesPublished) * perSec) * 10)) / 10d));
                    PlatformServiceInstance.this.stats.put(
                        IServiceStatsRecordFields.KB_PER_SEC,
                        DoubleValue.valueOf((((long) (((bytesPublished - this.lastBytesPublished) * inverse_1K * perSec) * 10)) / 10d)));
                    PlatformServiceInstance.this.stats.put(IServiceStatsRecordFields.AVG_MSG_SIZE,
                        // use the period stats for calculating the average message size
                        LongValue.valueOf(messagesPublished == 0 ? 0 : ((bytesPublished - this.lastBytesPublished)
                            / (messagesPublished - this.lastMessagesPublished))));
                    PlatformServiceInstance.this.stats.put(IServiceStatsRecordFields.SUBSCRIPTION_COUNT,
                        LongValue.valueOf(subscriptionCount));
                    PlatformServiceInstance.this.stats.put(
                        IServiceStatsRecordFields.UPTIME,
                        LongValue.valueOf((long) ((System.currentTimeMillis() - PlatformServiceInstance.this.startTimeMillis) * 0.001d)));
                    PlatformServiceInstance.this.stats.put(IServiceStatsRecordFields.MESSAGE_COUNT,
                        LongValue.valueOf(messagesPublished));
                    PlatformServiceInstance.this.stats.put(IServiceStatsRecordFields.KB_COUNT,
                        LongValue.valueOf((long) (bytesPublished * inverse_1K)));
                    PlatformServiceInstance.this.stats.put(IServiceStatsRecordFields.RECORD_COUNT,
                        LongValue.valueOf(getRecord(ISystemRecordNames.CONTEXT_RECORDS).size()));
                    PlatformServiceInstance.this.stats.put(IServiceStatsRecordFields.RPC_COUNT,
                        LongValue.valueOf(getRecord(ISystemRecordNames.CONTEXT_RPCS).size()));

                    this.lastMessagesPublished = messagesPublished;
                    this.lastBytesPublished = bytesPublished;

                    PlatformServiceInstance.this.context.publishAtomicChange(PlatformServiceInstance.this.stats);
                }
            }, 1, PlatformCoreProperties.Values.SERVICE_STATS_RECORD_PUBLISH_PERIOD_SECS, TimeUnit.SECONDS);

        this.publisher = new Publisher(this.context, this.wireProtocol.getCodec(), host, port, transportTechnology);
        this.recordAvailableNotifyingCache =
            PlatformUtils.createRecordAvailableNotifyingCache(this.context, ISystemRecordNames.CONTEXT_RECORDS, this);
        this.rpcAvailableNotifyingCache =
            PlatformUtils.createRpcAvailableNotifyingCache(this.context, ISystemRecordNames.CONTEXT_RPCS, this);
        this.subscriptionNotifyingCache =
            PlatformUtils.createSubscriptionNotifyingCache(this.context, ISystemRecordNames.CONTEXT_SUBSCRIPTIONS, this);
        this.proxyConnectionListenerCache = PlatformUtils.createProxyConnectionNotifyingCache(this.context, this);
        this.active = true;

        if (redundancyMode == RedundancyModeEnum.FAULT_TOLERANT)
        {
            RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
            {
                @Override
                public IValue execute(final IValue... args) throws TimeOutException, ExecutionException
                {
                    setFtState(Boolean.valueOf(args[0].textValue()));
                    return PlatformUtils.OK;
                }

            }, TypeEnum.TEXT, RPC_FT_SERVICE_STATUS, TypeEnum.TEXT);
            this.context.createRpc(rpc);
        }

        // NOTE: we need to return the actual port used by the publisher for transport assigned port
        // (e.g. ephemeral port for TCP)
        this.endPointAddress = new EndPointAddress(host, this.publisher.getEndPointAddress().getPort());
        
        Log.log(this, "Constructed ", ObjectUtils.safeToString(this));
    }

    @Override
    public Future<Map<String, Boolean>> addRecordListener(IRecordListener listener, String... recordNames)
    {
        return this.context.addObserver(listener, recordNames);
    }

    @Override
    public Future<Map<String, Boolean>> addRecordListener(String permissionToken, IRecordListener listener,
        String... recordNames)
    {
        return this.context.addObserver(permissionToken, listener, recordNames);
    }

    @Override
    public CountDownLatch removeRecordListener(IRecordListener listener, String... recordNames)
    {
        return this.context.removeObserver(listener, recordNames);
    }

    @Override
    public Set<String> getAllRecordNames()
    {
        return this.context.getRecordNames();
    }

    @Override
    public boolean addRecordSubscriptionListener(IRecordSubscriptionListener listener)
    {
        return this.subscriptionNotifyingCache.addListener(listener);
    }

    @Override
    public boolean removeRecordSubscriptionListener(IRecordSubscriptionListener listener)
    {
        return this.subscriptionNotifyingCache.removeListener(listener);
    }

    @Override
    public boolean addRecordAvailableListener(IRecordAvailableListener recordListener)
    {
        return this.recordAvailableNotifyingCache.addListener(recordListener);
    }

    @Override
    public boolean removeRecordAvailableListener(IRecordAvailableListener recordListener)
    {
        return this.recordAvailableNotifyingCache.removeListener(recordListener);
    }

    @Override
    public Map<String, IRpcInstance> getAllRpcs()
    {
        return this.context.getAllRpcs();
    }

    @Override
    public IRpcInstance getRpc(String rpcName)
    {
        return this.context.getRpc(rpcName);
    }
    
    @Override
    public IValue executeRpc(long discoveryTimeoutMillis, String rpcName, IValue... rpcArgs) throws TimeOutException,
        ExecutionException
    {
        return PlatformUtils.executeRpc(this, discoveryTimeoutMillis, rpcName, rpcArgs);
    }

    @Override
    public void executeRpcNoResponse(long discoveryTimeoutMillis, String rpcName, IValue... rpcArgs)
        throws TimeOutException, ExecutionException
    {
        PlatformUtils.executeRpcNoResponse(this, discoveryTimeoutMillis, rpcName, rpcArgs);
    }

    @Override
    public boolean addRpcAvailableListener(IRpcAvailableListener rpcListener)
    {
        return this.rpcAvailableNotifyingCache.addListener(rpcListener);
    }

    @Override
    public boolean removeRpcAvailableListener(IRpcAvailableListener rpcListener)
    {
        return this.rpcAvailableNotifyingCache.removeListener(rpcListener);
    }

    @Override
    public boolean addProxyConnectionListener(IProxyConnectionListener proxyConnectionListener)
    {
        return this.proxyConnectionListenerCache.addListener(proxyConnectionListener);
    }

    @Override
    public boolean removeProxyConnectionListener(IProxyConnectionListener proxyConnectionListener)
    {
        return this.proxyConnectionListenerCache.removeListener(proxyConnectionListener);
    }

    @Override
    public boolean createRecord(String name)
    {
        if (this.context.getRecord(name) != null)
        {
            return false;
        }
        this.context.createRecord(name);
        return true;
    }

    @Override
    public IRecord getRecord(String name)
    {
        return this.context.getRecord(name);
    }

    @Override
    public IRecord getOrCreateRecord(String name)
    {
        return this.context.getOrCreateRecord(name);
    }

    @Override
    public CountDownLatch publishRecord(IRecord record)
    {
        if (record == null || !is.eq(record.getContextName(), this.context.getName()))
        {
            return null;
        }
        return this.context.publishAtomicChange(record.getName());
    }

    @Override
    public boolean deleteRecord(IRecord record)
    {
        if (record == null)
        {
            return false;
        }
        if (!is.eq(record.getContextName(), this.context.getName()))
        {
            return false;
        }
        return this.context.removeRecord(record.getName()) != null;
    }

    /**
     * Destroy this component. This will close the TCP connection and release all resources used by
     * the object. There is no specification for what callbacks will be invoked for any attached
     * listeners.
     */
    public void destroy()
    {
        Log.log(this, "Destroying ", ObjectUtils.safeToString(this));
        this.statsUpdateTask.cancel(false);
        this.publisher.destroy();
        this.context.destroy();

        this.recordAvailableNotifyingCache.destroy();
        this.rpcAvailableNotifyingCache.destroy();
        this.subscriptionNotifyingCache.destroy();
        this.proxyConnectionListenerCache.destroy();

        this.active = false;
    }

    @Override
    public boolean publishRPC(IRpcInstance rpc)
    {
        if (this.context.getRpc(rpc.getName()) != null)
        {
            return false;
        }
        this.context.createRpc(rpc);
        return true;
    }

    @Override
    public boolean unpublishRPC(IRpcInstance rpc)
    {
        if (this.context.getRpc(rpc.getName()) == null)
        {
            return false;
        }
        this.context.removeRpc(rpc.getName());
        return true;
    }

    @Override
    public boolean isActive()
    {
        return this.active;
    }

    @Override
    public String toString()
    {
        return "PlatformServiceInstance [platform{" + this.platformName + "} service{" + this.serviceFamily
            + "} member{" + this.serviceMember + "}] " + getEndPointAddress();
    }

    @Override
    public String getPlatformName()
    {
        return this.platformName;
    }

    @Override
    public String getPlatformServiceFamily()
    {
        return this.serviceFamily;
    }

    @Override
    public String getPlatformServiceMemberName()
    {
        return this.serviceMember;
    }

    @Override
    public EndPointAddress getEndPointAddress()
    {
        return this.endPointAddress;
    }

    @Override
    public WireProtocolEnum getWireProtocol()
    {
        return this.wireProtocol;
    }

    @Override
    public RedundancyModeEnum getRedundancyMode()
    {
        return this.redundancyMode;
    }

    boolean addValidator(IValidator validator)
    {
        return this.context.addValidator(validator);
    }

    void updateValidator(IValidator validator)
    {
        this.context.updateValidator(validator);
    }

    boolean removeValidator(IValidator validator)
    {
        return this.context.removeValidator(validator);
    }

    @Override
    public Map<String, SubscriptionInfo> getAllSubscriptions()
    {
        return this.subscriptionNotifyingCache.getCacheSnapshot();
    }

    @Override
    public void addFtStatusListener(final IFtStatusListener ftStatusListener)
    {
        this.context.getUtilityExecutor().execute(new Runnable()
        {
            @Override
            public void run()
            {
                PlatformServiceInstance.this.ftStatusListeners.add(ftStatusListener);
                if (PlatformServiceInstance.this.isFtMasterInstance != null)
                {
                    if (PlatformServiceInstance.this.isFtMasterInstance.booleanValue())
                    {
                        ftStatusListener.onActive(PlatformServiceInstance.this.serviceFamily,
                            PlatformServiceInstance.this.serviceMember);
                    }
                    else
                    {
                        ftStatusListener.onStandby(PlatformServiceInstance.this.serviceFamily,
                            PlatformServiceInstance.this.serviceMember);
                    }
                }
            }
        });
    }

    @Override
    public void removeFtStatusListener(final IFtStatusListener ftStatusListener)
    {
        this.context.getUtilityExecutor().execute(new Runnable()
        {
            @Override
            public void run()
            {
                PlatformServiceInstance.this.ftStatusListeners.remove(ftStatusListener);
            }
        });
    }

    @Override
    public void executeSequentialCoreTask(ISequentialRunnable sequentialRunnable)
    {
        this.context.executeSequentialCoreTask(sequentialRunnable);
    }

    @Deprecated
    @Override
    public ScheduledExecutorService getUtilityExecutor()
    {
        return this.context.getUtilityExecutor();
    }

    void updateServiceStats()
    {
        IRecord subscriptions = this.context.getRecord(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);

        int subscriptionCount = 0;
        for (Iterator<Map.Entry<String, IValue>> it = subscriptions.entrySet().iterator(); it.hasNext();)
        {
            subscriptionCount += it.next().getValue().longValue();
        }

        this.stats.put(IServiceStatsRecordFields.SUBSCRIPTION_COUNT, subscriptionCount);
        this.stats.put(IServiceStatsRecordFields.UPTIME, (System.currentTimeMillis() - this.startTimeMillis) / 1000);
        this.stats.put(IServiceStatsRecordFields.MESSAGE_COUNT, this.publisher.getMessagesPublished());
        this.stats.put(IServiceStatsRecordFields.KB_COUNT, LongValue.valueOf(this.publisher.getBytesPublished() / 1024));

        this.context.publishAtomicChange(this.stats);
    }

    @Override
    public void setPermissionFilter(final IPermissionFilter filter)
    {
        this.context.setPermissionFilter(new IPermissionFilter()
        {
            @Override
            public boolean accept(String permissionToken, String recordName)
            {
                if(SERVICE_STATS_RECORD_NAME.equals(recordName))
                {
                    return true;
                }
                return filter.accept(permissionToken, recordName);
            }
        });
    }

    @Override
    public String getComponentName()
    {
        return this.context.getName();
    }


    void setFtState(final Boolean isMaster)
    {
        if (this.redundancyMode == RedundancyModeEnum.FAULT_TOLERANT)
        {
            this.context.getUtilityExecutor().execute(new Runnable()
            {
                @Override
                public void run()
                {
                    doSetFtState(isMaster);
                }
            });
        }
    }

    void doSetFtState(final Boolean isFtMaster)
    {
        if (!isFtMaster.equals(PlatformServiceInstance.this.isFtMasterInstance))
        {
            this.isFtMasterInstance = isFtMaster;
            
            final boolean isMaster = isFtMaster.booleanValue();
            
            Log.banner(this, this.toString() + " " + (isMaster ? "ACTIVE" : "STANDBY"));

            for (IFtStatusListener iFtStatusListener : this.ftStatusListeners)
            {
                try
                {
                    if (isMaster)
                    {
                        iFtStatusListener.onActive(this.serviceFamily, this.serviceMember);
                    }
                    else
                    {
                        iFtStatusListener.onStandby(this.serviceFamily, this.serviceMember);
                    }
                }
                catch (Exception e)
                {
                    Log.log(this, "Could not notify " + ObjectUtils.safeToString(iFtStatusListener), e);
                }
            }
        }
    }
}
