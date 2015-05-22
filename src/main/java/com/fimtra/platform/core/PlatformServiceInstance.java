/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields;
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
import com.fimtra.platform.IPlatformServiceInstance;
import com.fimtra.platform.RedundancyModeEnum;
import com.fimtra.platform.WireProtocolEnum;
import com.fimtra.platform.event.IFtStatusListener;
import com.fimtra.platform.event.IRecordAvailableListener;
import com.fimtra.platform.event.IRecordSubscriptionListener;
import com.fimtra.platform.event.IRecordSubscriptionListener.SubscriptionInfo;
import com.fimtra.platform.event.IRpcAvailableListener;
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
     * Defines the fields for the service stats record
     * 
     * @author Ramon Servadei
     */
    static interface IServiceStatsRecordFields
    {
        String SUBSCRIPTION_COUNT = "Subscriptions";
        String MESSAGE_COUNT = "Msgs published";
        String MSGS_PER_MIN = "Msgs per min";
        String KB_COUNT = "Kb published";
        String KB_PER_MIN = "Kb per min";
        String UPTIME = "Uptime(sec)";
    }

    static final String RPC_FT_SERVICE_STATUS = "ftServiceInstanceStatus";

    boolean active;
    final long startTimeMillis;
    final Context context;
    boolean isFtMasterInstance;
    final String platformName;
    final String serviceFamily;
    final String serviceMember;
    final Publisher publisher;
    final WireProtocolEnum wireProtocol;
    final RedundancyModeEnum redundancyMode;
    final DataRadarScanManager dataRadarScanManager;
    final List<IFtStatusListener> ftStatusListeners;
    final NotifyingCache<IRecordAvailableListener, String> recordAvailableNotifyingCache;
    final NotifyingCache<IRpcAvailableListener, IRpcInstance> rpcAvailableNotifyingCache;
    final NotifyingCache<IRecordSubscriptionListener, SubscriptionInfo> subscriptionNotifyingCache;
    final IRecord stats;

    final ScheduledFuture<?> statsUpdateTask;

    PlatformServiceInstance(String platformName, String serviceFamily, String serviceMember,
        WireProtocolEnum wireProtocol, String host, int port)
    {
        this(platformName, serviceFamily, serviceMember, wireProtocol, RedundancyModeEnum.FAULT_TOLERANT, host, port,
            null, null, null);
    }

    @SuppressWarnings({ "unchecked" })
    PlatformServiceInstance(String platformName, String serviceFamily, String serviceMember,
        WireProtocolEnum wireProtocol, RedundancyModeEnum redundancyMode, String host, int port,
        ThimbleExecutor coreExecutor, ThimbleExecutor rpcExecutor, ScheduledExecutorService utilityExecutor)
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

                    final long messagesPublished = PlatformServiceInstance.this.publisher.getMessagesPublished();
                    final long bytesPublished = PlatformServiceInstance.this.publisher.getBytesPublished();
                    final double perMin = 60d / (DataFissionProperties.Values.STATS_LOGGING_PERIOD_SECS);
                    PlatformServiceInstance.this.stats.put(IContextConnectionsRecordFields.MSGS_PER_MIN,
                        DoubleValue.valueOf((messagesPublished - this.lastMessagesPublished) * perMin));
                    PlatformServiceInstance.this.stats.put(IContextConnectionsRecordFields.KB_PER_MIN,
                        DoubleValue.valueOf(((bytesPublished - this.lastBytesPublished) / 1024) * perMin));

                    PlatformServiceInstance.this.stats.put(IServiceStatsRecordFields.SUBSCRIPTION_COUNT,
                        LongValue.valueOf(subscriptionCount));
                    PlatformServiceInstance.this.stats.put(
                        IServiceStatsRecordFields.UPTIME,
                        LongValue.valueOf((System.currentTimeMillis() - PlatformServiceInstance.this.startTimeMillis) / 1000));
                    PlatformServiceInstance.this.stats.put(IServiceStatsRecordFields.MESSAGE_COUNT,
                        LongValue.valueOf(messagesPublished));
                    PlatformServiceInstance.this.stats.put(IServiceStatsRecordFields.KB_COUNT,
                        LongValue.valueOf(bytesPublished / 1024));

                    this.lastMessagesPublished = messagesPublished;
                    this.lastBytesPublished = bytesPublished;

                    PlatformServiceInstance.this.context.publishAtomicChange(PlatformServiceInstance.this.stats);
                }
            }, DataFissionProperties.Values.STATS_LOGGING_PERIOD_SECS,
            DataFissionProperties.Values.STATS_LOGGING_PERIOD_SECS, TimeUnit.SECONDS);

        this.publisher = new Publisher(this.context, this.wireProtocol.getCodec(), host, port);
        this.recordAvailableNotifyingCache =
            PlatformUtils.createRecordAvailableNotifyingCache(this.context, ISystemRecordNames.CONTEXT_RECORDS, this);
        this.rpcAvailableNotifyingCache =
            PlatformUtils.createRpcAvailableNotifyingCache(this.context, ISystemRecordNames.CONTEXT_RPCS, this);
        this.subscriptionNotifyingCache =
            PlatformUtils.createSubscriptionNotifyingCache(this.context, ISystemRecordNames.CONTEXT_SUBSCRIPTIONS, this);
        this.active = true;

        this.dataRadarScanManager = new DataRadarScanManager(this);

        if (redundancyMode == RedundancyModeEnum.FAULT_TOLERANT)
        {
            RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
            {
                @Override
                public IValue execute(final IValue... args) throws TimeOutException, ExecutionException
                {
                    PlatformServiceInstance.this.context.getUtilityExecutor().execute(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            PlatformServiceInstance.this.isFtMasterInstance =
                                Boolean.valueOf(args[0].textValue()).booleanValue();
                            Log.banner(PlatformServiceInstance.this, PlatformServiceInstance.this.toString() + " "
                                + (PlatformServiceInstance.this.isFtMasterInstance ? "ACTIVE" : "STANDBY"));

                            for (IFtStatusListener iFtStatusListener : PlatformServiceInstance.this.ftStatusListeners)
                            {
                                try
                                {
                                    if (PlatformServiceInstance.this.isFtMasterInstance)
                                    {
                                        iFtStatusListener.onActive(PlatformServiceInstance.this.serviceFamily,
                                            PlatformServiceInstance.this.serviceMember);
                                    }
                                    else
                                    {
                                        iFtStatusListener.onStandby(PlatformServiceInstance.this.serviceFamily,
                                            PlatformServiceInstance.this.serviceMember);
                                    }
                                }
                                catch (Exception e)
                                {
                                    Log.log(PlatformServiceInstance.this,
                                        "Could not notify " + ObjectUtils.safeToString(iFtStatusListener), e);
                                }
                            }
                        }
                    });
                    return PlatformUtils.OK;
                }
            }, TypeEnum.TEXT, RPC_FT_SERVICE_STATUS, TypeEnum.TEXT);
            this.context.createRpc(rpc);
        }

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this));
    }

    @Override
    public CountDownLatch addRecordListener(IRecordListener listener, String... recordNames)
    {
        return this.context.addObserver(listener, recordNames);
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
        final IRecord rpcRecord = this.context.getRecord(ISystemRecordNames.CONTEXT_RPCS);
        final Map<String, IRpcInstance> rpcs = new HashMap<String, IRpcInstance>();
        for (String rpcName : rpcRecord.keySet())
        {
            rpcs.put(rpcName, this.context.getRpc(rpcName));
        }
        return rpcs;
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
        this.dataRadarScanManager.destroy();
        this.publisher.destroy();
        this.context.destroy();
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
        return "PlatformServiceInstance [" + this.platformName + "|" + this.serviceFamily + "|" + this.serviceMember
            + "] " + getEndPointAddress();
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
        return this.publisher.getEndPointAddress();
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
                if (PlatformServiceInstance.this.isFtMasterInstance)
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
}
