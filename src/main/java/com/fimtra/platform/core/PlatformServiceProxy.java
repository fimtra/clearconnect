/*
 * Copyright (c) 2013 Ramon Servadei, Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.core;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointAddressFactory;
import com.fimtra.channel.TransportChannelBuilderFactoryLoader;
import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.IStatusAttribute.Connection;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.core.ProxyContext.IRemoteSystemRecordNames;
import com.fimtra.platform.IPlatformServiceProxy;
import com.fimtra.platform.event.IRecordAvailableListener;
import com.fimtra.platform.event.IRecordConnectionStatusListener;
import com.fimtra.platform.event.IRecordSubscriptionListener;
import com.fimtra.platform.event.IRecordSubscriptionListener.SubscriptionInfo;
import com.fimtra.platform.event.IRpcAvailableListener;
import com.fimtra.platform.event.IServiceConnectionStatusListener;
import com.fimtra.util.Log;
import com.fimtra.util.NotifyingCache;
import com.fimtra.util.ObjectUtils;

/**
 * The standard platform service proxy.
 * <p>
 * Reconnection occurs automatically via its internal {@link ProxyContext}.
 * 
 * @author Ramon Servadei, Paul Mackinlay
 */
final class PlatformServiceProxy implements IPlatformServiceProxy
{
    final PlatformRegistryAgent registryAgent;
    final ProxyContext proxyContext;
    final NotifyingCache<IRecordAvailableListener, String> recordAvailableNotifyingCache;
    final NotifyingCache<IRpcAvailableListener, IRpcInstance> rpcAvailableNotifyingCache;
    final NotifyingCache<IRecordSubscriptionListener, SubscriptionInfo> subscriptionNotifyingCache;
    final NotifyingCache<IRecordConnectionStatusListener, IValue> recordConnectionStatusNotifyingCache;
    final NotifyingCache<IServiceConnectionStatusListener, Connection> serviceConnectionStatusNotifyingCache;
    private final String platformName;
    final String serviceFamily;

    @SuppressWarnings({ "rawtypes" })
    PlatformServiceProxy(PlatformRegistryAgent registryAgent, String serviceFamily, ICodec codec, final String host,
        final int port) throws IOException
    {
        this.platformName = registryAgent.getPlatformName();
        this.serviceFamily = serviceFamily;
        this.registryAgent = registryAgent;
        this.proxyContext =
            new ProxyContext(PlatformUtils.composeProxyName(serviceFamily, registryAgent.getAgentName()), codec, host,
                port);

        // set the channel builder factory to use an end-point factory that gets end-points from the
        // registry
        this.proxyContext.setTransportChannelBuilderFactory(TransportChannelBuilderFactoryLoader.load(
            codec.getFrameEncodingFormat(), new IEndPointAddressFactory()
            {
                @Override
                public EndPointAddress next()
                {
                    Log.log(this, "Obtaining service info record for '", PlatformServiceProxy.this.serviceFamily, "'");
                    final Map<String, IValue> serviceInfoRecord =
                        PlatformServiceProxy.this.registryAgent.getPlatformServiceInstanceInfoRecordImageForService(PlatformServiceProxy.this.serviceFamily);
                    if (serviceInfoRecord == null)
                    {
                        Log.log(this, "No service info record found for '", PlatformServiceProxy.this.serviceFamily,
                            "'");
                        return null;
                    }
                    final String node = PlatformUtils.getHostNameFromServiceInfoRecord(serviceInfoRecord);
                    final int port = PlatformUtils.getPortFromServiceInfoRecord(serviceInfoRecord);
                    final EndPointAddress next = new EndPointAddress(node, port);
                    Log.log(this, "Service '", PlatformServiceProxy.this.serviceFamily, "' ",
                        ObjectUtils.safeToString(next));
                    return next;
                }
            }));

        this.rpcAvailableNotifyingCache =
            PlatformUtils.createRpcAvailableNotifyingCache(this.proxyContext,
                IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS, this);
        this.subscriptionNotifyingCache =
            PlatformUtils.createSubscriptionNotifyingCache(this.proxyContext,
                IRemoteSystemRecordNames.REMOTE_CONTEXT_SUBSCRIPTIONS, this);
        this.recordAvailableNotifyingCache =
            PlatformUtils.createRecordAvailableNotifyingCache(this.proxyContext,
                IRemoteSystemRecordNames.REMOTE_CONTEXT_RECORDS, this);
        this.recordConnectionStatusNotifyingCache =
            PlatformUtils.createRecordConnectionStatusNotifyingCache(this.proxyContext, this);
        this.serviceConnectionStatusNotifyingCache =
            PlatformUtils.createServiceConnectionStatusNotifyingCache(this.proxyContext, this);

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this));
    }

    @Override
    public CountDownLatch addRecordListener(IRecordListener listener, String... recordNames)
    {
        return this.proxyContext.addObserver(listener, recordNames);
    }

    @Override
    public CountDownLatch removeRecordListener(IRecordListener listener, String... recordNames)
    {
        return this.proxyContext.removeObserver(listener, recordNames);
    }

    @Override
    public Set<String> getAllRecordNames()
    {
        return this.recordAvailableNotifyingCache.getCacheSnapshot().keySet();
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
    public boolean addRecordConnectionStatusListener(IRecordConnectionStatusListener listener)
    {
        return this.recordConnectionStatusNotifyingCache.addListener(listener);
    }

    @Override
    public boolean removeRecordConnectionStatusListener(IRecordConnectionStatusListener listener)
    {
        return this.recordConnectionStatusNotifyingCache.removeListener(listener);
    }

    @Override
    public Map<String, IRpcInstance> getAllRpcs()
    {
        return this.rpcAvailableNotifyingCache.getCacheSnapshot();
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

    /**
     * Destroy this component. This will close the TCP connection and release all resources used by
     * the object. There is no specification for what callbacks will be invoked for any attached
     * listeners.
     */
    public void destroy()
    {
        Log.log(this, "Destroying ", ObjectUtils.safeToString(this));
        this.proxyContext.destroy();
    }

    @Override
    public IRecord getRecordImage(String recordName, long timeoutMillis)
    {
        return this.proxyContext.getRemoteRecordImage(recordName, timeoutMillis);
    }

    @Override
    public String toString()
    {
        return "PlatformServiceProxy [" + this.platformName + "|" + this.serviceFamily + "] "
            + this.proxyContext.getChannelString();
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
    public int getReconnectPeriodMillis()
    {
        return this.proxyContext.getReconnectPeriodMillis();
    }

    @Override
    public void setReconnectPeriodMillis(int reconnectPeriodMillis)
    {
        this.proxyContext.setReconnectPeriodMillis(reconnectPeriodMillis);
    }

    @Override
    public boolean isActive()
    {
        return this.proxyContext.isActive();
    }

    @Override
    public Map<String, SubscriptionInfo> getAllSubscriptions()
    {
        return this.subscriptionNotifyingCache.getCacheSnapshot();
    }

    @Override
    public String getShortSocketDescription()
    {
        return this.proxyContext.getShortSocketDescription();
    }

    @Override
    public ScheduledExecutorService getUtilityExecutor()
    {
        return this.proxyContext.getUtilityExecutor();
    }

    @Override
    public boolean addServiceConnectionStatusListener(IServiceConnectionStatusListener listener)
    {
        return this.serviceConnectionStatusNotifyingCache.addListener(listener);
    }

    @Override
    public boolean removeServiceConnectionStatusListener(IServiceConnectionStatusListener listener)
    {
        return this.serviceConnectionStatusNotifyingCache.removeListener(listener);
    }
}
