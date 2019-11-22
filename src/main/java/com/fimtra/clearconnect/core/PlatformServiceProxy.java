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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.TransportChannelBuilderFactoryLoader;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.IPlatformServiceProxy;
import com.fimtra.clearconnect.event.EventListenerUtils;
import com.fimtra.clearconnect.event.IRecordAvailableListener;
import com.fimtra.clearconnect.event.IRecordConnectionStatusListener;
import com.fimtra.clearconnect.event.IRecordSubscriptionListener;
import com.fimtra.clearconnect.event.IRecordSubscriptionListener.SubscriptionInfo;
import com.fimtra.clearconnect.event.IRpcAvailableListener;
import com.fimtra.clearconnect.event.IServiceConnectionStatusListener;
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
import com.fimtra.datafission.core.session.ISessionListener;
import com.fimtra.util.LazyObject;
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
    final LazyObject<NotifyingCache<IRecordAvailableListener, String>> recordAvailableNotifyingCache;
    final LazyObject<NotifyingCache<IRpcAvailableListener, IRpcInstance>> rpcAvailableNotifyingCache;
    final LazyObject<NotifyingCache<IRecordSubscriptionListener, SubscriptionInfo>> subscriptionNotifyingCache;
    final LazyObject<NotifyingCache<IRecordConnectionStatusListener, IValue>> recordConnectionStatusNotifyingCache;
    final LazyObject<NotifyingCache<IServiceConnectionStatusListener, Connection>> serviceConnectionStatusNotifyingCache;
    private final String platformName;
    final String serviceFamily;

    Map<String, IRpcInstance> allRpcs;
    
    @SuppressWarnings({ "rawtypes" })
    PlatformServiceProxy(PlatformRegistryAgent registryAgent, String serviceFamily, ICodec codec, final String host,
        final int port, TransportTechnologyEnum transportTechnology)
    {
        this.platformName = registryAgent.getPlatformName();
        this.serviceFamily = serviceFamily;
        this.registryAgent = registryAgent;
        this.proxyContext =
            new ProxyContext(PlatformUtils.composeProxyName(serviceFamily, registryAgent.getAgentName()), codec, host,
                port, transportTechnology, serviceFamily);

        // set the channel builder factory to use an end-point factory that gets end-points from the
        // registry
        this.proxyContext.setTransportChannelBuilderFactory(
            TransportChannelBuilderFactoryLoader.load(codec.getFrameEncodingFormat(), () -> {
                return getServiceEndPointAddress();
            }));

        this.rpcAvailableNotifyingCache =
            new LazyObject<>(() -> PlatformUtils.createRpcAvailableNotifyingCache(this.proxyContext,
                IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS, this), (ref) -> ref.destroy());
        this.subscriptionNotifyingCache =
            new LazyObject<>(() -> PlatformUtils.createSubscriptionNotifyingCache(this.proxyContext,
                IRemoteSystemRecordNames.REMOTE_CONTEXT_SUBSCRIPTIONS, this), (ref) -> ref.destroy());
        this.recordAvailableNotifyingCache =
            new LazyObject<>(() -> PlatformUtils.createRecordAvailableNotifyingCache(this.proxyContext,
                IRemoteSystemRecordNames.REMOTE_CONTEXT_RECORDS, this), (ref) -> ref.destroy());
        this.recordConnectionStatusNotifyingCache =
            new LazyObject<>(() -> PlatformUtils.createRecordConnectionStatusNotifyingCache(this.proxyContext, this),
                (ref) -> ref.destroy());
        this.serviceConnectionStatusNotifyingCache =
            new LazyObject<>(() -> PlatformUtils.createServiceConnectionStatusNotifyingCache(this.proxyContext, this),
                (ref) -> ref.destroy());

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this));
    }

    @Override
    public Future<Map<String, Boolean>> addRecordListener(IRecordListener listener, String... recordNames)
    {
        return this.proxyContext.addObserver(listener, recordNames);
    }

    @Override
    public Future<Map<String, Boolean>> addRecordListener(String permissionToken, IRecordListener listener,
        String... recordNames)
    {
        return this.proxyContext.addObserver(permissionToken, listener, recordNames);
    }

    @Override
    public CountDownLatch removeRecordListener(IRecordListener listener, String... recordNames)
    {
        return this.proxyContext.removeObserver(listener, recordNames);
    }

    @Override
    public Set<String> getAllRecordNames()
    {
        return this.recordAvailableNotifyingCache.get().keySet();
    }

    @Override
    public boolean addRecordAvailableListener(IRecordAvailableListener recordListener)
    {
        return this.recordAvailableNotifyingCache.get().addListener(recordListener);
    }

    @Override
    public boolean removeRecordAvailableListener(IRecordAvailableListener recordListener)
    {
        return this.recordAvailableNotifyingCache.get().removeListener(recordListener);
    }

    @Override
    public boolean addRecordSubscriptionListener(IRecordSubscriptionListener listener)
    {
        return this.subscriptionNotifyingCache.get().addListener(listener);
    }

    @Override
    public boolean removeRecordSubscriptionListener(IRecordSubscriptionListener listener)
    {
        return this.subscriptionNotifyingCache.get().removeListener(listener);
    }

    @Override
    public boolean addRecordConnectionStatusListener(IRecordConnectionStatusListener listener)
    {
        return this.recordConnectionStatusNotifyingCache.get().addListener(listener);
    }

    @Override
    public boolean removeRecordConnectionStatusListener(IRecordConnectionStatusListener listener)
    {
        return this.recordConnectionStatusNotifyingCache.get().removeListener(listener);
    }

    @Override
    public Map<String, IRpcInstance> getAllRpcs()
    {
        synchronized (this.rpcAvailableNotifyingCache)
        {
            if (this.allRpcs == null)
            {
                final NotifyingCache<IRpcAvailableListener, IRpcInstance> notifyingCache =
                    this.rpcAvailableNotifyingCache.get();
                final ConcurrentHashMap<String, IRpcInstance> innerMap = new ConcurrentHashMap<>();
                notifyingCache.addListener(EventListenerUtils.synchronizedListener(new IRpcAvailableListener()
                {
                    @Override
                    public void onRpcUnavailable(IRpcInstance rpc)
                    {
                        innerMap.remove(rpc.getName());
                    }

                    @Override
                    public void onRpcAvailable(IRpcInstance rpc)
                    {
                        innerMap.put(rpc.getName(), rpc);
                    }
                }));
                innerMap.putAll(notifyingCache.getCacheSnapshot());
                this.allRpcs = Collections.unmodifiableMap(innerMap);
            }
        }
        return this.allRpcs;
    }

    @Override
    public IRpcInstance getRpc(String rpcName)
    {
        return this.rpcAvailableNotifyingCache.get().get(rpcName);
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
        return this.rpcAvailableNotifyingCache.get().addListener(rpcListener);
    }

    @Override
    public boolean removeRpcAvailableListener(IRpcAvailableListener rpcListener)
    {
        return this.rpcAvailableNotifyingCache.get().removeListener(rpcListener);
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
        this.recordAvailableNotifyingCache.destroy();
        this.recordConnectionStatusNotifyingCache.destroy();
        this.rpcAvailableNotifyingCache.destroy();
        this.serviceConnectionStatusNotifyingCache.destroy();
        this.subscriptionNotifyingCache.destroy();
    }

    @Override
    public IRecord getRecordImage(String recordName, long timeoutMillis)
    {
        return this.proxyContext.getRemoteRecordImage(recordName, timeoutMillis);
    }

    @Override
    public String toString()
    {
        return "PlatformServiceProxy [platform{" + this.platformName + "} service{" + this.serviceFamily + "}] "
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
        return this.subscriptionNotifyingCache.get().getCacheSnapshot();
    }

    @Override
    public String getShortSocketDescription()
    {
        return this.proxyContext.getShortSocketDescription();
    }

    @Deprecated
    @Override
    public ScheduledExecutorService getUtilityExecutor()
    {
        return this.proxyContext.getUtilityExecutor();
    }

    @Override
    public boolean addServiceConnectionStatusListener(IServiceConnectionStatusListener listener)
    {
        return this.serviceConnectionStatusNotifyingCache.get().addListener(listener);
    }

    @Override
    public boolean removeServiceConnectionStatusListener(IServiceConnectionStatusListener listener)
    {
        return this.serviceConnectionStatusNotifyingCache.get().removeListener(listener);
    }

    @Override
    public boolean isConnected()
    {
        return this.proxyContext.isConnected();
    }

    @Override
    public String getComponentName()
    {
        return this.proxyContext.getName();
    }

    @Override
    public boolean addSessionListener(ISessionListener listener)
    {
        return this.proxyContext.addSessionListener(listener);
    }

    @Override
    public boolean removeSessionListener(ISessionListener listener)
    {
        return this.proxyContext.removeSessionListener(listener);
    }

    EndPointAddress getServiceEndPointAddress()
    {
        if (this.registryAgent.serviceAvailableListeners.keySet().contains(this.serviceFamily))
        {
            Log.log(this, "Obtaining service info record for '", PlatformServiceProxy.this.serviceFamily, "'");
            final Map<String, IValue> serviceInfoRecord =
                this.registryAgent.getPlatformServiceInstanceInfoRecordImageForService(this.serviceFamily);

            if (serviceInfoRecord == null)
            {
                Log.log(this, "No service info record found for '", this.serviceFamily, "'");
                return null;
            }
            
            final String node = PlatformUtils.getHostNameFromServiceInfoRecord(serviceInfoRecord);
            final int port = PlatformUtils.getPortFromServiceInfoRecord(serviceInfoRecord);
            final EndPointAddress next = new EndPointAddress(node, port);
            Log.log(this, "Service '", this.serviceFamily, "' ", ObjectUtils.safeToString(next));
            return next;
        }
        else
        {
            Log.log(this, "Cannot re-connect to unavailable service '", this.serviceFamily, "'");
            return null;
        }
    }
}
