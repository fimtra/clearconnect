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

import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarFile;

import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.IPlatformServiceComponent;
import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.config.IConfig;
import com.fimtra.clearconnect.config.impl.ConfigServiceProxy;
import com.fimtra.clearconnect.config.impl.ConfigUtils;
import com.fimtra.clearconnect.core.PlatformRegistry.ServiceInfoRecordFields;
import com.fimtra.clearconnect.event.EventListenerUtils;
import com.fimtra.clearconnect.event.IProxyConnectionListener;
import com.fimtra.clearconnect.event.IRecordAvailableListener;
import com.fimtra.clearconnect.event.IRecordConnectionStatusListener;
import com.fimtra.clearconnect.event.IRecordSubscriptionListener;
import com.fimtra.clearconnect.event.IRecordSubscriptionListener.SubscriptionInfo;
import com.fimtra.clearconnect.event.IRpcAvailableListener;
import com.fimtra.clearconnect.event.IServiceAvailableListener;
import com.fimtra.clearconnect.event.IServiceConnectionStatusListener;
import com.fimtra.clearconnect.event.IServiceInstanceAvailableListener;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.IStatusAttribute;
import com.fimtra.datafission.core.IStatusAttribute.Connection;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.ClassUtils;
import com.fimtra.util.LazyObject.IDestructor;
import com.fimtra.util.Log;
import com.fimtra.util.NotifyingCache;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SystemUtils;
import com.fimtra.util.ThreadUtils;
import com.fimtra.util.is;

/**
 * Holds general utility methods used by various components in the platform core.
 * 
 * @author Ramon Servadei, Paul Mackinlay
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class PlatformUtils
{
    public final static String VERSION;

    static
    {
        String version = "";
        final String newline = SystemUtils.lineSeparator();
        try
        {
            final Enumeration<URL> manifests = PlatformUtils.class.getClassLoader().getResources(JarFile.MANIFEST_NAME);
            while (manifests.hasMoreElements())
            {
                version = ClassUtils.getManifestEntriesAsString(manifests.nextElement(), ClassUtils.fimtraVersionKeys);
                if (version.toLowerCase().contains("clearconnect@fimtra.com"))
                {
                    final String[] tokens = version.split(newline);
                    for (String token : tokens)
                    {
                        if (token.toLowerCase().startsWith("version"))
                        {
                            version = token;
                            break;
                        }
                    }
                    break;
                }
                else
                {
                    version = "";
                }
            }
        }
        catch (Exception e)
        {
            Log.log(ClassUtils.class, "Could not get manifest resources", e);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(newline).append("============ START System Properties ============").append(newline);
        TreeSet<String> sortedKeys = new TreeSet<>();
        final Enumeration<Object> keys = System.getProperties().keys();
        while (keys.hasMoreElements())
        {
            sortedKeys.add(keys.nextElement().toString());
        }
        for (String key : sortedKeys)
        {
            sb.append(key).append("=").append(System.getProperty(key)).append(newline);
        }
        sb.append("============ END System Properties ============");
        Log.log(PlatformUtils.class, sb.toString());

        sb = new StringBuilder();
        sb.append("ClearConnect ").append(version).append(newline);
        sb.append(newline).append("Licensed under the Apache License, Version 2.0 (the \"License\");").append(newline);
        sb.append("\thttp://www.apache.org/licenses/LICENSE-2.0").append(newline);
        sb.append(newline).append(
            "Developers: ramon.servadei@fimtra.com, paul.mackinlay@fimtra.com, james.lupton@fimtra.com").append(
                newline).append(newline);

        sb.append("Localhost IP: ").append(TcpChannelUtils.LOCALHOST_IP).append(newline);
        sb.append("CPU logical count: ").append(Runtime.getRuntime().availableProcessors()).append(newline);
        sb.append("Core thread count: ").append(DataFissionProperties.Values.CORE_THREAD_COUNT);
        Log.banner(PlatformUtils.class, sb.toString());

        String versionNumber = "?.?.?";
        for (int i = 0; i < version.length(); i++)
        {
            if (Character.isDigit(version.charAt(i)))
            {
                versionNumber = version.substring(i);
                break;
            }
        }
        VERSION = versionNumber;
    }

    public static final TextValue OK = TextValue.valueOf("OK");
    static final String SERVICE_INSTANCE_PREFIX = "[";
    static final String SERVICE_INSTANCE_SUFFIX = "]";
    static final String SERVICE_CLIENT_DELIMITER = "->";

    /**
     * Used to provide an efficient "one-shot" latch
     * 
     * @author Ramon Servadei
     */
    private static final class OneShotLatch
    {
        CountDownLatch latch;

        OneShotLatch()
        {
            this.latch = new CountDownLatch(1);
        }

        void countDown()
        {
            if (this.latch != null)
            {
                this.latch.countDown();
            }
        }

        boolean await(long timeout, TimeUnit unit) throws InterruptedException
        {
            if (this.latch != null)
            {
                try
                {
                    return this.latch.await(timeout, unit);
                }
                finally
                {
                    this.latch = null;
                }
            }
            return false;
        }
    }

    /**
     * Construct a {@link NotifyingCache} that handles when services are discovered.
     */
    static NotifyingCache<IServiceAvailableListener, String> createServiceAvailableNotifyingCache(
        final IObserverContext context, final String servicesRecordName, final Object logContext)
    {
        final AtomicReference<IRecordListener> listenerReference = new AtomicReference<>();
        final OneShotLatch updateWaitLatch = new OneShotLatch();
        final NotifyingCache<IServiceAvailableListener, String> serviceAvailableListeners =
            new NotifyingCache<IServiceAvailableListener, String>(
                (IDestructor) (ref) -> context.removeObserver(listenerReference.get(), servicesRecordName))
            {
                @Override
                protected void notifyListenerDataAdded(IServiceAvailableListener listener, String key, String data)
                {
                    listener.onServiceAvailable(data);
                }

                @Override
                protected void notifyListenerDataRemoved(IServiceAvailableListener listener, String key, String data)
                {
                    listener.onServiceUnavailable(data);
                }
            };
        final IRecordListener observer = (imageCopy, atomicChange) -> {
            final Set<String> newServices = atomicChange.getPutEntries().keySet();
            final List<String> toLog = new LinkedList<>();
            for (String serviceFamily : newServices)
            {
                if (ContextUtils.isSystemRecordName(serviceFamily))
                {
                    continue;
                }
                if (serviceAvailableListeners.notifyListenersDataAdded(serviceFamily, serviceFamily))
                {
                    toLog.add(serviceFamily);
                }
            }
            if (toLog.size() > 0)
            {
                Log.log(logContext, "Services available: ", toLog.toString());
            }
            toLog.clear();
            final Set<String> removedServices = atomicChange.getRemovedEntries().keySet();
            for (String serviceFamily : removedServices)
            {
                if (ContextUtils.isSystemRecordName(serviceFamily))
                {
                    continue;
                }
                if (serviceAvailableListeners.notifyListenersDataRemoved(serviceFamily))
                {
                    toLog.add(serviceFamily);
                }
            }
            if (toLog.size() > 0)
            {
                Log.log(logContext, "Services lost: ", toLog.toString());
            }
            updateWaitLatch.countDown();
        };
        listenerReference.set(observer);
        context.addObserver(observer, servicesRecordName);
        awaitUpdateLatch(logContext, servicesRecordName, updateWaitLatch);
        return serviceAvailableListeners;
    }

    /**
     * Construct a {@link NotifyingCache} that handles when services INSTANCES are discovered.
     */
    static NotifyingCache<IServiceInstanceAvailableListener, String> createServiceInstanceAvailableNotifyingCache(
        final IObserverContext context, final String serviceInstancesPerServiceRecordName, final Object logContext)
    {
        final AtomicReference<IRecordListener> listenerReference = new AtomicReference<>();
        final OneShotLatch updateWaitLatch = new OneShotLatch();
        final NotifyingCache<IServiceInstanceAvailableListener, String> serviceInstanceAvailableListeners =
            new NotifyingCache<IServiceInstanceAvailableListener, String>(
                (IDestructor) (ref) -> context.removeObserver(listenerReference.get(),
                    serviceInstancesPerServiceRecordName))
            {
                @Override
                protected void notifyListenerDataAdded(IServiceInstanceAvailableListener listener, String key,
                    String data)
                {
                    listener.onServiceInstanceAvailable(data);
                }

                @Override
                protected void notifyListenerDataRemoved(IServiceInstanceAvailableListener listener, String key,
                    String data)
                {
                    listener.onServiceInstanceUnavailable(data);
                }
            };
        final IRecordListener observer = (imageCopy, atomicChange) -> {
            /*
             * sub-map key: serviceFamily sub-map structure: {key=service member name (NOT the
             * service instance ID), value=system time when registered/last used}
             */
            IRecordChange changesForService;
            String serviceInstanceId;
            for (String serviceFamily : atomicChange.getSubMapKeys())
            {
                changesForService = atomicChange.getSubMapAtomicChange(serviceFamily);
                Set<String> newServices = changesForService.getPutEntries().keySet();
                for (String serviceMember : newServices)
                {
                    serviceInstanceId = PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember);
                    serviceInstanceAvailableListeners.notifyListenersDataAdded(serviceInstanceId, serviceInstanceId);
                }
                Set<String> removedServices = changesForService.getRemovedEntries().keySet();
                for (String serviceMember : removedServices)
                {
                    serviceInstanceId = PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember);
                    serviceInstanceAvailableListeners.notifyListenersDataRemoved(serviceInstanceId);
                }
            }
            updateWaitLatch.countDown();
        };
        listenerReference.set(observer);
        context.addObserver(observer, serviceInstancesPerServiceRecordName);
        awaitUpdateLatch(logContext, serviceInstancesPerServiceRecordName, updateWaitLatch);
        return serviceInstanceAvailableListeners;
    }

    /**
     * Construct a {@link NotifyingCache} that handles when records are added or removed from the
     * context.
     */
    static NotifyingCache<IRecordAvailableListener, String> createRecordAvailableNotifyingCache(
        final IObserverContext context, final String contextRecordsRecordName, final Object logContext)
    {
        final AtomicReference<IRecordListener> listenerReference = new AtomicReference<>();
        final OneShotLatch updateWaitLatch = new OneShotLatch();
        final NotifyingCache<IRecordAvailableListener, String> recordAvailableNotifyingCache =
            new NotifyingCache<IRecordAvailableListener, String>(
                (IDestructor) (ref) -> context.removeObserver(listenerReference.get(), contextRecordsRecordName))
            {
                @Override
                protected void notifyListenerDataAdded(IRecordAvailableListener listener, String key, String data)
                {
                    listener.onRecordAvailable(data);
                }

                @Override
                protected void notifyListenerDataRemoved(IRecordAvailableListener listener, String key, String data)
                {
                    listener.onRecordUnavailable(data);
                }
            };
        final IRecordListener observer = (imageCopy, atomicChange) -> {
            Set<String> newRecords = atomicChange.getPutEntries().keySet();
            for (String recordName : newRecords)
            {
                recordAvailableNotifyingCache.notifyListenersDataAdded(recordName, recordName);
            }
            Set<String> removedRecords = atomicChange.getRemovedEntries().keySet();
            for (String recordName : removedRecords)
            {
                recordAvailableNotifyingCache.notifyListenersDataRemoved(recordName);
            }
            updateWaitLatch.countDown();
        };
        listenerReference.set(observer);
        context.addObserver(observer, contextRecordsRecordName);
        awaitUpdateLatch(logContext, contextRecordsRecordName, updateWaitLatch);
        return recordAvailableNotifyingCache;
    }

    /**
     * Construct a {@link NotifyingCache} that handles when RPCs are added or removed from the
     * context.
     */
    static NotifyingCache<IRpcAvailableListener, IRpcInstance> createRpcAvailableNotifyingCache(
        final IObserverContext context, final String contextRpcRecordName, final Object logContext)
    {
        final AtomicReference<IRecordListener> listenerReference = new AtomicReference<>();
        final OneShotLatch updateWaitLatch = new OneShotLatch();
        final NotifyingCache<IRpcAvailableListener, IRpcInstance> rpcAvailableNotifyingCache =
            new NotifyingCache<IRpcAvailableListener, IRpcInstance>(
                (IDestructor) (ref) -> context.removeObserver(listenerReference.get(), contextRpcRecordName))
            {
                @Override
                protected void notifyListenerDataAdded(IRpcAvailableListener listener, String key, IRpcInstance data)
                {
                    listener.onRpcAvailable(data);
                }

                @Override
                protected void notifyListenerDataRemoved(IRpcAvailableListener listener, String key, IRpcInstance data)
                {
                    listener.onRpcUnavailable(data);
                }
            };
        final IRecordListener observer = (imageCopy, atomicChange) -> {
            Log.log(logContext, "RPC change: " + atomicChange.toString());
            Set<Entry<String, IValue>> newRpcs = atomicChange.getPutEntries().entrySet();
            for (Entry<String, IValue> newRpc : newRpcs)
            {
                final IRpcInstance rpc = context.getRpc(newRpc.getKey());
                if (rpc != null)
                {
                    if (rpcAvailableNotifyingCache.notifyListenersDataAdded(rpc.getName(), rpc))
                    {
                        Log.log(logContext, "RPC available: '", newRpc.getKey(), "' in ",
                            ObjectUtils.safeToString(logContext));
                    }
                }
                else
                {
                    Log.log(logContext, "RPC '", newRpc.getKey(), "' available but not found in ",
                        ObjectUtils.safeToString(logContext));
                }
            }
            Set<Entry<String, IValue>> removedRpcs = atomicChange.getRemovedEntries().entrySet();
            for (Entry<String, IValue> removedRpc : removedRpcs)
            {
                if (rpcAvailableNotifyingCache.notifyListenersDataRemoved(removedRpc.getKey()))
                {
                    Log.log(logContext, "RPC removed: '", removedRpc.getKey(), "' in ",
                        ObjectUtils.safeToString(logContext));
                }
            }
            updateWaitLatch.countDown();
        };
        listenerReference.set(observer);
        context.addObserver(observer, contextRpcRecordName);
        awaitUpdateLatch(logContext, contextRpcRecordName, updateWaitLatch);
        return rpcAvailableNotifyingCache;
    }

    /**
     * Construct a {@link NotifyingCache} that handles when records are subscribed for in the
     * context.
     */
    static NotifyingCache<IRecordSubscriptionListener, SubscriptionInfo> createSubscriptionNotifyingCache(
        final IObserverContext context, final String contextSubscriptionsRecordName, final Object logContext)
    {
        final AtomicReference<IRecordListener> listenerReference = new AtomicReference<>();
        final OneShotLatch updateWaitLatch = new OneShotLatch();
        final NotifyingCache<IRecordSubscriptionListener, SubscriptionInfo> subscriptionNotifyingCache =
            new NotifyingCache<IRecordSubscriptionListener, SubscriptionInfo>(
                (IDestructor) (ref) -> context.removeObserver(listenerReference.get(), contextSubscriptionsRecordName))
            {
                @Override
                protected void notifyListenerDataAdded(IRecordSubscriptionListener listener, String key,
                    SubscriptionInfo data)
                {
                    listener.onRecordSubscriptionChange(data);
                }

                @Override
                protected void notifyListenerDataRemoved(IRecordSubscriptionListener listener, String key,
                    SubscriptionInfo data)
                {
                    // noop
                }
            };
        final IRecordListener observer = (imageCopy, atomicChange) -> {
            Set<Entry<String, IValue>> subscriptions = atomicChange.getPutEntries().entrySet();
            for (Entry<String, IValue> subsription : subscriptions)
            {
                IValue previous = atomicChange.getOverwrittenEntries().get(subsription.getKey());
                int previousSubscriberCount = 0;
                if (previous != null)
                {
                    previousSubscriberCount = (int) previous.longValue();
                }
                int currentSubscriberCount = (int) subsription.getValue().longValue();
                SubscriptionInfo info =
                    new SubscriptionInfo(subsription.getKey(), currentSubscriberCount, previousSubscriberCount);
                subscriptionNotifyingCache.notifyListenersDataAdded(info.getRecordName(), info);
            }

            Set<Entry<String, IValue>> removedSubscriptions = atomicChange.getRemovedEntries().entrySet();
            for (Entry<String, IValue> removed : removedSubscriptions)
            {
                int currentSubscriberCount = 0;
                int previousSubscriberCount = (int) removed.getValue().longValue();
                SubscriptionInfo info =
                    new SubscriptionInfo(removed.getKey(), currentSubscriberCount, previousSubscriberCount);
                subscriptionNotifyingCache.notifyListenersDataAdded(info.getRecordName(), info);
                if (currentSubscriberCount == 0)
                {
                    subscriptionNotifyingCache.notifyListenersDataRemoved(info.getRecordName());
                }
            }
            updateWaitLatch.countDown();
        };
        listenerReference.set(observer);
        context.addObserver(observer, contextSubscriptionsRecordName);
        awaitUpdateLatch(logContext, contextSubscriptionsRecordName, updateWaitLatch);
        return subscriptionNotifyingCache;
    }

    /**
     * Construct the {@link NotifyingCache} that handles record connection status changes
     */
    static NotifyingCache<IRecordConnectionStatusListener, IValue> createRecordConnectionStatusNotifyingCache(
        final IObserverContext proxyContext, final Object logContext)
    {
        final AtomicReference<IRecordListener> listenerReference = new AtomicReference<>();
        final OneShotLatch updateWaitLatch = new OneShotLatch();
        final NotifyingCache<IRecordConnectionStatusListener, IValue> recordStatusNotifyingCache =
            new NotifyingCache<IRecordConnectionStatusListener, IValue>(
                (IDestructor) (ref) -> proxyContext.removeObserver(listenerReference.get(),
                    ProxyContext.RECORD_CONNECTION_STATUS_NAME))
            {
                @Override
                protected void notifyListenerDataRemoved(IRecordConnectionStatusListener listener, String key,
                    IValue data)
                {
                }

                @Override
                protected void notifyListenerDataAdded(IRecordConnectionStatusListener listener, String key,
                    IValue data)
                {
                    if (ProxyContext.RECORD_CONNECTED == (data))
                    {
                        listener.onRecordConnected(key);
                    }
                    else
                    {
                        if (ProxyContext.RECORD_CONNECTING == (data))
                        {
                            listener.onRecordConnecting(key);
                        }
                        else
                        {
                            listener.onRecordDisconnected(key);
                        }
                    }
                }
            };
        final IRecordListener observer = (imageCopy, atomicChange) -> {
            for (Entry<String, IValue> entry : atomicChange.getPutEntries().entrySet())
            {
                recordStatusNotifyingCache.notifyListenersDataAdded(entry.getKey(), entry.getValue());
            }
            updateWaitLatch.countDown();
        };
        listenerReference.set(observer);
        proxyContext.addObserver(observer, ProxyContext.RECORD_CONNECTION_STATUS_NAME);
        awaitUpdateLatch(logContext, ProxyContext.RECORD_CONNECTION_STATUS_NAME, updateWaitLatch);
        return recordStatusNotifyingCache;
    }

    /**
     * Construct the {@link NotifyingCache} that handles proxy connection status changes
     */
    static NotifyingCache<IProxyConnectionListener, IValue> createProxyConnectionNotifyingCache(
        final IObserverContext proxyContext, final Object logContext)
    {
        final AtomicReference<IRecordListener> listenerReference = new AtomicReference<>();
        final OneShotLatch updateWaitLatch = new OneShotLatch();
        final NotifyingCache<IProxyConnectionListener, IValue> proxyConnectionNotifyingCache =
            new NotifyingCache<IProxyConnectionListener, IValue>(
                (IDestructor) (ref) -> proxyContext.removeObserver(listenerReference.get(),
                    ISystemRecordNames.CONTEXT_CONNECTIONS))
            {
                @Override
                protected void notifyListenerDataRemoved(IProxyConnectionListener listener, String key, IValue data)
                {
                    listener.onDisconnected(key);
                }

                @Override
                protected void notifyListenerDataAdded(IProxyConnectionListener listener, String key, IValue data)
                {
                    listener.onConnected(key);
                }
            };
        final IRecordListener observer = new IRecordListener()
        {
            final Map<String, String> current = new HashMap<>();

            @Override
            public void onChange(final IRecord imageCopy, IRecordChange atomicChange)
            {
                final Set<String> subMapKeys = atomicChange.getSubMapKeys();
                final Set<String> currentConnections = imageCopy.getSubMapKeys();

                final Set<String> added = new HashSet<>();
                final Set<String> removed = new HashSet<>();
                IValue proxyId;
                for (String connectionId : subMapKeys)
                {
                    if (!currentConnections.contains(connectionId))
                    {
                        removed.add(connectionId);
                    }
                    else
                    {
                        if (!this.current.containsKey(connectionId))
                        {
                            proxyId = atomicChange.getSubMapAtomicChange(connectionId).getPutEntries().get(
                                IContextConnectionsRecordFields.PROXY_ID);
                            if (proxyId != null)
                            {
                                this.current.put(connectionId, proxyId.textValue());
                                added.add(connectionId);
                            }
                        }
                    }
                }

                for (String connectionId : removed)
                {
                    if (proxyConnectionNotifyingCache.notifyListenersDataRemoved(this.current.remove(connectionId)))
                    {
                        Log.log(logContext, "Proxy DISCONNECTED: ", connectionId);
                    }
                }

                for (String connectionId : added)
                {
                    if (proxyConnectionNotifyingCache.notifyListenersDataAdded(this.current.get(connectionId),
                        LongValue.valueOf(1)))
                    {
                        Log.log(logContext, "Proxy CONNECTED: ", connectionId);
                    }
                }
                updateWaitLatch.countDown();
            }
        };
        listenerReference.set(observer);
        proxyContext.addObserver(observer, ISystemRecordNames.CONTEXT_CONNECTIONS);
        awaitUpdateLatch(logContext, ISystemRecordNames.CONTEXT_CONNECTIONS, updateWaitLatch);
        return proxyConnectionNotifyingCache;
    }

    /**
     * Construct the {@link NotifyingCache} that handles service connection status changes
     */
    static NotifyingCache<IServiceConnectionStatusListener, Connection> createServiceConnectionStatusNotifyingCache(
        final IObserverContext proxyContext, final Object logContext)
    {
        final AtomicReference<IRecordListener> listenerReference = new AtomicReference<>();
        final OneShotLatch updateWaitLatch = new OneShotLatch();
        final NotifyingCache<IServiceConnectionStatusListener, Connection> serviceStatusNotifyingCache =
            new NotifyingCache<IServiceConnectionStatusListener, Connection>(
                (IDestructor) (ref) -> proxyContext.removeObserver(listenerReference.get(),
                    ISystemRecordNames.CONTEXT_STATUS))
            {
                @Override
                protected void notifyListenerDataRemoved(IServiceConnectionStatusListener listener, String key,
                    Connection data)
                {
                }

                @Override
                protected void notifyListenerDataAdded(IServiceConnectionStatusListener listener, String key,
                    Connection data)
                {
                    switch(data)
                    {
                        case CONNECTED:
                            listener.onConnected(decomposeServiceFromProxyName(proxyContext.getName()),
                                System.identityHashCode(proxyContext));
                            break;
                        case DISCONNECTED:
                            listener.onDisconnected(decomposeServiceFromProxyName(proxyContext.getName()),
                                System.identityHashCode(proxyContext));
                            break;
                        case RECONNECTING:
                            listener.onReconnecting(decomposeServiceFromProxyName(proxyContext.getName()),
                                System.identityHashCode(proxyContext));
                            break;
                    }
                }
            };
        final IRecordListener observer = (imageCopy, atomicChange) -> {
            Connection status = IStatusAttribute.Utils.getStatus(Connection.class, imageCopy);
            serviceStatusNotifyingCache.notifyListenersDataAdded(Connection.class.getSimpleName(), status);
            updateWaitLatch.countDown();
        };
        listenerReference.set(observer);
        proxyContext.addObserver(observer, ISystemRecordNames.CONTEXT_STATUS);
        awaitUpdateLatch(logContext, ISystemRecordNames.CONTEXT_STATUS, updateWaitLatch);
        return serviceStatusNotifyingCache;
    }

    /**
     * <b>This assumes that the token <tt>'->'</tt> does not appear in the name of the service or
     * client</b>
     * 
     * @return the client part of the proxy name format <tt>'service->client'</tt>. If it is not in
     *         this format, returns the proxyName
     */
    public static String decomposeClientFromProxyName(String proxyName)
    {
        final int indexOf = proxyName.indexOf(SERVICE_CLIENT_DELIMITER);
        if (indexOf > -1)
        {
            return proxyName.substring(indexOf + SERVICE_CLIENT_DELIMITER.length());
        }
        return proxyName;
    }

    /**
     * <b>This assumes that the token <tt>'->'</tt> does not appear in the name of the service or
     * client</b>
     * 
     * @return the service part of the proxy name format <tt>'service->client'</tt>. If it is not in
     *         this format, returns the proxyName
     */
    public static String decomposeServiceFromProxyName(String proxyName)
    {
        final int indexOf = proxyName.indexOf(SERVICE_CLIENT_DELIMITER);
        if (indexOf > -1)
        {
            return proxyName.substring(0, indexOf);
        }
        return proxyName;
    }

    /**
     * @return a proxy name format <tt>'service->client'</tt>
     */
    public static String composeProxyName(String service, String client)
    {
        return service + SERVICE_CLIENT_DELIMITER + client;
    }

    /**
     * @return a string in the form <tt>'name[0]@canonical_host_name'</tt>. <br>
     *         If there are no name arguments, the 'name' is the calling class.
     */
    public static String composeHostQualifiedName(String... name)
    {
        try
        {
            return (name == null || name.length == 0 ? ThreadUtils.getIndirectCallingClassSimpleName() : name[0]) + "@"
                + TcpChannelUtils.LOCALHOST_IP;
        }
        catch (Exception e)
        {
            Log.log(PlatformRegistryAgent.class, "Could not create default name", e);
            return "default:" + System.currentTimeMillis();
        }
    }

    static String getHostNameFromServiceInfoRecord(Map<String, IValue> serviceRecord)
    {
        return serviceRecord.get(ServiceInfoRecordFields.HOST_NAME_FIELD).textValue();
    }

    static int getPortFromServiceInfoRecord(Map<String, IValue> serviceRecord)
    {
        return (int) serviceRecord.get(ServiceInfoRecordFields.PORT_FIELD).longValue();
    }

    static TransportTechnologyEnum getTransportTechnologyFromServiceInfoRecord(Map<String, IValue> serviceRecord)
    {
        return TransportTechnologyEnum.valueOf(
            serviceRecord.get(ServiceInfoRecordFields.TRANSPORT_TECHNOLOGY_FIELD).textValue());
    }

    static ICodec<?> getCodecFromServiceInfoRecord(Map<String, IValue> serviceRecord)
    {
        String codecName = serviceRecord.get(ServiceInfoRecordFields.WIRE_PROTOCOL_FIELD).textValue();
        return WireProtocolEnum.valueOf(codecName).getCodec();
    }

    /**
     * Compose the service instance ID representing a service member of a platform service family.
     * 
     * @return the ID to uniquely identify the service member of this service, format is
     *         <tt>'serviceFamily[serviceMember]'</tt>
     * @throws IllegalArgumentException
     *             if the service member contains "[" or "]"
     */
    public static String composePlatformServiceInstanceID(String serviceFamily, String serviceMember)
    {
        if (serviceMember.indexOf(SERVICE_INSTANCE_PREFIX, 0) > -1
            || serviceMember.indexOf(SERVICE_INSTANCE_SUFFIX, 0) > -1)
        {
            throw new IllegalArgumentException(
                "Service member name '" + serviceMember + "' cannot contain characters [ or ]");
        }

        final StringBuilder sb = new StringBuilder(serviceFamily.length() + serviceMember.length()
            + SERVICE_INSTANCE_PREFIX.length() + SERVICE_INSTANCE_SUFFIX.length());
        sb.append(serviceFamily).append(SERVICE_INSTANCE_PREFIX).append(serviceMember).append(SERVICE_INSTANCE_SUFFIX);
        return sb.toString();
    }

    /**
     * Get an array holding the service name and service member name extracted from a service
     * instance key. The service instance key must have been created via
     * {@link #composePlatformServiceInstanceID(String, String)}.
     * 
     * @return an array <code>[serviceFamily,serviceMember]</code>, <code>null</code> if the key is
     *         not in the expected format
     */
    public static String[] decomposePlatformServiceInstanceID(String platformServiceInstanceID)
    {
        final int length = platformServiceInstanceID.length();
        int index = platformServiceInstanceID.lastIndexOf(SERVICE_INSTANCE_PREFIX, length);
        if (index == -1)
        {
            return null;
        }
        return new String[] { platformServiceInstanceID.substring(0, index), platformServiceInstanceID.substring(
            index + SERVICE_INSTANCE_PREFIX.length(), length - SERVICE_INSTANCE_SUFFIX.length()) };
    }

    /**
     * Convenience method to execute the RPC hosted by the service component - this waits for the
     * RPC to be published
     * 
     * @param component
     *            the service component hosting the RPC
     * @param discoveryTimeoutMillis
     *            the timeout to wait for the RPC to be available
     * @param rpcName
     *            the RPC name
     * @param rpcArgs
     *            the arguments for the RPC
     * @return the return value of the RPC execution
     */
    public static IValue executeRpc(IPlatformServiceComponent component, long discoveryTimeoutMillis,
        final String rpcName, final IValue... rpcArgs) throws TimeOutException, ExecutionException
    {
        return getRpc(component, discoveryTimeoutMillis, rpcName, rpcArgs).execute(rpcArgs);
    }

    /**
     * Convenience method to execute the RPC hosted by the service component - this waits for the
     * RPC to be published
     * 
     * @param component
     *            the service component hosting the RPC
     * @param discoveryTimeoutMillis
     *            the timeout to wait for the RPC to be available
     * @param rpcName
     *            the RPC name
     * @param rpcArgs
     *            the arguments for the RPC
     */
    public static void executeRpcNoResponse(IPlatformServiceComponent component, long discoveryTimeoutMillis,
        final String rpcName, final IValue... rpcArgs) throws TimeOutException, ExecutionException
    {
        getRpc(component, discoveryTimeoutMillis, rpcName, rpcArgs).executeNoResponse(rpcArgs);
    }

    /**
     * Convenience method for getting an RPC instance from a platform service component.
     * 
     * @param component
     *            the service component hosting the RPC
     * @param discoveryTimeoutMillis
     *            the timeout to wait for the RPC to be available
     * @param rpcName
     *            the RPC name
     * @param rpcArgs
     *            the arguments for the RPC
     * @return the RPC instance
     * @throws TimeOutException
     *             if no RPC is found
     */
    public static IRpcInstance getRpc(IPlatformServiceComponent component, long discoveryTimeoutMillis,
        final String rpcName, final IValue... rpcArgs) throws TimeOutException
    {
        final IRpcInstance rpc = component.getRpc(rpcName);
        if (rpc != null)
        {
            return rpc;
        }

        final AtomicReference<IRpcInstance> rpcRef = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final IRpcAvailableListener rpcListener = EventListenerUtils.synchronizedListener(new IRpcAvailableListener()
        {
            @Override
            public void onRpcUnavailable(IRpcInstance rpc)
            {
            }

            @Override
            public void onRpcAvailable(IRpcInstance rpc)
            {
                if (is.eq(rpc.getName(), rpcName) && rpcArgs.length == rpc.getArgTypes().length)
                {
                    try
                    {
                        rpcRef.set(rpc);
                    }
                    finally
                    {
                        latch.countDown();
                    }
                }
            }
        });
        try
        {
            component.addRpcAvailableListener(rpcListener);
            try
            {
                if (!latch.await(discoveryTimeoutMillis, TimeUnit.MILLISECONDS))
                {
                    throw new TimeOutException("No RPC found with name [" + rpcName + "] and [" + rpcArgs.length
                        + "] arguments during discovery period " + discoveryTimeoutMillis + "ms");
                }
            }
            catch (InterruptedException e)
            {
                // we don't care!
            }
        }
        finally
        {
            component.removeRpcAvailableListener(rpcListener);
        }
        return rpcRef.get();
    }

    /**
     * @deprecated use {@link #getNextAvailableServicePort()}
     */
    @SuppressWarnings("unused")
    @Deprecated
    public static int getNextFreeDefaultTcpServerPort(String host)
    {
        return getNextAvailableServicePort();
    }

    /**
     * @see TransportTechnologyEnum#getNextAvailableServicePort()
     */
    public static int getNextAvailableServicePort()
    {
        return TransportTechnologyEnum.getDefaultFromSystemProperty().getNextAvailableServicePort();
    }

    private static void awaitUpdateLatch(final Object logContext, String recordName, final OneShotLatch updateWaitLatch)
    {
        try
        {
            boolean isCountedDown = updateWaitLatch.await(
                DataFissionProperties.Values.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS, TimeUnit.MILLISECONDS);
            if (!isCountedDown)
            {
                Log.log(logContext, "Initial image for '", recordName, "' was not received after waiting [",
                    String.valueOf(DataFissionProperties.Values.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS), "] millis.");
            }
        }
        catch (InterruptedException e)
        {
            // ignore
        }
    }

    /**
     * This is a "no-brainer" convenience method that will get a platform service instance, creating
     * it if necessary. During creation, relevant configuration for the instance (host, port,
     * wireprotocol, redundancy mode) is retrieved from the ConfigService, if it is available,
     * otherwise suitable defaults will be used.
     * 
     * @param serviceName
     *            the service family name
     * @param serviceMemberName
     *            the service member name
     * @param agent
     *            the agent to use
     * @return the instance
     */
    public static IPlatformServiceInstance getOrCreatePlatformServiceInstance(String serviceName,
        String serviceMemberName, IPlatformRegistryAgent agent)
    {
        final IConfig config =
            ConfigServiceProxy.getDefaultInstanceOrDummy(agent).getConfig(serviceName, serviceMemberName);
        Log.log(PlatformUtils.class, ObjectUtils.safeToString(config));
        return ConfigUtils.getPlatformServiceInstance(serviceName, serviceMemberName, config, agent);
    }

    /**
     * Checks if a record name is used by ClearConnect.
     * 
     * @param recordName
     *            the record name to check
     * @return true if the recordName is used by ClearConnect
     */
    public static boolean isClearConnectRecord(String recordName)
    {
        return (ContextUtils.isSystemRecordName(recordName))
            || PlatformServiceInstance.SERVICE_STATS_RECORD_NAME.equals(recordName);
    }
}
