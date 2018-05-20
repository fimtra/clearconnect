/*
 * Copyright (c) 2018 Ramon Servadei
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
package com.fimtra.clearconnect.event;

import com.fimtra.datafission.IRpcInstance;

/**
 * Provides utility methods to wrap an {@link IEventListener} in a synchronized version
 * 
 * @author Ramon Servadei
 */
public abstract class EventListenerUtils
{
    private EventListenerUtils()
    {
        // no instantiation
    }
    
    /**
     * Base class for synchronized listener adapter instances
     * 
     * @author Ramon Servadei
     * @param <T>
     */
    private static abstract class AbstractSynchronizedListenerAdapter<T>
    {
        final T delegate;

        AbstractSynchronizedListenerAdapter(T delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public final int hashCode()
        {
            return this.delegate.hashCode();
        }

        @Override
        public final boolean equals(Object obj)
        {
            return (this == obj || obj == this.delegate);
        }
    }

    /**
     * Synchronized adapter
     * 
     * @author Ramon Servadei
     */
    private final static class SynchronizedFtStatusListener
        extends AbstractSynchronizedListenerAdapter<IFtStatusListener> implements IFtStatusListener
    {
        SynchronizedFtStatusListener(IFtStatusListener target)
        {
            super(target);
        }

        @Override
        public synchronized void onActive(String serviceFamily, String serviceMember)
        {
            this.delegate.onActive(serviceFamily, serviceMember);
        }

        @Override
        public synchronized void onStandby(String serviceFamily, String serviceMember)
        {
            this.delegate.onStandby(serviceFamily, serviceMember);
        }
    }

    /**
     * Synchronized adapter
     * 
     * @author Ramon Servadei
     */
    private final static class SynchronizedProxyConnectionListener
        extends AbstractSynchronizedListenerAdapter<IProxyConnectionListener> implements IProxyConnectionListener
    {

        SynchronizedProxyConnectionListener(IProxyConnectionListener delegate)
        {
            super(delegate);
        }

        @Override
        public synchronized void onConnected(String proxyId)
        {
            this.delegate.onConnected(proxyId);
        }

        @Override
        public synchronized void onDisconnected(String proxyId)
        {
            this.delegate.onDisconnected(proxyId);
        }
    }

    /**
     * Synchronized adapter
     * 
     * @author Ramon Servadei
     */
    private final static class SynchronizedRecordAvailableListener
        extends AbstractSynchronizedListenerAdapter<IRecordAvailableListener> implements IRecordAvailableListener
    {

        SynchronizedRecordAvailableListener(IRecordAvailableListener delegate)
        {
            super(delegate);
        }

        @Override
        public synchronized void onRecordAvailable(String recordName)
        {
            this.delegate.onRecordAvailable(recordName);
        }

        @Override
        public synchronized void onRecordUnavailable(String recordName)
        {
            this.delegate.onRecordUnavailable(recordName);
        }
    }

    /**
     * Synchronized adapter
     * 
     * @author Ramon Servadei
     */
    private final static class SynchronizedRecordConnectionStatusListener extends
        AbstractSynchronizedListenerAdapter<IRecordConnectionStatusListener> implements IRecordConnectionStatusListener
    {
        SynchronizedRecordConnectionStatusListener(IRecordConnectionStatusListener delegate)
        {
            super(delegate);
        }

        @Override
        public synchronized void onRecordConnected(String recordName)
        {
            this.delegate.onRecordConnected(recordName);
        }

        @Override
        public synchronized void onRecordConnecting(String recordName)
        {
            this.delegate.onRecordConnecting(recordName);
        }

        @Override
        public synchronized void onRecordDisconnected(String recordName)
        {
            this.delegate.onRecordDisconnected(recordName);
        }
    }

    /**
     * Synchronized adapter
     * 
     * @author Ramon Servadei
     */
    private final static class SynchronizedRecordSubscriptionListener
        extends AbstractSynchronizedListenerAdapter<IRecordSubscriptionListener> implements IRecordSubscriptionListener
    {
        SynchronizedRecordSubscriptionListener(IRecordSubscriptionListener delegate)
        {
            super(delegate);
        }

        @Override
        public synchronized void onRecordSubscriptionChange(SubscriptionInfo subscriptionInfo)
        {
            this.delegate.onRecordSubscriptionChange(subscriptionInfo);
        }
    }

    /**
     * Synchronized adapter
     * 
     * @author Ramon Servadei
     */
    private final static class SynchronizedRegistryAvailableListener
        extends AbstractSynchronizedListenerAdapter<IRegistryAvailableListener> implements IRegistryAvailableListener
    {
        SynchronizedRegistryAvailableListener(IRegistryAvailableListener delegate)
        {
            super(delegate);
        }

        @Override
        public synchronized void onRegistryConnected()
        {
            this.delegate.onRegistryConnected();
        }

        @Override
        public synchronized void onRegistryDisconnected()
        {
            this.delegate.onRegistryDisconnected();
        }
    }

    /**
     * Synchronized adapter
     * 
     * @author Ramon Servadei
     */
    private final static class SynchronizedRpcAvailableListener
        extends AbstractSynchronizedListenerAdapter<IRpcAvailableListener> implements IRpcAvailableListener
    {
        SynchronizedRpcAvailableListener(IRpcAvailableListener delegate)
        {
            super(delegate);
        }

        @Override
        public synchronized void onRpcAvailable(IRpcInstance rpc)
        {
            this.delegate.onRpcAvailable(rpc);
        }

        @Override
        public synchronized void onRpcUnavailable(IRpcInstance rpc)
        {
            this.delegate.onRpcUnavailable(rpc);
        }
    }

    /**
     * Synchronized adapter
     * 
     * @author Ramon Servadei
     */
    private final static class SynchronizedServiceAvailableListener
        extends AbstractSynchronizedListenerAdapter<IServiceAvailableListener> implements IServiceAvailableListener
    {
        SynchronizedServiceAvailableListener(IServiceAvailableListener delegate)
        {
            super(delegate);
        }

        @Override
        public synchronized void onServiceAvailable(String serviceFamily)
        {
            this.delegate.onServiceAvailable(serviceFamily);
        }

        @Override
        public synchronized void onServiceUnavailable(String serviceFamily)
        {
            this.delegate.onServiceUnavailable(serviceFamily);
        }
    }

    /**
     * Synchronized adapter
     * 
     * @author Ramon Servadei
     */
    private final static class SynchronizedServiceConnectionStatusListener
        extends AbstractSynchronizedListenerAdapter<IServiceConnectionStatusListener>
        implements IServiceConnectionStatusListener
    {
        SynchronizedServiceConnectionStatusListener(IServiceConnectionStatusListener delegate)
        {
            super(delegate);
        }

        @Override
        public synchronized void onConnected(String serviceFamily, int identityHash)
        {
            this.delegate.onConnected(serviceFamily, identityHash);
        }

        @Override
        public synchronized void onReconnecting(String serviceFamily, int identityHash)
        {
            this.delegate.onReconnecting(serviceFamily, identityHash);
        }

        @Override
        public synchronized void onDisconnected(String serviceFamily, int identityHash)
        {
            this.delegate.onDisconnected(serviceFamily, identityHash);
        }
    }

    /**
     * Synchronized adapter
     * 
     * @author Ramon Servadei
     */
    private final static class SynchronizedServiceInstanceAvailableListener
        extends AbstractSynchronizedListenerAdapter<IServiceInstanceAvailableListener>
        implements IServiceInstanceAvailableListener
    {
        SynchronizedServiceInstanceAvailableListener(IServiceInstanceAvailableListener delegate)
        {
            super(delegate);
        }

        @Override
        public synchronized void onServiceInstanceAvailable(String serviceInstanceId)
        {
            this.delegate.onServiceInstanceAvailable(serviceInstanceId);
        }

        @Override
        public synchronized void onServiceInstanceUnavailable(String serviceInstanceId)
        {
            this.delegate.onServiceInstanceUnavailable(serviceInstanceId);
        }

    }

    /**
     * @return a synchronized instance
     */
    public static IFtStatusListener synchronizedListener(IFtStatusListener target)
    {
        return new SynchronizedFtStatusListener(target);
    }

    /**
     * @return a synchronized instance
     */
    public static IProxyConnectionListener synchronizedListener(IProxyConnectionListener target)
    {
        return new SynchronizedProxyConnectionListener(target);
    }

    /**
     * @return a synchronized instance
     */
    public static IRecordAvailableListener synchronizedListener(IRecordAvailableListener target)
    {
        return new SynchronizedRecordAvailableListener(target);
    }

    /**
     * @return a synchronized instance
     */
    public static IRecordConnectionStatusListener synchronizedListener(IRecordConnectionStatusListener target)
    {
        return new SynchronizedRecordConnectionStatusListener(target);
    }

    /**
     * @return a synchronized instance
     */
    public static IRecordSubscriptionListener synchronizedListener(IRecordSubscriptionListener target)
    {
        return new SynchronizedRecordSubscriptionListener(target);
    }

    /**
     * @return a synchronized instance
     */
    public static IRegistryAvailableListener synchronizedListener(IRegistryAvailableListener target)
    {
        return new SynchronizedRegistryAvailableListener(target);
    }

    /**
     * @return a synchronized instance
     */
    public static IRpcAvailableListener synchronizedListener(IRpcAvailableListener target)
    {
        return new SynchronizedRpcAvailableListener(target);
    }

    /**
     * @return a synchronized instance
     */
    public static IServiceAvailableListener synchronizedListener(IServiceAvailableListener target)
    {
        return new SynchronizedServiceAvailableListener(target);
    }

    /**
     * @return a synchronized instance
     */
    public static IServiceConnectionStatusListener synchronizedListener(IServiceConnectionStatusListener target)
    {
        return new SynchronizedServiceConnectionStatusListener(target);
    }

    /**
     * @return a synchronized instance
     */
    public static IServiceInstanceAvailableListener synchronizedListener(IServiceInstanceAvailableListener target)
    {
        return new SynchronizedServiceInstanceAvailableListener(target);
    }
}
