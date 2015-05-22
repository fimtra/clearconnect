/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.core;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fimtra.datafission.core.IStatusAttribute.Connection;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.platform.event.IServiceConnectionStatusListener;
import com.fimtra.util.Log;
import com.fimtra.util.NotifyingCache;
import com.fimtra.util.ObjectUtils;

/**
 * A template class that monitors the connection to a single platform service.
 * 
 * @author Ramon Servadei
 */
class PlatformServiceConnectionMonitor
{
    Future<?> reconnectTask;
    final String serviceInstanceId;
    final ProxyContext proxyContext;
    final Lock callbackLock = new ReentrantLock();
    Connection previous = Connection.DISCONNECTED;
    NotifyingCache<IServiceConnectionStatusListener, Connection> serviceConnectionStatusNotifyingCache;

    /**
     * @param context
     *            the proxy context to monitor
     * @param serviceInstanceId
     *            the service instance ID the proxy points to (for logging purposes)
     */
    PlatformServiceConnectionMonitor(ProxyContext context, String serviceInstanceId)
    {
        this.serviceInstanceId = serviceInstanceId;
        this.proxyContext = context;
        this.serviceConnectionStatusNotifyingCache =
            PlatformUtils.createServiceConnectionStatusNotifyingCache(this.proxyContext, this);
        this.serviceConnectionStatusNotifyingCache.addListener(new IServiceConnectionStatusListener()
        {

            @Override
            public void onConnected(String platformServiceName, int identityHash)
            {
                check(Connection.CONNECTED);
                doConnected();
                store(Connection.CONNECTED);
            }

            @Override
            public void onReconnecting(String platformServiceName, int identityHash)
            {
                check(Connection.RECONNECTING);
                doReconnected();
                store(Connection.RECONNECTING);
            }

            @Override
            public void onDisconnected(String platformServiceName, int identityHash)
            {
                check(Connection.DISCONNECTED);
                doDisconnected();
                store(Connection.DISCONNECTED);
            }
        });
    }

    public void destroy()
    {
        try
        {
            this.serviceConnectionStatusNotifyingCache.destroy();
        }
        catch (Exception e)
        {
            Log.log(this, "Could not destroy connection monitor for " + this.serviceInstanceId, e);
        }
    }

    void check(Connection status)
    {
        if (status != this.previous)
        {
            Log.log(this, "Status of service ", this.serviceInstanceId, " is ", ObjectUtils.safeToString(status),
                ", previously ", ObjectUtils.safeToString(this.previous));
        }
    }

    void store(Connection status)
    {
        this.previous = status;
    }

    void doConnected()
    {
        if (this.reconnectTask != null)
        {
            this.reconnectTask.cancel(true);
            this.reconnectTask = null;
        }
        if (this.previous != Connection.CONNECTED)
        {
            this.callbackLock.lock();
            try
            {
                Log.log(PlatformServiceConnectionMonitor.this, "onPlatformServiceConnected ", this.serviceInstanceId);
                onPlatformServiceConnected();
            }
            finally
            {
                this.callbackLock.unlock();
            }
        }
    }

    void doReconnected()
    {
        if (this.previous != Connection.RECONNECTING)
        {
            this.reconnectTask = this.proxyContext.getUtilityExecutor().schedule(new Runnable()
            {
                @Override
                public void run()
                {
                    if (PlatformServiceConnectionMonitor.this.previous != Connection.CONNECTED)
                    {
                        PlatformServiceConnectionMonitor.this.callbackLock.lock();
                        try
                        {
                            Log.log(PlatformServiceConnectionMonitor.this, "onPlatformServiceDisconnected ",
                                PlatformServiceConnectionMonitor.this.serviceInstanceId);
                            onPlatformServiceDisconnected();
                        }
                        finally
                        {
                            PlatformServiceConnectionMonitor.this.callbackLock.unlock();
                        }
                    }
                    PlatformServiceConnectionMonitor.this.reconnectTask = null;
                }
            }, this.proxyContext.getReconnectPeriodMillis() * 2, TimeUnit.MILLISECONDS);

            this.callbackLock.lock();
            try
            {
                Log.log(PlatformServiceConnectionMonitor.this, "onPlatformServiceReconnecting ", this.serviceInstanceId);
                onPlatformServiceReconnecting();
            }
            finally
            {
                this.callbackLock.unlock();
            }
        }
    }

    void doDisconnected()
    {
        if (this.previous != Connection.DISCONNECTED)
        {
            if (this.reconnectTask != null)
            {
                this.reconnectTask.cancel(true);
                this.reconnectTask = null;
            }
            this.callbackLock.lock();
            try
            {
                Log.log(PlatformServiceConnectionMonitor.this, "onPlatformServiceDisconnected ", this.serviceInstanceId);
                onPlatformServiceDisconnected();
            }
            finally
            {
                this.callbackLock.unlock();
            }
        }
    }

    protected void onPlatformServiceConnected()
    {
        // noop
    }

    protected void onPlatformServiceReconnecting()
    {
        // noop
    }

    protected void onPlatformServiceDisconnected()
    {
        // noop
    }

    @Override
    public final String toString()
    {
        return "PlatformServiceConnectionMonitor [" + this.proxyContext + "]";
    }
}
