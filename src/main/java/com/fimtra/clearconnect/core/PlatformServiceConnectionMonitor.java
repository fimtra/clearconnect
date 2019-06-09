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

import com.fimtra.clearconnect.event.EventListenerUtils;
import com.fimtra.clearconnect.event.IServiceConnectionStatusListener;
import com.fimtra.datafission.core.IStatusAttribute.Connection;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.util.Log;
import com.fimtra.util.NotifyingCache;

/**
 * A template class that monitors the connection to a single platform service.
 * 
 * @author Ramon Servadei
 */
class PlatformServiceConnectionMonitor
{
    final String serviceInstanceId;
    final ProxyContext proxyContext;
    final NotifyingCache<IServiceConnectionStatusListener, Connection> serviceConnectionStatusNotifyingCache;

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
        this.serviceConnectionStatusNotifyingCache.addListener(
            EventListenerUtils.synchronizedListener(new IServiceConnectionStatusListener()
            {
                @Override
                public void onConnected(String platformServiceName, int identityHash)
                {
                    onPlatformServiceConnected();
                }

                @Override
                public void onReconnecting(String platformServiceName, int identityHash)
                {
                    onPlatformServiceReconnecting();
                }

                @Override
                public void onDisconnected(String platformServiceName, int identityHash)
                {
                    onPlatformServiceDisconnected();
                }
            }));
    }

    public final void destroy()
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
