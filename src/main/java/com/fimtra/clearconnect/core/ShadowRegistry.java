/*
 * Copyright (c) 2015 Ramon Servadei 
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

import com.fimtra.channel.EndPointAddress;
import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.IPlatformRegistryAgent.RegistryNotAvailableException;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.util.Log;
import com.fimtra.util.ThreadUtils;

/**
 * A shadow registry monitors a primary registry and if the primary registry goes off-line, the
 * shadow registry starts a {@link PlatformRegistry} instance. If the primary registry is
 * re-started, the shadow registry stops its registry.
 * 
 * @author Ramon Servadei
 */
public final class ShadowRegistry
{
    PlatformRegistry registry;
    final String platformName;
    final EndPointAddress primaryRegistryEndPoint;
    final EndPointAddress shadowRegistryEndPoint;
    final IPlatformRegistryAgent primaryRegistryAgent;

    /**
     * Start the shadow registry
     * 
     * @param platformName
     *            the name of the platform to startup if the primary goes down
     * @param primaryRegistryEndPoint
     *            the primary registry to monitor
     * @param shadowRegistryEndPoint
     *            the end point to use for when starting the shadow registry
     * @throws RegistryNotAvailableException
     *             if the primary registry is not available during startup
     */
    public ShadowRegistry(String platformName, EndPointAddress primaryRegistryEndPoint,
        EndPointAddress shadowRegistryEndPoint) throws RegistryNotAvailableException
    {
        this.platformName = platformName;
        this.primaryRegistryEndPoint = primaryRegistryEndPoint;
        this.shadowRegistryEndPoint = shadowRegistryEndPoint;

        this.primaryRegistryAgent = new PlatformRegistryAgent("ShadowRegistryMonitor", primaryRegistryEndPoint);
        this.primaryRegistryAgent.addRegistryAvailableListener(new IRegistryAvailableListener()
        {
            @Override
            public void onRegistryDisconnected()
            {
                startShadowRegistry();
            }

            @Override
            public void onRegistryConnected()
            {
                stopShadowRegistry();
            }
        });
    }

    void startShadowRegistry()
    {
        ThreadUtils.newThread(new Runnable()
        {
            @Override
            public void run()
            {
                if (ShadowRegistry.this.registry == null)
                {
                    Log.log(ShadowRegistry.this, "Starting shadow registry");
                    ShadowRegistry.this.registry =
                        new PlatformRegistry(ShadowRegistry.this.platformName,
                            ShadowRegistry.this.shadowRegistryEndPoint);
                }
            }
        }, "shadow-registry-startup").start();
    }

    void stopShadowRegistry()
    {
        if (this.registry != null)
        {
            Log.log(this, "Stopping shadow registry");
            this.registry.destroy();
            this.registry = null;
        }
    }

    public void destroy()
    {
        stopShadowRegistry();
    }
}
