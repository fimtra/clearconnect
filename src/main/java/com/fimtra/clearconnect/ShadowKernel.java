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
package com.fimtra.clearconnect;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.clearconnect.IPlatformRegistryAgent.RegistryNotAvailableException;
import com.fimtra.clearconnect.core.PlatformRegistryAgent;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.util.Log;
import com.fimtra.util.SystemUtils;
import com.fimtra.util.ThreadUtils;

/**
 * A shadow kernel that monitors a primary kernel and if the primary goes off-line, the shadow
 * kernel starts a {@link PlatformKernel}. If the primary comes back on-line, the shadow stops its
 * kernel.
 * 
 * @author Ramon Servadei
 */
public class ShadowKernel
{
    /**
     * Access for starting a {@link ShadowKernel} using command line.
     * 
     * @param args
     *            the parameters used to start the {@link ShadowKernel}.
     * 
     *            <pre>
     *  arg[0] is the platform name (mandatory)
     *  arg[1] is the primary kernel host (mandatory)
     *  arg[2] is the primary kernel port (mandatory)
     *  arg[3] is the shadow kernel host (mandatory)
     *  arg[4] is the shadow kernel port (mandatory)
     * </pre>
     * @throws RegistryNotAvailableException
     */
    @SuppressWarnings("unused")
    public static void main(String[] args) throws InterruptedException, RegistryNotAvailableException
    {
        try
        {
            switch(args.length)
            {
                case 5:
                    new ShadowKernel(args[0], new EndPointAddress(args[1], Integer.parseInt(args[2])),
                        new EndPointAddress(args[3], Integer.parseInt(args[4])));
                    break;
                default :
                    throw new IllegalArgumentException("Incorrect number of arguments.");
            }
        }
        catch (RuntimeException e)
        {
            throw new RuntimeException(SystemUtils.lineSeparator() + "Usage: " + ShadowKernel.class.getSimpleName()
                + " platformName primaryKernelHost primaryKernelPort shadowKernelHost shadowKernelPort", e);
        }
        synchronized (args)
        {
            args.wait();
        }
    }

    PlatformKernel kernel;
    final String platformName;
    final EndPointAddress primaryRegistryEndPoint;
    final EndPointAddress shadowRegistryEndPoint;
    final IPlatformRegistryAgent primaryRegistryAgent;

    /**
     * Start the shadow kernel
     * 
     * @param platformName
     *            the name of the platform to startup if the primary goes down
     * @param primaryRegistryEndPoint
     *            the primary kernel to monitor
     * @param shadowRegistryEndPoint
     *            the end point to use for when starting the shadow kernel
     * @throws RegistryNotAvailableException
     *             if the primary kernel is not available during startup
     */
    public ShadowKernel(String platformName, EndPointAddress primaryRegistryEndPoint,
        EndPointAddress shadowRegistryEndPoint) throws RegistryNotAvailableException
    {
        this.platformName = platformName;
        this.primaryRegistryEndPoint = primaryRegistryEndPoint;
        this.shadowRegistryEndPoint = shadowRegistryEndPoint;

        this.primaryRegistryAgent = new PlatformRegistryAgent("shadowKernelMonitor", primaryRegistryEndPoint);
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
                // if a shadow is running as a warm-standby on the same host, the agent will
                // re-connect to the shadow and cause it to stop - leads to an infinite start/stop
                // loop, so don't do anything when connected
                if (!ShadowKernel.this.primaryRegistryEndPoint.equals(ShadowKernel.this.shadowRegistryEndPoint))
                {
                    stopShadowKernel();
                }
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
                if (ShadowKernel.this.kernel == null)
                {
                    Log.log(ShadowKernel.this, "Starting shadow kernel");
                    ShadowKernel.this.kernel =
                        new PlatformKernel(ShadowKernel.this.platformName, ShadowKernel.this.shadowRegistryEndPoint);
                }
            }
        }, "ShadowKernelStartup").start();
    }

    void stopShadowKernel()
    {
        if (this.kernel != null)
        {
            Log.log(this, "Stopping shadow kernel");
            this.kernel.destroy();
            this.kernel = null;
        }
    }

    public void destroy()
    {
        stopShadowKernel();
    }
}
