/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei, Fimtra
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

import java.io.IOException;
import java.net.InetSocketAddress;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.clearconnect.config.IConfig;
import com.fimtra.clearconnect.config.IConfigServiceProxy;
import com.fimtra.clearconnect.config.impl.ConfigService;
import com.fimtra.clearconnect.config.impl.ConfigServiceProxy;
import com.fimtra.clearconnect.config.impl.ConfigUtils;
import com.fimtra.clearconnect.core.PlatformRegistryAgent;
import com.fimtra.clearconnect.core.PlatformUtils;
import com.fimtra.util.Log;
import com.fimtra.util.StringUtils;

/**
 * This is a convenience class for application code to connect to a platform and have a single
 * service created on the platform. The access object handles all the necessary internal plumbing to
 * provide application code with a simple way of getting access to this service.
 * <p>
 * This <b>does not limit</b> application code to only use the service available via
 * {@link #getPlatformServiceInstance()}. Application code can create any number of services or
 * service proxies using the agent that is available from {@link #getPlatformRegistryAgent()}.
 * <p>
 * Additionally, this provides out-of-the-box access to the {@link ConfigService} via calls to
 * {@link #getConfigServiceProxy()}. This allows configuration to be stored/retrieved from the
 * config service.
 * 
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
public class PlatformServiceAccess
{
    final IConfigServiceProxy configServiceProxy;
    final IPlatformServiceInstance platformServiceInstance;
    final IPlatformRegistryAgent platformRegistryAgent;

    /**
     * Constructs service access for the platform connecting to the registry at the passed in
     * registry host.
     * <P>
     * <b>THIS CONSTRUCTOR PROVIDES NO REGISTRY CONNECTION REDUNDANCY</b>.
     * 
     * @param serviceFamily
     *            the name of the service
     * @param serviceMember
     *            the name of the service member
     * @param registryHost
     *            the registry host
     * @see #PlatformServiceAccess(String, String, InetSocketAddress...)
     */
    public PlatformServiceAccess(String serviceFamily, String serviceMember, String registryHost)
    {
        this(serviceFamily, serviceMember, new EndPointAddress(registryHost,
            PlatformCoreProperties.Values.REGISTRY_PORT));
    }

    /**
     * Constructs service access for the platform.
     * 
     * @param serviceFamily
     *            the name of the service
     * @param serviceMember
     *            the name of the service member
     * @param registryAddresses
     *            the addresses of registry servers to use - this provides redundancy for registry
     *            connections
     * @throws RuntimeException
     *             if construction fails
     */
    public PlatformServiceAccess(String serviceFamily, String serviceMember, EndPointAddress... registryAddresses)
    {
        final String platformServiceInstanceId =
            PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember);
        try
        {
            this.platformRegistryAgent =
                new PlatformRegistryAgent("PlatformServiceAccess:" + platformServiceInstanceId, registryAddresses);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to create platform registry agent", e);
        }

        this.configServiceProxy = ConfigServiceProxy.getDefaultInstance(this.platformRegistryAgent);

        IConfig config = this.configServiceProxy.getConfig(serviceFamily, serviceMember);

        if (!StringUtils.isEmpty(serviceMember))
        {
            this.platformServiceInstance =
                ConfigUtils.getPlatformServiceInstance(serviceFamily, serviceMember, config, this.platformRegistryAgent);
        }
        else
        {
            this.platformServiceInstance =
                ConfigUtils.getPlatformServiceInstance(serviceFamily, config, this.platformRegistryAgent);
        }

        Log.banner(this, "CONNECTED TO PLATFORM '" + this.platformRegistryAgent.getPlatformName() + "'");
    }

    /**
     * @return the {@link IConfigServiceProxy}
     */
    public IConfigServiceProxy getConfigServiceProxy()
    {
        return this.configServiceProxy;
    }

    /**
     * @return the {@link IPlatformServiceInstance}
     */
    public IPlatformServiceInstance getPlatformServiceInstance()
    {
        return this.platformServiceInstance;
    }

    /**
     * @return the {@link IPlatformRegistryAgent}
     */
    public IPlatformRegistryAgent getPlatformRegistryAgent()
    {
        return this.platformRegistryAgent;
    }

    /**
     * Destroys {@link PlatformServiceAccess}
     */
    public void destroy()
    {
        this.platformRegistryAgent.destroy();
    }
}
