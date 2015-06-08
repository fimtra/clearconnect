/*
 * Copyright (c) 2014 Ramon Servadei, Paul Mackinlay, Fimtra
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
import com.fimtra.clearconnect.config.IConfigServiceProxy;
import com.fimtra.clearconnect.config.impl.ConfigService;
import com.fimtra.clearconnect.config.impl.ConfigServiceProxy;
import com.fimtra.clearconnect.core.PlatformRegistryAgent;
import com.fimtra.util.Log;

/**
 * This is a convenience class for application code to connect to a platform with the intention of
 * accessing as a client.
 * <p>
 * This <b>does not limit</b> application code from creating services. Application code can create any number of services or service proxies
 * using the agent that is available from {@link #getPlatformRegistryAgent()}.
 * <p>
 * Additionally, this provides out-of-the-box access to the {@link ConfigService} via calls to {@link #getConfigServiceProxy()}. This allows
 * configuration to be stored/retrieved from the config service.
 * 
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public class PlatformClientAccess {
	final IConfigServiceProxy configServiceProxy;
	final IPlatformRegistryAgent platformRegistryAgent;

	/**
	 * Constructs service access for the platform connecting to the registry at the passed in
	 * registry host.
	 * <P>
	 * <b>THIS CONSTRUCTOR PROVIDES NO REGISTRY CONNECTION REDUNDANCY</b>.
	 * 
	 * @param clientName
	 *            the name of the client on the platform (this will become part of the name of the
	 *            agent)
	 * @param registryHost
	 *            the registry host
	 * @see #PlatformClientAccess(String, InetSocketAddress...)
	 */
	public PlatformClientAccess(String clientName, String registryHost) {
		this(clientName, new EndPointAddress(registryHost, PlatformCoreProperties.Values.REGISTRY_PORT));
	}

	/**
	 * Constructs a client access instance for the platform.
	 * 
	 * @param clientName
	 *            the name of the client on the platform (this will become part of the name of the
	 *            agent)
	 * @param registryAddresses
	 *            the addresses of registry servers to use - this provides redundancy for registry
	 *            connections
	 * @throws RuntimeException
	 *             if construction fails
	 */
	public PlatformClientAccess(String clientName, EndPointAddress... registryAddresses) {
		try {
			this.platformRegistryAgent = new PlatformRegistryAgent("PlatformClientAccess:" + clientName, registryAddresses);
		} catch (IOException e) {
			throw new RuntimeException("Unable to create platform registry agent", e);
		}

		this.platformRegistryAgent.waitForPlatformService(IConfigServiceProxy.CONFIG_SERVICE);

		this.configServiceProxy = new ConfigServiceProxy(
				this.platformRegistryAgent.getPlatformServiceProxy(IConfigServiceProxy.CONFIG_SERVICE));

		Log.banner(this, "CONNECTED TO PLATFORM '" + this.platformRegistryAgent.getPlatformName() + "'");
	}

	/**
	 * @return the {@link IConfigServiceProxy}
	 */
	public IConfigServiceProxy getConfigServiceProxy() {
		return this.configServiceProxy;
	}

	/**
	 * @return the {@link IPlatformRegistryAgent}
	 */
	public IPlatformRegistryAgent getPlatformRegistryAgent() {
		return this.platformRegistryAgent;
	}

	/**
	 * Destroys {@link PlatformClientAccess}
	 */
	public void destroy() {
		this.platformRegistryAgent.destroy();
	}
}
