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
package com.fimtra.clearconnect;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.clearconnect.config.impl.ConfigService;
import com.fimtra.clearconnect.core.PlatformRegistry;
import com.fimtra.util.Log;
import com.fimtra.util.SystemUtils;

/**
 * The platform kernel is the core of the platform; it is composed of a {@link PlatformRegistry} and
 * {@link ConfigService}. These two components run as a pair and provide the core service discovery
 * and configuration for the platform.
 *
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
@SuppressWarnings("unused")
public class PlatformKernel {
	private final PlatformRegistry platformRegistry;
	private final ConfigService configService;

	/**
	 * Construct the kernel using the default port
	 *
	 * @see PlatformCoreProperties#REGISTRY_PORT
	 * @param platformName
	 *            the platform name
	 * @param host
	 *            the hostname or IP address for the registry service and config service
	 */
	public PlatformKernel(String platformName, String host) {
		this(platformName, host, PlatformCoreProperties.Values.REGISTRY_PORT);
	}

	/**
	 * Construct the kernel using an {@link EndPointAddress}.
	 *
	 * @param platformName
	 *            the platform name
	 * @param enpointAddress
	 *            the registry endpointAddress
	 */
	public PlatformKernel(String platformName, EndPointAddress endPointAddress) {
		this(platformName, endPointAddress.getNode(), endPointAddress.getPort());
	}

	/**
	 * Construct the kernel using the explicit parameters
	 *
	 * @param platformName
	 *            the platform name
	 * @param host
	 *            the hostname or IP address for the registry service and config service
	 * @param registryPort
	 *            the registry port
	 */
	public PlatformKernel(String platformName, String host, int registryPort) {
		this.platformRegistry = new PlatformRegistry(platformName, host, registryPort);
		this.configService = new KernelConfigService(host, registryPort);
		Log.banner(this, "CLEARCONNECT PLATFORM '" + platformName + "' STARTED ON " + host + ":" + registryPort);
	}

	/**
	 * Access for starting a {@link PlatformKernel} using command line.
	 *
	 * @param args - the parameters used to start the {@link PlatformKernel}.
	 * <pre>
	 *  arg[0] is the platform name (mandatory)
	 *  arg[1] is the host (mandatory)
	 *  arg[2] is the port (optional)
	 * </pre>
	 */
	public static void main(String[] args) throws InterruptedException {
		try {
			switch (args.length) {
				case 2:
					new PlatformKernel(args[0], args[1]);
					break;
				case 3:
					new PlatformKernel(args[0], args[1], Integer.parseInt(args[2]));
					break;
				default:
					throw new IllegalArgumentException("Incorrect number of arguments.");
			}
		} catch (RuntimeException e) {
			throw new RuntimeException(SystemUtils.lineSeparator() + "Usage: " + PlatformKernel.class.getSimpleName()
					+ " platformName hostName [tcpPort]" + SystemUtils.lineSeparator() + "    platformName is mandatory"
					+ SystemUtils.lineSeparator() + "    hostName is mandatory and is either the hostname or IP address"
					+ SystemUtils.lineSeparator() + "    tcpPort is optional", e);
		}
		synchronized (args) {
			args.wait();
		}
	}

	/**
	 * Destroys the {@link PlatformKernel}.
	 */
	public void destroy() {
		this.platformRegistry.destroy();
		this.configService.destroy();
	}

	private class KernelConfigService extends ConfigService {
		KernelConfigService(String host, int port) {
			super(host, port);
		}
	}
}
