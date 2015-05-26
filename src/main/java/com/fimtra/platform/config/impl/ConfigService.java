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
package com.fimtra.platform.config.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.platform.IPlatformServiceInstance;
import com.fimtra.platform.PlatformCoreProperties;
import com.fimtra.platform.config.ConfigServiceProperties;
import com.fimtra.platform.config.IConfig;
import com.fimtra.platform.config.IConfigServiceProxy;
import com.fimtra.platform.core.PlatformRegistryAgent;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.FileUtils;
import com.fimtra.util.ThreadUtils;
import com.fimtra.util.is;

/**
 * The config service manages configuration records for platform services. The configuration
 * mechanics are discussed in the documentation of the {@link IConfig}. The config service is
 * accessed by client {@link IConfigServiceProxy} instances that create relevent {@link IConfig} objects.
 * <p>
 * <h3>Internals</h3>
 * The config service reads the config directory and publishes all config records it finds. The config records are simply the serialised
 * form of an {@link IRecord} as performed by {@link ContextUtils#serializeRecordToFile(IRecord, File)}. The config directory is the
 * 'config' directory that is in the working directory of the process. This can be changed by setting the system property
 * 'platform.configService.configDir' to the full path of the config directory (a 'config' directory will NOT be created in this user
 * specified directory location).
 * <p>
 * The config service will poll the config directory every 60 seconds to check for config changes. Any changes to the files in the directory
 * are published. The changes that are detected are: file modified timestamp, file size change, new files and deleted files;
 *
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public class ConfigService {
	/**
	 * Construct the {@link IPlatformServiceInstance} for the config service. This looks for a file
	 * called <tt>ConfigService.properties</tt> in the config directory for any overrides to default
	 * platform service instance connection arguments.
	 *
	 * @see ConfigUtils#getPlatformServiceInstance(String, String, IConfig, com.fimtra.platform.IPlatformRegistryAgent)
	 */
	static IPlatformServiceInstance constructConfigServiceInstance(File configDir, PlatformRegistryAgent agent, String kernelHost)
			throws IOException, FileNotFoundException {
		File[] propertyFiles = FileUtils.readFiles(configDir, ConfigDirReader.propertyFileFilter);
		IConfig config = null;
		final String configServicePropertiesFileName = IConfigServiceProxy.CONFIG_SERVICE + "." + FileUtils.propertyFileExtension;
		for (File propertyFile : propertyFiles) {
			if (is.eq(propertyFile.getName(), configServicePropertiesFileName)) {
				final Properties props = new Properties();
				props.load(new FileInputStream(propertyFile));
				config = new SimplePropertiesConfig(props);
				break;
			}
		}
		if (config == null) {
			config = new SimplePropertiesConfig(new Properties());
		}

		int port = ConfigUtils.getPort(config, kernelHost);
		String serviceMemberName = kernelHost + ":" + port;
		return ConfigUtils.getPlatformServiceInstanceInternal(IConfigServiceProxy.CONFIG_SERVICE, serviceMemberName, config, agent,
				kernelHost, port);
	}

	private final ConfigPublisher configPublisher;
	private final ScheduledExecutorService scheduledExecutor;
	private final PlatformRegistryAgent platformRegistryAgent;
	final IPlatformServiceInstance platformServiceInstance;

	protected ConfigService() {
		this(TcpChannelUtils.LOCALHOST_IP, PlatformCoreProperties.Values.REGISTRY_PORT);
	}

	protected ConfigService(EndPointAddress registryEndpoint) {
		try {
			this.scheduledExecutor = ThreadUtils.newScheduledExecutorService("config-polling", 1);

			File configDir = new File(ConfigServiceProperties.Values.CONFIG_DIR);
			if (!configDir.exists()) {
				configDir.mkdir();
			}
			ConfigDirReader configDirReader = new ConfigDirReader(configDir);

			this.platformRegistryAgent = new PlatformRegistryAgent(IConfigServiceProxy.CONFIG_SERVICE, registryEndpoint);

			this.platformServiceInstance = constructConfigServiceInstance(configDir, this.platformRegistryAgent, registryEndpoint.getNode());

			this.configPublisher = new ConfigPublisher(configDirReader, this.platformServiceInstance);
			publishConfigRecords(ConfigServiceProperties.Values.POLLING_PERIOD_SECS);
			publishRpcs(this.platformServiceInstance, configDirReader);
		} catch (Exception e) {
			throw new RuntimeException("Could not construct config service", e);
		}
	}

	protected ConfigService(String registryHost, int registryPort) {
		this(new EndPointAddress(registryHost, registryPort));
	}

	public void destroy() {
		this.scheduledExecutor.shutdownNow();
		this.platformRegistryAgent.destroy();
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		destroy();
	}

	void publishConfig() {
		if (this.configPublisher != null && this.scheduledExecutor != null) {
			this.scheduledExecutor.execute(this.configPublisher);
		}
	}

	private void publishConfigRecords(int pollingIntervalSec) {
		this.scheduledExecutor.scheduleAtFixedRate(this.configPublisher, 0, pollingIntervalSec, TimeUnit.SECONDS);
	}

	@SuppressWarnings("unused")
	private void publishRpcs(IPlatformServiceInstance platformServiceInstance, ConfigDirReader configDirReader) {
		new RpcCreateOrUpdateMemberConfig(this, configDirReader, platformServiceInstance);
		new RpcDeleteMemberConfig(this, configDirReader, platformServiceInstance);
		new RpcCreateOrUpdateFamilyConfig(this, configDirReader, platformServiceInstance);
		new RpcDeleteFamilyConfig(this, configDirReader, platformServiceInstance);
	}

}
