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
package com.fimtra.clearconnect.config.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.config.ConfigServiceProperties;
import com.fimtra.clearconnect.config.IConfig;
import com.fimtra.clearconnect.config.IConfigServiceProxy;
import com.fimtra.clearconnect.core.PlatformRegistryAgent;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.FileUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.ThreadUtils;
import com.fimtra.util.is;

/**
 * The config service manages configuration records for platform services. The configuration mechanics are discussed in the documentation of
 * the {@link IConfig}. The config service is accessed by client {@link IConfigServiceProxy} instances that create relevent {@link IConfig}
 * objects.
 * <p>
 * <h3>Internals</h3>
 * The config service reads existing config from a persistent store and publishes all config records it finds. The config record store is
 * accessed using {@link IConfigPersist} which has a default implementation where records are simply the serialised
 * form of an {@link IRecord} as performed by {@link ContextUtils#serializeRecordToFile(IRecord, File)}. The config directory is the
 * 'config' directory that is in the working directory of the process. This can be changed by setting the system property for
 * {@link ConfigServiceProperties.Values#CONFIG_DIR} to the full path of the config directory (a 'config' directory will NOT be created in
 * this user specified directory location).
 * <p>
 * The config service will poll the config store every 60 seconds to check for config changes. By default any changes to the files in the
 * directory are published. The changes that are detected are: file modified timestamp, file size change, new files and deleted files.
 * <p>
 * <b>Defaults:</b> on startup, the classpath is searched for any directory called 'defaultConfig'. The files in here will be copied into 
 * the 'config' directory. If the config directory already contains the file, it is not copied from defaultConfig. This mechanism allows 
 * default configuration to be packaged in a jar file and unpacked as initial defaults.
 * <p>
 * A custom implementation of {@link IConfigPersist} can be defined using {@link ConfigServiceProperties.Values#CONFIG_PERSIST_CLASS}.
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
	 * @see ConfigUtils#getPlatformServiceInstance(String, String, IConfig, com.fimtra.clearconnect.IPlatformRegistryAgent)
	 */
	static IPlatformServiceInstance constructConfigServiceInstance(File configDir, PlatformRegistryAgent agent, String kernelHost)
			throws IOException, FileNotFoundException {
		File[] propertyFiles = FileUtils.readFiles(configDir, ConfigDirReader.propertyFileFilter);
		IConfig config = null;
		final String configServicePropertiesFileName = IConfigServiceProxy.CONFIG_SERVICE + "." + ConfigDirReader.propertyFileExtension;
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

    protected ConfigService(String registryHost, int registryPort) {
        this(new EndPointAddress(registryHost, registryPort));
    }
    
	protected ConfigService(EndPointAddress registryEndpoint) {
		try {
			this.scheduledExecutor = ThreadUtils.newScheduledExecutorService("config-polling", 1);

			File configDir = new File(ConfigServiceProperties.Values.CONFIG_DIR);
			if (!configDir.exists()) {
				configDir.mkdir();
			}
			
			unpackDefaultConfig(configDir);
			
			IConfigPersist configPersist = ConfigPersistFactory.getInstance(configDir).getIConfigPersist();

			this.platformRegistryAgent = new PlatformRegistryAgent(IConfigServiceProxy.CONFIG_SERVICE, registryEndpoint);

			this.platformServiceInstance = constructConfigServiceInstance(configDir, this.platformRegistryAgent, registryEndpoint.getNode());

			this.configPublisher = new ConfigPublisher(configPersist, this.platformServiceInstance);
			publishConfigRecords(ConfigServiceProperties.Values.POLLING_PERIOD_SECS);
			publishRpcs(this.platformServiceInstance, configPersist);
		} catch (Exception e) {
			throw new RuntimeException("Could not construct config service", e);
		}
	}

	public void destroy() {
		ConfigPersistFactory.reset();
		this.scheduledExecutor.shutdownNow();
		this.platformRegistryAgent.destroy();
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		destroy();
	}
	
    void unpackDefaultConfig(final File configDir) throws IOException, MalformedURLException
    {
        final String defaultConfig = "defaultConfig";
        final URL resource = getClass().getClassLoader().getResource(defaultConfig);
        if (resource != null)
        {
            final File resourceFile = new File(resource.getFile());
            if (resourceFile.isDirectory())
            {
                Log.log(this, "Default configuration files found: ", resourceFile.getPath());
                for (File defaultConfigFile : resourceFile.listFiles())
                {
                    if(defaultConfigFile.isDirectory())
                    {
                        continue;
                    }
                    
                    final File configFile = new File(configDir, defaultConfigFile.getName());
                    if (!configFile.exists())
                    {
                        Log.log(this, "Creating default config: ", configFile.getPath());
                        FileUtils.copyFile(defaultConfigFile, configFile);
                    }
                    else
                    {
                        Log.log(this, "Config already exists: ", configFile.getPath());
                    }
                }
            }
            else
            {
                String canonicalPath = resourceFile.getParent();
                if (canonicalPath.endsWith("jar!"))
                {
                    canonicalPath = canonicalPath.substring(0, canonicalPath.length() - 1);
                    Log.log(this, "Default configuration files found (jar): ", canonicalPath);
                    ZipFile configJar = null;
                    try
                    {
                        configJar = new ZipFile(new URL(canonicalPath).getFile());
                        ZipEntry configFileZipEntry;
                        final Enumeration<? extends ZipEntry> entries = configJar.entries();
                        while (entries.hasMoreElements())
                        {
                            configFileZipEntry = entries.nextElement();
                            if (configFileZipEntry.getName().startsWith(defaultConfig, 0))
                            {
                                if (configFileZipEntry.isDirectory())
                                {
                                    continue;
                                }
                                final File configFile = new File(configDir,
                                    configFileZipEntry.getName().substring(defaultConfig.length() + 1));
                                if (!configFile.exists())
                                {
                                    Log.log(this, "Creating default config: ", configFile.getPath());
                                    final InputStream configInputStream = configJar.getInputStream(configFileZipEntry);
                                    try
                                    {
                                        FileUtils.writeInputStreamToFile(configInputStream, configFile);
                                    }
                                    finally
                                    {
                                        FileUtils.safeClose(configInputStream);
                                    }
                                }
                                else
                                {
                                    Log.log(this, "Config already exists: ", configFile.getPath());
                                }
                            }
                        }
                    }
                    finally
                    {
                        FileUtils.safeClose(configJar);
                    }
                }
                else
                {
                    Log.log(this, "Unknown default config resource:", ObjectUtils.safeToString(resourceFile));
                }
            }
        }
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
	private void publishRpcs(IPlatformServiceInstance platformServiceInstance, IConfigPersist configPersist) {
		new RpcCreateOrUpdateMemberConfig(this, configPersist, platformServiceInstance);
		new RpcDeleteMemberConfig(this, configPersist, platformServiceInstance);
		new RpcCreateOrUpdateFamilyConfig(this, configPersist, platformServiceInstance);
		new RpcDeleteFamilyConfig(this, configPersist, platformServiceInstance);
	}

}
