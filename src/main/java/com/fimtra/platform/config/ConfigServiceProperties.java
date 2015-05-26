/*
 * Copyright (c) 2013 Ramon Servadei, Paul Mackinlay, Fimtra
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
package com.fimtra.platform.config;

import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.platform.PlatformCoreProperties;
import com.fimtra.platform.config.impl.ConfigService;

/**
 * Defines the system-level properties and property keys used for the {@link ConfigService}.
 * 
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public abstract class ConfigServiceProperties {

	/**
	 * The names of config service properties
	 * 
	 * @author Ramon Servadei
	 */
	public static interface Names {
		/**
		 * The base token for the name-space for configService specific property names.
		 */
		String BASE = PlatformCoreProperties.Names.BASE + "configService.";

		/**
		 * The system property name to define the directory to store the {@link IConfig} records.<br>
		 * E.g. <code>-Dplatform.configService.configDir=./logs</code>
		 */
		String CONFIG_DIR = BASE + "configDir";

		/**
		 * The system property name to define the polling period (in seconds) to discover changes to
		 * any {@link IConfig} records in the config directory.<br>
		 * E.g. <code>-Dplatform.configService.pollingIntervalSec=3600</code>
		 */
		String CONFIG_POLLING_INTERVAL_SEC = BASE + "pollingIntervalSec";

		/**
		 * The system property name to define the {@link IConfigManager} RPC timeout in milliseconds<br>
		 * E.g. <code>-Dplatform.configService.configManagerRpcTimeoutMillis=3000</code>
		 */
		String CONFIG_MANAGER_RPC_TIMEOUT_MILLIS = BASE + "configManagerRpcTimeoutMillis";

		/**
		 * The system property name to define the {@link IConfig} RPC timeout in milliseconds<br>
		 * E.g. <code>-Dplatform.configService.configRpcTimeoutMillis=3000</code>
		 */
		String CONFIG_RPC_TIMEOUT_MILLIS = BASE + "configRpcTimeoutMillis";
	}

	/**
	 * The values of config service properties described in {@link Names}
	 * 
	 * @author Ramon Servadei
	 */
	public static interface Values {
		/**
		 * The directory for the storing the {@link IConfig} records. Default is
		 * 
		 * <pre>
		 * {user.dir}/config
		 * </pre>
		 * 
		 * @see Names#CONFIG_DIR
		 */
		String CONFIG_DIR = System.getProperty(Names.CONFIG_DIR, System.getProperty("user.dir") + System.getProperty("file.separator")
				+ "config");

		/**
		 * The polling period in seconds to check for changes in {@link IConfig} record files.
		 * Default is 60 seconds.
		 * 
		 * @see Names#CONFIG_POLLING_INTERVAL_SEC
		 */
		int POLLING_PERIOD_SECS = Integer.parseInt(System.getProperty(Names.CONFIG_POLLING_INTERVAL_SEC, "60"));

		/**
		 * The RPC timeout for the {@link IConfigManager} in milliseconds.
		 * <p>
		 * <b>This should not be less than (2 x DataFissionProperties.Values.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS)</b>
		 * 
		 * @see Names#CONFIG_MANAGER_RPC_TIMEOUT_MILLIS
		 */
		public static final long DEFAULT_CONFIG_MANAGER_RPC_TIMEOUT_MILLIS = Integer.parseInt(System.getProperty(
				Names.CONFIG_MANAGER_RPC_TIMEOUT_MILLIS, "" + (2 * DataFissionProperties.Values.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS)));

		/**
		 * The RPC timeout for {@link IConfig} in milliseconds.
		 * <p>
		 * <b>This should not be less than (2 x DataFissionProperties.Values.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS)</b>
		 * 
		 * @see Names#CONFIG_RPC_TIMEOUT_MILLIS
		 */
		public static final long DEFAULT_CONFIG_RPC_TIMEOUT_MILLIS = Integer.parseInt(System.getProperty(Names.CONFIG_RPC_TIMEOUT_MILLIS,
				"" + (2 * DataFissionProperties.Values.PROXY_CONTEXT_RECONNECT_PERIOD_MILLIS)));
	}

	private ConfigServiceProperties() {
	}
}
