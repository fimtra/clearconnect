/*
 * Copyright (c) 2013 Paul Mackinlay, Fimtra
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.clearconnect.config.impl;

import java.util.HashMap;
import java.util.Map;

import com.fimtra.clearconnect.IPlatformServiceProxy;
import com.fimtra.clearconnect.config.ConfigServiceProperties;
import com.fimtra.clearconnect.config.IConfig;
import com.fimtra.clearconnect.config.IConfigManager;
import com.fimtra.clearconnect.config.IConfigServiceProxy;
import com.fimtra.clearconnect.core.PlatformUtils;

/**
 * Implementation of {@link IConfigServiceProxy}. This is a tailored view of the config services {@link IPlatformServiceProxy}.
 *
 * @author Paul Mackinlay
 */
public final class ConfigServiceProxy implements IConfigServiceProxy {

	private final IPlatformServiceProxy proxyForConfigService;
	private final Map<String, ConfigManager> configManagers;

	public ConfigServiceProxy(IPlatformServiceProxy proxyForConfigService) {
		this.proxyForConfigService = proxyForConfigService;
		this.configManagers = new HashMap<String, ConfigManager>();
	}

	@Override
	public IConfig getConfig(String serviceFamily, String serviceMember) {
		return new Config(serviceFamily, serviceMember, this.proxyForConfigService);
	}

	@Override
	public IConfigManager getConfigManager(String serviceFamily, String serviceMember) {
		String key = PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember);
		if (!this.configManagers.containsKey(key)) {
			this.configManagers.put(key, new ConfigManager(this.proxyForConfigService, serviceFamily, serviceMember,
					ConfigServiceProperties.Values.DEFAULT_CONFIG_MANAGER_RPC_TIMEOUT_MILLIS));
		}
		return this.configManagers.get(key);
	}
}
