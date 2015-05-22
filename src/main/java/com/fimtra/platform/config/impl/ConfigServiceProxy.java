/*
 * Copyright (c) 2013 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config.impl;

import com.fimtra.platform.IPlatformServiceProxy;
import com.fimtra.platform.config.ConfigServiceProperties;
import com.fimtra.platform.config.IConfig;
import com.fimtra.platform.config.IConfigManager;
import com.fimtra.platform.config.IConfigServiceProxy;

/**
 * Implementation of {@link IConfigServiceProxy}. This is a tailored view of the config services {@link IPlatformServiceProxy}.
 *
 * @author Paul Mackinlay
 */
public final class ConfigServiceProxy implements IConfigServiceProxy {

	private final IPlatformServiceProxy proxyForConfigService;

	public ConfigServiceProxy(IPlatformServiceProxy proxyForConfigService) {
		this.proxyForConfigService = proxyForConfigService;
	}

	@Override
	public IConfig getConfig(String serviceFamily, String serviceMember) {
		return new Config(serviceFamily, serviceMember, this.proxyForConfigService);
	}

	@Override
	public IConfigManager getConfigManager(String serviceFamily, String serviceMember) {
		return new ConfigManager(this.proxyForConfigService, serviceFamily, serviceMember,
				ConfigServiceProperties.Values.DEFAULT_CONFIG_MANAGER_RPC_TIMEOUT_MILLIS);
	}
}
