/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config;

/**
 * Provides config service functionality.
 * 
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
public interface IConfigServiceProxy {

	/**
	 * The registered service family name for the configuration service.
	 */
	static final String CONFIG_SERVICE = "ConfigService";

	/**
	 * Gets the {@link IConfig} for the service instance. This provides read access of config for a service instance.
	 */
	IConfig getConfig(String serviceFamily, String serviceMember);

	/**
	 * Gets the {@link IConfigManager} for the service instance. This provides create, update and delete access of
	 * config for a service instance.
	 * 
	 * @param serviceFamily
	 * 				the family of the service instance
	 * @param serviceMember
	 * 				the member name of the service instance
	 * 
	 * @return the {@link IConfigManager}
	 */
	IConfigManager getConfigManager(String serviceFamily, String serviceMember);
}
