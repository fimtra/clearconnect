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
