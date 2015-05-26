/*
 * Copyright (c) 2014 Paul Mackinlay, Fimtra
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

import com.fimtra.datafission.IValue;

/**
 * Provides access to configuration management for a platform service instance. This enables config for the service family
 * and the service member to be created, updated an deleted.
 *
 * @author Paul Mackinlay
 */
public interface IConfigManager {

	/**
	 * Creates or updates configuration for the service family. It will return true if config has been created or updated.
	 *
	 * @param configKey
	 *            the configuration key
	 * @param configValue
	 *            the configuration value
	 *
	 * @return true if the config was created or updated
	 */
	boolean createOrUpdateFamilyConfig(String configKey, IValue configValue);

	/**
	 * Deletes configuration for the service family. It will return true if config has been deleted.
	 *
	 * @param configKey
	 *            the key of the configuration the will be deleted
	 *
	 * @return true if the config was deleted
	 */
	boolean deleteFamilyConfig(String configKey);

	/**
	 * Creates or updates configuration for the service member. It will return true if config has been created or updated.
	 *
	 * @param configKey
	 *            the configuration key
	 * @param configValue
	 *            the configuration value
	 *
	 * @return true if the config was created or updated
	 */
	boolean createOrUpdateMemberConfig(String configKey, IValue configValue);

	/**
	 * Deletes configuration for the service member. It will return true if config has been deleted.
	 *
	 * @param configKey
	 *            the key of the configuration the will be deleted
	 *
	 * @return true if the config was deleted
	 */
	boolean deleteMemberConfig(String configKey);

}
