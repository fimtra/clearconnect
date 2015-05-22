/*
 * Copyright (c) 2014 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
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
