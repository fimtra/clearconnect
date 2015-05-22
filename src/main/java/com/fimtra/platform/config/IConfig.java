/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config;

import java.util.Set;

import com.fimtra.datafission.IValue;
import com.fimtra.platform.core.PlatformUtils;

/**
 * Provides read access to configuration for a platform service instance. The configuration is
 * composed from 3 sources that are super-imposed over each other:
 * <ol>
 * <li>The configuration for the service family as retrieved from the configuration service
 * <li>The configuration for the service instance as retrieved from the configuration service
 * <li>The configuration specified in a local file; the file is in the {@link ConfigProperties#LOCAL_CONFIG_DIR} and is called
 * <tt>[serviceInstanceId].properties</tt>
 * </ol>
 * Local configuration overwrites service instance configuration which overwrites service family
 * family configuration.
 * <p>
 * Configuration that is changed in the configuration service can be notified of this config instance by attaching an
 * {@link IConfigChangeListener} via the {@link #addConfigChangeListener(IConfigChangeListener)}. This allows application code to react to
 * runtime configuration changes.
 * 
 * @see PlatformUtils#composePlatformServiceInstanceID(String, String)
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public interface IConfig {
	/**
	 * Gets the property identified by propertyKey.
	 */
	IValue getProperty(String propertyKey);

	/**
	 * @return all the property keys in this config
	 */
	Set<String> getPropertyKeys();

	/**
	 * Used to register for property changes.
	 */
	void addConfigChangeListener(IConfigChangeListener configChangeListener);

	/**
	 * Used to unregister for property changes.
	 */
	void removeConfigChangeListener(IConfigChangeListener configChangeListener);

	/**
	 * The listener that is used to register for property changes.
	 */
	public static interface IConfigChangeListener {
		/**
		 * The callback when a property changes.
		 */
		void onPropertyChange(String propertyKey, IValue propertyValue);
	}
}
