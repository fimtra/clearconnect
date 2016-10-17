/*
 * Copyright (c) 2016 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.clearconnect.config.impl;

import java.io.File;

import com.fimtra.clearconnect.config.ConfigServiceProperties;
import com.fimtra.util.Log;

public class ConfigPersistFactory {

	private static ConfigPersistFactory instance;
	private final IConfigPersist configPersist;

	private ConfigPersistFactory(File configDir) {

		String customPersistClassname = ConfigServiceProperties.Values.CONFIG_PERSIST_CLASS;
		if (customPersistClassname != null) {
			try {
				this.configPersist = (IConfigPersist) Class.forName(customPersistClassname).newInstance();
			} catch (Exception e) {
				Log.log(this, "It's not possible to construct custom persistence [", customPersistClassname,
						"]. A public no argument constructor is required.");
				throw new RuntimeException(e);
			}
		} else {
			this.configPersist = new FileSystemConfigPersist(new ConfigDirReader(configDir));
		}
	}

	/**
	 * Gets the instance of ConfigPersistFactory with confiDir as the filesystem persistent store which is used for the default
	 * implementation of {@link IConfigPersist}. Repeated calls to this method will return the same instance and ignore the subsequently
	 * used confiDir unless reset() is called first.
	 */
	static synchronized ConfigPersistFactory getInstance(File configDir) {
		if (instance == null) {
			instance = new ConfigPersistFactory(configDir);
		}
		return instance;
	}

	/**
	 * Resets the internal state of the factory, getInstance() will recreate it.
	 */
	synchronized static void reset() {
		instance = null;
	}

	IConfigPersist getIConfigPersist() {
		return this.configPersist;
	}
}
