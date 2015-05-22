/*
 * Copyright (c) 2014 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config.impl;

import java.io.File;
import java.io.IOException;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.platform.IPlatformServiceInstance;
import com.fimtra.util.Log;

/**
 * Encapsulates common logic for deleting configuration.
 * 
 * @author Paul Mackinlay
 */
abstract class AbstractDeleteConfig implements IRpcExecutionHandler {

	private final ConfigService configService;
	private final ConfigDirReader configDirReader;
	final IPlatformServiceInstance platformServiceInstance;

	AbstractDeleteConfig(ConfigService configService, ConfigDirReader configDirReader, IPlatformServiceInstance platformServiceInstance) {
		this.configService = configService;
		this.configDirReader = configDirReader;
		this.platformServiceInstance = platformServiceInstance;
	}

	IValue deleteConfig(String configRecordName, String configKey) {
		File configDir = this.configDirReader.getConfigDir();
		IRecord record = this.platformServiceInstance.getOrCreateRecord(configRecordName);
		try {
			ContextUtils.resolveRecordFromFile(record, configDir);
		} catch (IOException e) {
			return new TextValue("RPC failed: unable to delete config from record file (" + e.getMessage() + ")");
		}
		record.remove(configKey);
		try {
			ContextUtils.serializeRecordToFile(record, configDir);
		} catch (IOException e) {
			return new TextValue("RPC failed: unable to save config record file (" + e.getMessage() + ")");
		}
		this.configService.publishConfig();
		final String message = "Deleted config with key [" + configKey + "] for " + configRecordName;
		Log.log(this, message);
		return new TextValue(message);
	}
}
