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
 * Encapsulates common logic for creating or updating configuration.
 *
 * @author Paul Mackinlay
 */
abstract class AbstractCreateOrUpdateConfig implements IRpcExecutionHandler {

	private final ConfigService configService;
	private final ConfigDirReader configDirReader;
	final IPlatformServiceInstance platformServiceInstance;

	AbstractCreateOrUpdateConfig(ConfigService configService, ConfigDirReader configDirReader,
			IPlatformServiceInstance platformServiceInstance) {
		this.configService = configService;
		this.configDirReader = configDirReader;
		this.platformServiceInstance = platformServiceInstance;
	}

	final IValue createOrUpdateConfig(String configRecordName, String configKey, String configValue) {
		StringBuilder responseBuilder = new StringBuilder();
		File configDir = this.configDirReader.getConfigDir();
		IRecord record = this.platformServiceInstance.getOrCreateRecord(configRecordName);
		try {
			ContextUtils.resolveRecordFromFile(record, configDir);
		} catch (IOException e) {
			responseBuilder.append("A new record file for ").append(configRecordName).append(" will be created. ");
		}
		record.put(configKey, configValue);
		try {
			ContextUtils.serializeRecordToFile(record, configDir);
		} catch (IOException e) {
			String message = "RPC failed: unable to save config record file (" + e.getMessage() + ")";
			Log.log(this, message, e);
			return new TextValue(message);
		}
		this.configService.publishConfig();
		responseBuilder.append("Amended config for ").append(configRecordName).append(" published: ").append(configKey).append("=")
				.append(configValue);
		String message = responseBuilder.toString();
		Log.log(this, message);
		return new TextValue(message);
	}
}