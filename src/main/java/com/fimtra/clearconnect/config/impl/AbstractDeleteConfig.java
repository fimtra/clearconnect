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
package com.fimtra.clearconnect.config.impl;

import java.io.File;
import java.io.IOException;

import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.datafission.field.TextValue;
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
