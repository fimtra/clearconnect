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

import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;
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
	private final StringBuilder responseBuilder;
	private final IConfigPersist configPersist;
	final IPlatformServiceInstance platformServiceInstance;

	AbstractDeleteConfig(ConfigService configService, IConfigPersist configPersist, IPlatformServiceInstance platformServiceInstance) {
		this.configService = configService;
		this.configPersist = configPersist;
		this.responseBuilder = new StringBuilder();
		this.platformServiceInstance = platformServiceInstance;
	}

	IValue deleteConfig(String configRecordName, String configKey) {
		this.responseBuilder.setLength(0);
		IRecord record = this.platformServiceInstance.getOrCreateRecord(configRecordName);
		try {
			this.configPersist.populate(record);
		} catch (Exception e) {
			this.responseBuilder.append("RPC failed: unable to delete config from record file (").append(e.getMessage()).append(")");
			return TextValue.valueOf(this.responseBuilder.toString());
		}
		record.remove(configKey);
		try {
			this.configPersist.save(record);
		} catch (Exception e) {
			this.responseBuilder.append("RPC failed: unable to save config record file");
			String message = this.responseBuilder.toString();
			Log.log(this, message, e);
			return TextValue.valueOf(message);
		}
		this.configService.publishConfig();
		this.responseBuilder.append("Deleted config with key [").append(configKey).append("] for ").append(configRecordName);
		final String message = this.responseBuilder.toString();
		return TextValue.valueOf(message);
	}
}
