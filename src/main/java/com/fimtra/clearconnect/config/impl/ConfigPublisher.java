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
package com.fimtra.clearconnect.config.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.util.FileUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;

/**
 * Responsible for publishing config.
 * 
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
final class ConfigPublisher implements Runnable {

	private final ConfigDirReader configDirReader;
	private final IPlatformServiceInstance platformServiceInstance;

	ConfigPublisher(ConfigDirReader configDirReader, IPlatformServiceInstance platformServiceInstance) {
        Log.log(this, "ConfigDir=", ObjectUtils.safeToString(configDirReader.getConfigDir()));
		this.configDirReader = configDirReader;
		this.platformServiceInstance = platformServiceInstance;
	}

	@Override
	public void run() {
		List<File> updatedFiles = this.configDirReader.updateRecordFileCache();
		if (!updatedFiles.isEmpty()) {
			Log.log(this, "Updated files ", ObjectUtils.safeToString(updatedFiles));
		}
		String recordName;
		IRecord record;
		for (File file : updatedFiles) {
			recordName = FileUtils.getRecordNameFromFile(file);
			record = this.platformServiceInstance.getOrCreateRecord(recordName);
			if (record != null) {
				try {
					ContextUtils.resolveRecordFromFile(record, this.configDirReader.getConfigDir());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				Log.log(this, "Publishing config change for ", recordName);
				this.platformServiceInstance.publishRecord(record);
			}
		}
	}

}
