/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.platform.IPlatformServiceInstance;
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
