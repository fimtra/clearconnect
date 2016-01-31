/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei, Fimtra
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.clearconnect.config.impl;

import java.util.Collection;

import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.datafission.IRecord;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;

/**
 * Responsible for publishing config.
 * 
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
final class ConfigPublisher implements Runnable {

	private final IPlatformServiceInstance platformServiceInstance;
	private final IConfigPersist configPersist;

	ConfigPublisher(IConfigPersist configPersist, IPlatformServiceInstance platformServiceInstance) {
		this.configPersist = configPersist;
		this.platformServiceInstance = platformServiceInstance;
	}

	@Override
	public void run() {
		Collection<String> recordNames = this.configPersist.getChangedRecordNames();
		if (!recordNames.isEmpty()) {
			Log.log(this, "Updated records ", ObjectUtils.safeToString(recordNames));
		}
		for (String recordName : recordNames) {
			IRecord record = this.platformServiceInstance.getOrCreateRecord(recordName);
			if (record != null) {
				this.configPersist.populate(record);
				Log.log(this, "Publishing config change for ", recordName);
				this.platformServiceInstance.publishRecord(record);
			}
		}
	}

}
