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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;

public class FileSystemConfigPersist implements IConfigPersist {

	private final ConfigDirReader configDirReader;

	FileSystemConfigPersist(ConfigDirReader configDirReader) {
		this.configDirReader = configDirReader;
		Log.log(this, "ConfigDir=", ObjectUtils.safeToString(this.configDirReader.getConfigDir()));
	}

	@Override
	public void save(IRecord record) {
		try {
			ContextUtils.serializeRecordToFile(record, this.configDirReader.getConfigDir());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void populate(IRecord record) {
		try {
			ContextUtils.resolveRecordFromFile(record, this.configDirReader.getConfigDir());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Collection<String> getChangedRecordNames() {

		List<File> changedFiles = this.configDirReader.updateRecordFileCache();
		Collection<String> changedRecordNames = new ArrayList<String>(changedFiles.size());
		for (File file : changedFiles) {
			final String recordNameFromFile = ContextUtils.getRecordNameFromFile(file);
			if(recordNameFromFile != null)
			{
			    changedRecordNames.add(recordNameFromFile);
			}
		}
		return changedRecordNames;
	}

}
