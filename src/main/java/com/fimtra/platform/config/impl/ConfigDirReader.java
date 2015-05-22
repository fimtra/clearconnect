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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fimtra.util.FileUtils;
import com.fimtra.util.FileUtils.ExtensionFileFilter;
import com.fimtra.util.is;

/**
 * Responsible for reading a config directory and maintaining a file cache of record files.
 * 
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
class ConfigDirReader {

	static final ExtensionFileFilter recordFileFilter = new FileUtils.ExtensionFileFilter(FileUtils.recordFileExtension);
	static final ExtensionFileFilter propertyFileFilter = new FileUtils.ExtensionFileFilter(FileUtils.propertyFileExtension);

	/**
	 * Encapsulates meta data for a config file.
	 * 
	 * @author Ramon Servadei
	 * @author Paul Mackinlay
	 */
	private static final class FileMetaData {
		boolean canRead;
		long fileSize;
		long lastModTimestamp;
		String serviceInstanceNameFromFile;

		FileMetaData(String serviceInstanceNameFromFile, boolean canRead, long lastModTimestamp, long fileSize) {
			super();
			this.canRead = canRead;
			this.serviceInstanceNameFromFile = serviceInstanceNameFromFile;
			this.lastModTimestamp = lastModTimestamp;
			this.fileSize = fileSize;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (this.canRead ? 1231 : 1237);
			result = prime * result + (int) (this.lastModTimestamp ^ (this.lastModTimestamp >>> 32));
			result = prime * result + (int) (this.fileSize ^ (this.fileSize >>> 32));
			result = prime * result + ((this.serviceInstanceNameFromFile == null) ? 0 : this.serviceInstanceNameFromFile.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (is.same(this, obj)) {
				return true;
			}
			if (is.differentClass(this, obj)) {
				return false;
			}
			final FileMetaData other = (FileMetaData) obj;
			return is.eq(this.canRead, other.canRead) && is.eq(this.lastModTimestamp, other.lastModTimestamp)
					&& is.eq(this.fileSize, other.fileSize) && is.eq(this.serviceInstanceNameFromFile, other.serviceInstanceNameFromFile);
		}

	}

	private final File configDir;
	private final Map<File, FileMetaData> fileCache;

	ConfigDirReader(File configDir) {
		this.configDir = configDir;
		this.fileCache = new HashMap<File, FileMetaData>();
	}

	/**
	 * Updates the record file cache and returns the {@link List} of {@link File}s that have
	 * changed.
	 */
	List<File> updateRecordFileCache() {
		final File[] propertyFiles = FileUtils.readFiles(this.configDir, recordFileFilter);
		final List<File> changedFiles = new ArrayList<File>(propertyFiles.length);
		final List<File> allFiles = new ArrayList<File>(propertyFiles.length);
		FileMetaData fileMetaData;
		for (File propertyFile : propertyFiles) {
			allFiles.add(propertyFile);
			fileMetaData = new FileMetaData(FileUtils.getRecordNameFromFile(propertyFile), propertyFile.canRead(),
					propertyFile.lastModified(), propertyFile.length());
			if (!(this.fileCache.containsKey(propertyFile)) || !(this.fileCache.get(propertyFile).equals(fileMetaData))) {
				this.fileCache.put(propertyFile, fileMetaData);
				changedFiles.add(propertyFile);
			}
		}
		final Set<File> deletedFiles = new HashSet<File>(this.fileCache.keySet());
		deletedFiles.removeAll(allFiles);
		changedFiles.addAll(deletedFiles);
		return changedFiles;
	}

	File getConfigDir() {
		return this.configDir;
	}

}
