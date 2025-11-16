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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fimtra.datafission.core.ContextUtils;
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

    static final String propertyFileExtension = "properties";
	static final ExtensionFileFilter propertyFileFilter = new FileUtils.ExtensionFileFilter(ConfigDirReader.propertyFileExtension);

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

	// These are lazy initialised and reused
	private List<File> changedFilesCollection;
	private List<File> allFilesCollection;
	private Set<File> deletedFilesCollection;

	ConfigDirReader(File configDir) {
		this.configDir = configDir;
		this.fileCache = new HashMap<>();
	}

	/**
	 * Updates the record file cache and returns the {@link List} of {@link File}s that have
	 * changed.
	 */
	List<File> updateRecordFileCache() {
		final File[] recordFiles = FileUtils.readFiles(this.configDir, ContextUtils.RECORD_FILE_FILTER);
		final List<File> changedFiles = emptyChangedFiles(recordFiles);
		final List<File> allFiles = emptyAllFiles(recordFiles);
		FileMetaData fileMetaData;
		for (File recordFile : recordFiles) {
			allFiles.add(recordFile);
			fileMetaData = new FileMetaData(ContextUtils.getRecordNameFromFile(recordFile), recordFile.canRead(),
					recordFile.lastModified(), recordFile.length());
			if (!(this.fileCache.containsKey(recordFile)) || !(this.fileCache.get(recordFile).equals(fileMetaData))) {
				this.fileCache.put(recordFile, fileMetaData);
				changedFiles.add(recordFile);
			}
		}
		final Set<File> deletedFiles = setWithCachedFiles();
		deletedFiles.removeAll(allFiles);
		changedFiles.addAll(deletedFiles);
		return changedFiles;
	}

	private Set<File> setWithCachedFiles() {
		if (this.deletedFilesCollection == null) {
			this.deletedFilesCollection = new HashSet<>(this.fileCache.keySet());
			return this.deletedFilesCollection;
		}
		this.deletedFilesCollection.clear();
		this.deletedFilesCollection.addAll(this.fileCache.keySet());
		return this.deletedFilesCollection;
	}

	private List<File> emptyChangedFiles(final File[] propertyFiles) {
		if (this.changedFilesCollection == null) {
			this.changedFilesCollection = new ArrayList<>(propertyFiles.length);
			return this.changedFilesCollection;
		}
		this.changedFilesCollection.clear();
		return this.changedFilesCollection;
	}

	private List<File> emptyAllFiles(final File[] propertyFiles) {
		if (this.allFilesCollection == null) {
			this.allFilesCollection = new ArrayList<>(propertyFiles.length);
			return this.allFilesCollection;
		}
		this.allFilesCollection.clear();
		return this.allFilesCollection;
	}

	File getConfigDir() {
		return this.configDir;
	}

}
