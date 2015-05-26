/*
 * Copyright (c) 2013 Paul Mackinlay, Fimtra
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
package com.fimtra.platform.config.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.platform.config.impl.ConfigDirReader;

/**
 * @author Paul Mackinlay
 */
public class ConfigDirReaderTest {

	private ConfigDirReader configDirReader;
	private File configDir;

	@SuppressWarnings("boxing")
    @Before
	public void setUp() {
		this.configDir = mock(File.class);
		this.configDirReader = new ConfigDirReader(this.configDir);

		when(this.configDir.isDirectory()).thenReturn(Boolean.TRUE);
		when(this.configDir.listFiles(any(FileFilter.class))).thenReturn(new File[] {});
	}

	@Test
	public void shouldNotUpdateIfNoConfigRecordFiles() {
		List<File> changedFiles = this.configDirReader.updateRecordFileCache();
		assertEquals(Collections.emptyList(), changedFiles);
	}

	@Test
	public void shouldUpdateChangedConfigRecordFiles() {
		File[] origFiles = new File[] { new File("test1.record"), new File("test2.record") };
		when(this.configDir.listFiles(any(FileFilter.class))).thenReturn(origFiles);
		List<File> changedFiles = this.configDirReader.updateRecordFileCache();
		assertEquals(Arrays.asList(origFiles), changedFiles);
		changedFiles = this.configDirReader.updateRecordFileCache();
		assertEquals(Collections.emptyList(), changedFiles);

		//add record
		File addedFile = new File("test3.record");
		File[] moreFiles = new File[] { new File("test1.record"), new File("test2.record"), addedFile };
		when(this.configDir.listFiles(any(FileFilter.class))).thenReturn(moreFiles);
		changedFiles = this.configDirReader.updateRecordFileCache();
		assertEquals(1, changedFiles.size());
		assertEquals(addedFile, changedFiles.get(0));

		//delete record
		when(this.configDir.listFiles(any(FileFilter.class))).thenReturn(origFiles);
		changedFiles = this.configDirReader.updateRecordFileCache();
		assertEquals(1, changedFiles.size());
		assertEquals(addedFile, changedFiles.get(0));
	}
}
