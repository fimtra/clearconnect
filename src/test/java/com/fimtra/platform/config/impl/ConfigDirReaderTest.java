/*
 * Copyright (c) 2013 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
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
