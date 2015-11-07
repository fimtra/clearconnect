/*
 * Copyright (c) 2013 Paul Mackinlay 
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
package com.fimtra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.util.FileUtils;
import com.fimtra.util.FileUtils.ExtensionFileFilter;

/**
 * @author Paul Mackinlay
 */
@SuppressWarnings("boxing")
public class FileUtilsTest {

	private static final String EXT_OK = "okext";
	private static final String EXT_RECORD = "record";
	private ExtensionFileFilter recordFileFilter;
	private File directory;

	@Before
	public void setUp() {
		this.recordFileFilter = new FileUtils.ExtensionFileFilter(EXT_RECORD, EXT_OK);
		this.directory = mock(File.class);
		when(this.directory.isDirectory()).thenReturn(Boolean.TRUE);
		when(this.directory.isFile()).thenReturn(Boolean.FALSE);
		when(this.directory.isHidden()).thenReturn(Boolean.FALSE);
	}

	@Test
	public void shouldReadFromDirectoryWithFilter() {
		FileUtils.readFiles(this.directory, this.recordFileFilter);
		verify(this.directory, times(1)).listFiles(this.recordFileFilter);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldNotReadFromFileWithFilter() {
		when(this.directory.isDirectory()).thenReturn(Boolean.FALSE);
		FileUtils.readFiles(this.directory, this.recordFileFilter);
	}

	@Test
	public void shouldTestExtensionFileFilter() {
		File file1 = new File("file1." + EXT_RECORD);
		File file2 = new File("file2.txt");
		File file3 = new File("file3." + EXT_OK);
		assertTrue(this.recordFileFilter.accept(file1));
		assertFalse(this.recordFileFilter.accept(file2));
		assertTrue(this.recordFileFilter.accept(file3));
	}

	@Test
	public void shouldGetRecordFromFile() {
		String[] validFileNames = new String[] { "test." + FileUtils.recordFileExtension, "test.another." + FileUtils.recordFileExtension };
		for (String fileName : validFileNames) {
			File recordFile = new File(fileName);
			assertEquals(fileName.substring(0, fileName.lastIndexOf(".")), FileUtils.getRecordNameFromFile(recordFile));
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldNotGetRecordFromFile() {
		File recordFile = new File("test.someext");
		FileUtils.getRecordNameFromFile(recordFile);
	}

	@Test
	public void testCopyMoveDeleteDirectory() throws IOException {
		final File srcDir = new File(System.getProperty("user.dir"));
		final File targetDir = new File(System.getProperty("java.io.tmpdir"), srcDir.getName());

		// copy
		FileUtils.copyRecursive(srcDir, targetDir);
		assertTrue(targetDir.exists());
		assertEquals(srcDir.listFiles().length, targetDir.listFiles().length);

		// move
		final File moveTarget = new File(targetDir.getParent(), "unit-test-files");
		FileUtils.move(targetDir, moveTarget);
		assertTrue(!targetDir.exists());
		assertTrue(moveTarget.exists());
		assertEquals(srcDir.listFiles().length, moveTarget.listFiles().length);

		// delete
		FileUtils.deleteRecursive(moveTarget);
		assertTrue(!moveTarget.exists());
		assertTrue(!targetDir.exists());
	}
}
