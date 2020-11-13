/*
 * Copyright (c) 2013 Paul Mackinlay
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
package com.fimtra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;

import com.fimtra.util.FileUtils.ExtensionFileFilter;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Paul Mackinlay
 */
@SuppressWarnings("boxing")
public class FileUtilsTest {

	private static final String logFileName = "Test-messages_20151122_221934.log.0.logged";
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
	public void testWriteInputStreamToFile() throws IOException
    {
        String string = "hello-" + UUID.randomUUID();
        for (int i = 0; i < 1000; i++)
        {
            string += "-hello-" + UUID.randomUUID();
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(string.getBytes());
        final File file = new File("target/testWriteInputStreamToFile.txt");
        FileUtils.writeInputStreamToFile(bis, file);
        assertEquals(string, new BufferedReader(new FileReader(file)).readLine());
    }
	
	@Test
	public void testCopyMoveDeleteDirectory() throws IOException {
		final File srcDir = new File("./logs");
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

	@Test
	public void shouldGzipFile() throws URISyntaxException {
		File sourceFile = new File(this.getClass().getClassLoader().getResource(logFileName).toURI());
		File dir = sourceFile.getParentFile();

		assertTrue(FileUtils.gzip(sourceFile, dir));
		File gzipFile = new File(dir, logFileName + ".gz");
		assertTrue(gzipFile.isFile() && gzipFile.exists());
		assertTrue(gzipFile.length() < sourceFile.length());
		gzipFile.delete();

		File newDir = new File(dir.getAbsolutePath() + File.separator + "level1" + File.separator + "level2");
		assertTrue(FileUtils.gzip(sourceFile, newDir));
		gzipFile = new File(newDir, logFileName + ".gz");
		assertTrue(gzipFile.isFile() && gzipFile.exists());
		assertTrue(gzipFile.length() < sourceFile.length());
		gzipFile.delete();

		assertFalse(FileUtils.gzip(sourceFile, sourceFile));
	}

	@Test
	public void shouldFindOldFiles() throws URISyntaxException {
		File dir = new File("./..");

		File[] files = FileUtils.findFiles(dir, 1);
		assertTrue(files.length > 0);

		files = FileUtils.findFiles(dir, Long.MAX_VALUE);
		assertEquals(0, files.length);
	}

	@Test
	public void shouldArchiveAndPurge() {
		FileUtils.archiveLogs(1);
		FileUtils.purgeArchiveLogs(1);
	}
}
