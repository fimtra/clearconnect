/*
 * Copyright (c) 2013 Ramon Servadei 
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link GZipUtils}
 * 
 * @author Ramon Servadei
 */
public class GZipUtilsTest
{
    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testSimpleZipUnzip()
    {
        String s = "hello";
        final byte[] zipped = GZipUtils.compress(s.getBytes());
        final byte[] unzipped = GZipUtils.uncompress(zipped);
        assertEquals(s, new String(unzipped));
    }
    
    @Test
    public void testMassiveSimpleZipUnzip()
    {
        final int MAX = 10000;
        Random rnd = new Random();
        StringBuilder sb = new StringBuilder(MAX * 10);
        for(int i = 0; i < MAX; i++)
        {
            sb.append("hello" + i + rnd.nextInt());
        }
        final String s = sb.toString();
        final byte[] zipped = GZipUtils.compress(s.getBytes());
        System.err.println("Zipped []=" + zipped.length + ", original=" + s.length());
        assertTrue("Zipped []=" + zipped.length + ", original=" + s.length(), zipped.length < s.length());
        final byte[] unzipped = GZipUtils.uncompress(zipped);
        assertEquals(s, new String(unzipped));
    }

	@Test
	public void shouldCompressStream() throws Exception {
		File sourceFile = new File(this.getClass().getClassLoader().getResource("Test-messages_20151122_221934.log.0.logged").toURI());
		File targetFile = new File("discard.gz");
		FileInputStream inputStream = new FileInputStream(sourceFile);
		FileOutputStream outputStream = new FileOutputStream(targetFile);
		try {
			GZipUtils.compressInputToOutput(inputStream, outputStream);
			assertTrue(sourceFile.length() > targetFile.length());
		} finally {
			targetFile.delete();
			outputStream.close();
			inputStream.close();
		}
    }
}
