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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.fimtra.util.RollingFileAppender.AppendableFlushableCloseable;

/**
 * Tests the {@link RollingFileAppender}
 * 
 * @author Ramon Servadei
 */
public class RollingFileAppenderTest
{
    @Rule
    public TestName name = new TestName();

    File file;
    RollingFileAppender candidate;

    @Before
    public void setUp() throws Exception
    {
        this.file = new File(this.name.getMethodName() + ".log");
        deleteLogged();
        this.candidate = new RollingFileAppender(this.file, 10, 5l, this.name.getMethodName());
    }

    @After
    public void tearDown() throws Exception
    {
        deleteLogged();
    }

    void deleteLogged()
    {
        new File(this.name.getMethodName()).delete();
        final File[] files = FileUtils.readFiles(new File("."), new FileUtils.ExtensionFileFilter("logged"));
        for (File file : files)
        {
            file.delete();
        }
    }

    @Test
    public void testAppendCharSequence() throws IOException
    {
        this.candidate.append("hello").append(" this is ").append(" some text").append('\n').append(
            "This is some more text", 13, 17).append("This is some more text", 17, 22);
        this.candidate.flush();

        final File[] files = FileUtils.readFiles(new File("."), new FileUtils.ExtensionFileFilter("logged"));
        assertEquals(4, files.length);
    }

    @Test
    public void testEmergency() throws IOException
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream current = System.err;
        try
        {
            System.setErr(new PrintStream(out));
            final AppendableFlushableCloseable mock = mock(AppendableFlushableCloseable.class);
            this.candidate.delegate = mock;
            try
            {
                // simulate some emergency situation
                this.candidate.switchToStdErr(new IOException("some error"));
                fail("Should throw IOException");
            }
            catch (IOException e)
            {
            }
            this.candidate.append("writing after emergency");
            final String result = new String(out.toByteArray());
            verify(mock).close();
            assertTrue(result.contains("ALERT!"));
            assertTrue(result.contains("some error"));
        }
        finally
        {
            System.setErr(current);
        }
    }
}
