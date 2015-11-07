/*
 * Copyright (c) 2014 Ramon Servadei 
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.util.SerializationUtils;

/**
 * Tests for {@link SerializationUtils}
 * @author Ramon Servadei
 */
public class SerializationUtilsTest
{
    @Before
    public void setUp() throws Exception
    {
    }

    @After
    public void tearDown()
    {
        new File(".", "SerializationUtilsTest.bin").delete();
    }
    
    @Test
    public void test() throws FileNotFoundException, IOException, ClassNotFoundException
    {
        File file = new File(".", "SerializationUtilsTest.bin");
        String expected = "result string";
        SerializationUtils.serializeToFile(expected, file);
        String result = SerializationUtils.resolveFromFile(file);
        assertEquals(expected, result);
    }
    
    @Test
    public void testToFromByteArr() throws ClassNotFoundException, IOException
    {
        assertEquals("Hello", SerializationUtils.fromByteArray(SerializationUtils.toByteArray("Hello")));
    }

}
