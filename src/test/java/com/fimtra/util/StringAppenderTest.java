/*
 * Copyright (c) 2019 Ramon Servadei
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

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link StringAppender}
 * 
 * @author Ramon Servadei
 */
public class StringAppenderTest
{
    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void test()
    {
        StringAppender candidate = new StringAppender(0);
        candidate.append('c').append(1l).append(0.1d).append("string").append("string".toCharArray()).append(
            "_string".toCharArray(), 1, 2).append((String) null);
        assertEquals("c10.1stringstringstnull", candidate.toString());
    }

}
