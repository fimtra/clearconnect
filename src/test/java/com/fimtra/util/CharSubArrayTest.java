/*
 * Copyright (c) 2017 Ramon Servadei
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

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link CharSubArray}
 * 
 * @author Ramon Servadei
 */
public class CharSubArrayTest
{
    CharSubArray candidate;
    
    @Before
    public void setUp() throws Exception
    {
        candidate = new CharSubArray("hello-world!".toCharArray(), 6, 5);
    }

    @Test
    public void testHashCodeAndEquals()
    {
        assertEquals(candidate.hashCode(), candidate.hashCode());
        assertEquals(candidate.hashCode(), new CharSubArray("hello-world!".toCharArray(), 6, 5).hashCode());
        assertEquals(candidate.hashCode(), new CharSubArray("bye-world!".toCharArray(), 4, 5).hashCode());
        assertEquals(candidate.hashCode(), new CharSubArray("world".toCharArray(), 0, 5).hashCode());

        assertFalse(candidate.hashCode() == (new CharSubArray("world!".toCharArray(), 1, 5)).hashCode());
        
        assertFalse(candidate.equals(new CharSubArray("world!".toCharArray(), 1, 5)));
        assertEquals(candidate, candidate);
        assertEquals(candidate, new CharSubArray("hello-world!".toCharArray(), 6, 5));
        assertEquals(candidate, new CharSubArray("bye-world!".toCharArray(), 4, 5));
        assertEquals(candidate, new CharSubArray("world".toCharArray(), 0, 5));
        
        assertFalse(candidate.equals(new CharSubArray("world!".toCharArray(), 1, 5)));
    }

}
