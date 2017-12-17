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
 * Tests for the {@link ByteArrayPool}
 * @author Ramon Servadei
 */
public class ByteArrayPoolTest
{
    @Before
    public void setUp() throws Exception
    {
    }
    
    @Test
    public void testGetOffer()
    {
        final byte[] b1 = ByteArrayPool.get(23);
        final byte[] b2 = ByteArrayPool.get(25);
        assertNotSame(b1, b2);
        assertEquals(b1.length, b2.length);
        
        ByteArrayPool.offer(b2);
        final byte[] b3 = ByteArrayPool.get(26);
        assertSame(b3, b2);
    }

    @Test
    public void testGetIndex()
    {
        for(int i = 0; i < ByteArrayPool.POOLS.length * 2; i++)
        {
            int index = ByteArrayPool.getIndex(i);
            assertTrue("index=" + index + " for size=" + i, index >= i);
        }
        
        assertEquals(1024, ByteArrayPool.getIndex(1024));
    }

}
