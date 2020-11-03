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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link KeyedObjectPool}
 * 
 * @author Ramon Servadei
 */
public class KeyedObjectPoolTest
{
    KeyedObjectPool<String, char[]> candidate;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new KeyedObjectPool<>("sdf", 2);
    }

    @Test
    public void testPoolLimit()
    {
        this.candidate.intern("hello", "hello".toCharArray());
        assertNotNull(this.candidate.get("hello"));
        assertSame(this.candidate.get("hello"), this.candidate.get("hello"));

        this.candidate.intern("world", "world".toCharArray());
        this.candidate.intern("more", "more".toCharArray());

        assertEquals(this.candidate.order.size(), 2);
        assertEquals("[world, more]", this.candidate.order.toString());
        assertEquals(this.candidate.pool.size(), 2);

        assertNull(this.candidate.get("hello"));
        assertNotNull(this.candidate.get("world"));
        assertSame(this.candidate.get("world"), this.candidate.get("world"));
        assertNotNull(this.candidate.get("more"));
        assertSame(this.candidate.get("more"), this.candidate.get("more"));
    }

}
