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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link LazyObject}
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings("rawtypes")
public class LazyObjectTest
{
    LazyObject<Map> candidate;
    Map map;

    @Before
    public void setUp() throws Exception
    {
        this.map = new HashMap();
        this.candidate = new LazyObject<>(() -> this.map, (m) -> m.clear());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test()
    {
        assertNull(this.candidate.ref);
        this.candidate.get().put("one", "one");
        assertSame(this.map, this.candidate.ref);
        assertSame(this.map, this.candidate.get());
        assertEquals(1, this.map.size());

        this.candidate.destroy();
        assertEquals(0, this.map.size());
        assertNull(this.candidate.ref);
    }
}
