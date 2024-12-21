/*
 * Copyright (c) 2015 Ramon Servadei 
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
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link CollectionUtils}
 * 
 * @author Ramon Servadei
 */
public class CollectionUtilsTest
{
    @Before
    public void setUp() throws Exception
    {
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnmodifiableEntrySetIterator()
    {
        Map<Long, Long> m = new HashMap<>();
        CollectionUtils.unmodifiableEntrySet(m.entrySet()).iterator().remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnmodifiableEntrySetAdd()
    {
        Map<Long, Long> m = new HashMap<>();
        CollectionUtils.unmodifiableEntrySet(m.entrySet()).add(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnmodifiableEntrySetIteratorSetValue()
    {
        Map<Long, Long> m = new HashMap<>();
        m.put(1L, 1L);
        CollectionUtils.unmodifiableEntrySet(m.entrySet()).iterator().next().setValue(2L);
    }
    
    @Test
    public void testNewSetFromString()
    {
        HashSet<String> expected = new HashSet<>();
        assertEquals(expected, CollectionUtils.newSetFromString(null, ","));
        
        expected = new HashSet<>();
        expected.add("");
        assertEquals(expected, CollectionUtils.newSetFromString("", ","));
        assertEquals(expected, CollectionUtils.newSetFromString(" ", ","));
        
        expected = new HashSet<>();
        expected.add("1");
        expected.add("2");
        expected.add("3");
        assertEquals(expected, CollectionUtils.newSetFromString("1  , 2,3", ","));
    }

    @Test
    public void test_emptyIfNull()
    {
        final Map m = new HashMap();
        assertSame(m, CollectionUtils.emptyIfNull(m));
        assertSame(Collections.EMPTY_MAP, CollectionUtils.emptyIfNull(null));
    }

    @Test
    public void noopMap()
    {
        final Map<String, String> m = CollectionUtils.noopMap();
        assertTrue(m.isEmpty());
        assertEquals(0, m.size());

        assertNull(m.put("one", "two"));
        assertNull(m.put("one", "two"));
        // will still be empty
        assertTrue(m.isEmpty());
        assertEquals(0, m.size());

        assertNull(m.remove("one"));
        assertTrue(m.isEmpty());
        assertEquals(0, m.size());
    }
}
