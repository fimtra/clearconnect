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
        Map<Long, Long> m = new HashMap<Long, Long>();
        CollectionUtils.unmodifiableEntrySet(m.entrySet()).iterator().remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnmodifiableEntrySetAdd()
    {
        Map<Long, Long> m = new HashMap<Long, Long>();
        CollectionUtils.unmodifiableEntrySet(m.entrySet()).add(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnmodifiableEntrySetIteratorSetValue()
    {
        Map<Long, Long> m = new HashMap<Long, Long>();
        m.put(1l, 1l);
        CollectionUtils.unmodifiableEntrySet(m.entrySet()).iterator().next().setValue(2l);
    }
    
    @Test
    public void testNewSetFromString()
    {
        HashSet<String> expected = new HashSet<String>();
        assertEquals(expected, CollectionUtils.newSetFromString(null));
        
        expected = new HashSet<String>();
        expected.add("");
        assertEquals(expected, CollectionUtils.newSetFromString(""));
        assertEquals(expected, CollectionUtils.newSetFromString(" "));
        
        expected = new HashSet<String>();
        expected.add("1");
        expected.add("2");
        expected.add("3");
        assertEquals(expected, CollectionUtils.newSetFromString("1  , 2,3"));
    }

}
