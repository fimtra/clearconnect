/*
 * Copyright (c) 2018 Ramon Servadei
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

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link LowGcLinkedList}
 * 
 * @author Ramon Servadei
 */
public class LowGcLinkedListTest
{
    LowGcLinkedList<String> candidate;
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        candidate = new LowGcLinkedList<String>();
    }

    @Test
    public void testNodeReuse()    
    {
        assertNull(candidate.spare);
        
        // add 3 items to the list
        candidate.offer("1");
        assertNull(candidate.spare);
        candidate.offer("2");
        candidate.offer("3");
        
        // remove first
        assertEquals("1", candidate.pollFirst());
        assertNotNull(candidate.spare);
        assertNull(candidate.spare.next);
        assertNull(candidate.spare.prev);
        assertNull(candidate.spare.item);

        // remove last
        assertEquals("3", candidate.pollLast());
        assertNotNull(candidate.spare);
        assertNull(candidate.spare.next);
        assertNotNull(candidate.spare.prev);
        assertNull(candidate.spare.item);
        // check the prev
        assertNull(candidate.spare.prev.next);
        assertNull(candidate.spare.prev.prev);
        assertNull(candidate.spare.prev.item);

        // remove - list is empty after this
        assertEquals("2", candidate.pollFirst());
        assertNotNull(candidate.spare);
        assertNull(candidate.spare.next);
        assertNotNull(candidate.spare.prev);
        assertNull(candidate.spare.item);
        // check the prev
        assertNull(candidate.spare.prev.next);
        assertNotNull(candidate.spare.prev.prev);
        assertNull(candidate.spare.prev.item);
        // check the prev.prev
        assertNull(candidate.spare.prev.prev.next);
        assertNull(candidate.spare.prev.prev.prev);
        assertNull(candidate.spare.prev.prev.item);
        
        // now add - check we re-use
        candidate.offer("4");
        assertNotNull(candidate.spare);
        assertNull(candidate.spare.next);
        assertNotNull(candidate.spare.prev);
        assertNull(candidate.spare.item);
        // check the prev
        assertNull(candidate.spare.prev.next);
        assertNull(candidate.spare.prev.prev);
        assertNull(candidate.spare.prev.item);
        
        candidate.offer("5");
        assertNotNull(candidate.spare);
        assertNull(candidate.spare.next);
        assertNull(candidate.spare.prev);
        assertNull(candidate.spare.item);
        
        candidate.offer("6");
        assertNull(candidate.spare);
    }

}
