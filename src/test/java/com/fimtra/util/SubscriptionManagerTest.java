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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.util.ArrayUtils;
import com.fimtra.util.SubscriptionManager;

/**
 * Tests for the {@link SubscriptionManager}.
 * 
 * @author Ramon Servadei
 */
public class SubscriptionManagerTest
{
    private final static Integer K1 = Integer.valueOf(1);
    private final static Integer K2 = Integer.valueOf(2);
    private final static String S1 = "L1";
    private final static String S2 = "L2";
    private final static String S1_AND_2 = "L1_and_2";

    SubscriptionManager<Integer, String> candidate;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new SubscriptionManager<Integer, String>(String.class);
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testWhenEmpty()
    {
        assertNotNull(this.candidate.getSubscribersFor(null));
        assertEquals(0, this.candidate.getSubscribersFor(null).length);
        final Integer duffKey = new Integer(293847);
        assertNotNull(this.candidate.getSubscribersFor(duffKey));
        assertEquals(0, this.candidate.getSubscribersFor(duffKey).length);
    }

    @Test
    public void testRemovingSubscribers()
    {
        assertFalse(this.candidate.removeSubscriberFor(K1, S1));
        
        assertTrue(this.candidate.addSubscriberFor(K1, S1));
        // true invalid calls first
        assertFalse(this.candidate.removeSubscriberFor(K1, S2));
        assertFalse(this.candidate.removeSubscriberFor(K2, S1)); 
        
        assertTrue(this.candidate.removeSubscriberFor(K1, S1));
        assertFalse(this.candidate.removeSubscriberFor(K1, S1));
        
        // with no more subscribers, check the internal record is empty
        assertEquals(0, this.candidate.subscribersPerKey.size());
    }
    
    @Test
    public void testRemovingAllSubscribers()
    {
        
        assertTrue(this.candidate.addSubscriberFor(K1, S1));
        assertTrue(this.candidate.addSubscriberFor(K1, S2));
        
        assertNull(this.candidate.removeSubscribersFor(K2));
        final String[] removeSubscribersFor = this.candidate.removeSubscribersFor(K1);
        assertNotNull(removeSubscribersFor);
        assertNull(this.candidate.removeSubscribersFor(K1));

        assertEquals(2, removeSubscribersFor.length);
        
        assertEquals(0, this.candidate.subscribersPerKey.size());
    }
    
    @Test
    public void testAddingAndRemovingMultipleSubscribers()
    {
        // add K1->S1
        assertTrue(this.candidate.addSubscriberFor(K1, S1));
        assertFalse(this.candidate.addSubscriberFor(K1, S1));
        assertEquals(1, this.candidate.subscribersPerKey.size());
        assertNotNull(this.candidate.getSubscribersFor(K1));
        assertEquals(1, this.candidate.getSubscribersFor(K1).length);
        assertTrue(ArrayUtils.containsInstance(this.candidate.getSubscribersFor(K1), (S1)));

        // add K1->S1_AND2
        assertTrue(this.candidate.addSubscriberFor(K1, S1_AND_2));
        assertNotNull(this.candidate.getSubscribersFor(K1));
        assertEquals(2, this.candidate.getSubscribersFor(K1).length);
        assertTrue(ArrayUtils.containsInstance(this.candidate.getSubscribersFor(K1), (S1)));
        assertTrue(ArrayUtils.containsInstance(this.candidate.getSubscribersFor(K1), (S1_AND_2)));

        // add K2->S2
        assertTrue(this.candidate.addSubscriberFor(K2, S2));
        assertFalse(this.candidate.addSubscriberFor(K2, S2));
        assertEquals(2, this.candidate.subscribersPerKey.size());
        assertNotNull(this.candidate.getSubscribersFor(K2));
        assertEquals(1, this.candidate.getSubscribersFor(K2).length);
        assertTrue(ArrayUtils.containsInstance(this.candidate.getSubscribersFor(K2), (S2)));
     
        // add K2->S1_AND_2
        assertTrue(this.candidate.addSubscriberFor(K2, S1_AND_2));
        assertNotNull(this.candidate.getSubscribersFor(K2));
        assertEquals(2, this.candidate.getSubscribersFor(K2).length);
        assertTrue(ArrayUtils.containsInstance(this.candidate.getSubscribersFor(K2), (S2)));
        assertTrue(ArrayUtils.containsInstance(this.candidate.getSubscribersFor(K2), (S1_AND_2)));
        
        // remove K2->S1_AND_2
        assertTrue(this.candidate.removeSubscriberFor(K2, S1_AND_2));
        assertEquals(2, this.candidate.subscribersPerKey.size());
        assertFalse(this.candidate.removeSubscriberFor(K2, S1_AND_2));
        assertNotNull(this.candidate.getSubscribersFor(K2));
        assertEquals(1, this.candidate.getSubscribersFor(K2).length);
        assertTrue(ArrayUtils.containsInstance(this.candidate.getSubscribersFor(K2), (S2)));

        // check K1 subscriptions
        assertNotNull(this.candidate.getSubscribersFor(K1));
        assertEquals(2, this.candidate.getSubscribersFor(K1).length);
        assertTrue(ArrayUtils.containsInstance(this.candidate.getSubscribersFor(K1), (S1)));
        assertTrue(ArrayUtils.containsInstance(this.candidate.getSubscribersFor(K1), (S1_AND_2)));

    }

}
