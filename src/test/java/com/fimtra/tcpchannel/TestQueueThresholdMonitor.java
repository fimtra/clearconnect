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
package com.fimtra.tcpchannel;

import static com.fimtra.tcpchannel.TcpChannelProperties.Values.SEND_QUEUE_THRESHOLD;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link QueueThresholdMonitor}
 * 
 * @author Ramon Servadei
 */
public class TestQueueThresholdMonitor
{
    QueueThresholdMonitor candidate;

    @Before
    public void setUp() throws Exception
    {
        candidate = new QueueThresholdMonitor(this, 500);
    }
    
    @Test
    public void testCheckQSize() throws InterruptedException
    {
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.4)));
        assertEquals(0, candidate.thresholdWarningLevel);
        
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.5)));
        assertEquals(0, candidate.thresholdWarningLevel);
        
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.6)));
        assertEquals(1, candidate.thresholdWarningLevel);
        
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.4)));
        assertEquals(1, candidate.thresholdWarningLevel);

        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.39)));
        assertEquals(0, candidate.thresholdWarningLevel);
        
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.7)));
        assertEquals(1, candidate.thresholdWarningLevel);
        
        // check level 2->0,1
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.8)));
        assertEquals(2, candidate.thresholdWarningLevel);

        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.39)));
        assertEquals(0, candidate.thresholdWarningLevel);
        
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.8)));
        assertEquals(2, candidate.thresholdWarningLevel);

        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.59)));
        assertEquals(1, candidate.thresholdWarningLevel);

        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.9)));
        assertEquals(2, candidate.thresholdWarningLevel);

        // check level 3->0,1,2
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.95)));
        assertEquals(3, candidate.thresholdWarningLevel);

        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.39)));
        assertEquals(0, candidate.thresholdWarningLevel);
        
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.95)));
        assertEquals(3, candidate.thresholdWarningLevel);

        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.59)));
        assertEquals(1, candidate.thresholdWarningLevel);

        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.95)));
        assertEquals(3, candidate.thresholdWarningLevel);
        
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.39)));
        assertEquals(0, candidate.thresholdWarningLevel);
        
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.95)));
        assertEquals(3, candidate.thresholdWarningLevel);        
        
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.8)));
        assertEquals(3, candidate.thresholdWarningLevel);
        
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.79)));
        assertEquals(2, candidate.thresholdWarningLevel);
        
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.6)));
        assertEquals(2, candidate.thresholdWarningLevel);

        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.59)));
        assertEquals(1, candidate.thresholdWarningLevel);

        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.50)));
        assertEquals(1, candidate.thresholdWarningLevel);

        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.40)));
        assertEquals(1, candidate.thresholdWarningLevel);

        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.39)));
        assertEquals(0, candidate.thresholdWarningLevel);
    }
    
    @Test
    public void testCheckQSize_increasing() throws InterruptedException
    {
        // check increase
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 1.01)));
        assertEquals(3, candidate.thresholdWarningLevel);
        Thread.sleep(550);
        assertTrue(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 1.30)));
    }

    @Test
    public void testCheckQSize_increases_then_decreases() throws InterruptedException
    {
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 1.30)));
        Thread.sleep(550);
        // check decrease after breach
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 1.20)));
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 1.10)));
        
        Thread.sleep(550);
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 0.5)));
        assertFalse(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 1.1)));
        
        // now stationary
        Thread.sleep(550);
        assertTrue(candidate.checkQSize((int) (SEND_QUEUE_THRESHOLD * 1.1)));
    }

}
