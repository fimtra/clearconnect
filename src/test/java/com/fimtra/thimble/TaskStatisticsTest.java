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
package com.fimtra.thimble;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.fimtra.thimble.TaskStatistics;

/**
 * Tests for {@link TaskStatistics}
 * 
 * @author Ramon Servadei
 */
public class TaskStatisticsTest
{
    @Test
    public void testStatistics()
    {
        TaskStatistics candidate = new TaskStatistics("JUnitTest");
        assertEquals(0, candidate.getIntervalSubmitted());
        assertEquals(0, candidate.getIntervalExecuted());
        assertEquals(0, candidate.getTotalSubmitted());
        assertEquals(0, candidate.getTotalExecuted());
        
        candidate.itemSubmitted();
        candidate.itemSubmitted();
        candidate.itemExecuted();
        candidate.intervalFinished();
        
        assertEquals(2, candidate.getIntervalSubmitted());
        assertEquals(1, candidate.getIntervalExecuted());
        assertEquals(2, candidate.getTotalSubmitted());
        assertEquals(1, candidate.getTotalExecuted());
        
        candidate.itemSubmitted();
        candidate.itemSubmitted();
        candidate.itemSubmitted();
        candidate.itemExecuted();
        candidate.intervalFinished();
        
        assertEquals(3, candidate.getIntervalSubmitted());
        assertEquals(1, candidate.getIntervalExecuted());
        assertEquals(5, candidate.getTotalSubmitted());
        assertEquals(2, candidate.getTotalExecuted());
        
        candidate.itemSubmitted();
        candidate.itemSubmitted();
        candidate.itemExecuted();
        candidate.itemExecuted();
        candidate.intervalFinished();
        
        assertEquals(2, candidate.getIntervalSubmitted());
        assertEquals(2, candidate.getIntervalExecuted());
        assertEquals(7, candidate.getTotalSubmitted());
        assertEquals(4, candidate.getTotalExecuted());
    }

}
