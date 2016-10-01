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

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.fimtra.util.DeadlockDetector.DeadlockObserver;
import com.fimtra.util.DeadlockDetector.ThreadInfoWrapper;

/**
 * Tests for the {@link DeadlockDetector#newDeadlockDetectorThread} method. We need a separate test
 * because there are deadlocked threads left over from the {@link DeadlockDetectorTest}!
 * 
 * @author Ramon Servadei
 */
public class DeadlockDetectorThreadTest
{
    @Test
    public void testStartAndStopNewDeadlockDetectorThread() throws InterruptedException
    {
        if(DeadlockDetectorTest.skipDeadlockTest())
        {
            return;
        }
        
        DeadlockObserver observer = Mockito.mock(DeadlockObserver.class);
        final Future<?> flag = DeadlockDetector.newDeadlockDetectorTask(50, observer, false);
        Thread.sleep(100);
        flag.cancel(false);
        Thread.sleep(200);

        DeadlockDetectorTest.createDeadlock();

        Mockito.verifyNoMoreInteractions(observer);
    }

    @Test
    public void testDeadlockDetectorThread() throws InterruptedException
    {
        if(DeadlockDetectorTest.skipDeadlockTest())
        {
            return;
        }
        
        DeadlockObserver observer = Mockito.mock(DeadlockObserver.class);
        DeadlockDetector.newDeadlockDetectorTask(50, observer, false);
        Thread.sleep(100);
        Mockito.verify(observer, Mockito.atLeastOnce()).onDeadlockFound(Matchers.any(ThreadInfoWrapper[].class));
    }
}
