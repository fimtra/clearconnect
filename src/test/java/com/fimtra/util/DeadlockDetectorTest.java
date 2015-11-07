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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.fimtra.util.DeadlockDetector.ThreadInfoWrapper;

/**
 * Tests for the {@link DeadlockDetector}
 * 
 * @author Ramon Servadei
 */
public class DeadlockDetectorTest
{
    public static class DeadlockCreator implements Runnable
    {
        static final int TIME_WINDOW = 500;
        final Object lock1, lock2;
        final CyclicBarrier barrier;

        public DeadlockCreator(Object lock1, Object lock2, CyclicBarrier barrier)
        {
            super();
            this.lock1 = lock1;
            this.lock2 = lock2;
            this.barrier = barrier;
        }

        @Override
        public void run()
        {
            synchronized (this)
            {
                m1();
            }
        }

        private void m1()
        {
            m2();
        }

        private void m2()
        {
            m3();
        }

        private void m3()
        {
            m4();
        }

        private void m4()
        {
            m5();
        }

        private void m5()
        {
            m6();
        }

        private void m6()
        {
            m7();
        }

        private void m7()
        {
            m8();
        }

        private void m8()
        {
            m9();
        }

        private void m9()
        {
            deadlock();
        }

        private void deadlock()
        {
            synchronized (this.lock1)
            {
                try
                {
                    this.barrier.await(10, TimeUnit.SECONDS);
                    synchronized (this.lock2)
                    {
                        this.lock2.wait();
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Test method for {@link thimble.DeadlockDetector#findDeadlocks()}.
     * 
     * @throws InterruptedException
     */
    @SuppressWarnings("null")
    @Test
    public void testFindDeadlocks() throws InterruptedException
    {
        if(skipDeadlockTest())
        {
            return;
        }
        
        DeadlockDetector candidate = new DeadlockDetector();
        ThreadInfoWrapper[] deadlockedThreads = candidate.findDeadlocks();
        assertNull(deadlockedThreads);

        createDeadlock();
        deadlockedThreads = candidate.findDeadlocks();
        assertTrue(deadlockedThreads != null);
        assertEquals(2, deadlockedThreads.length);
        assertTrue(deadlockedThreads[0].getStackTrace().length > 8);
        System.out.println(Arrays.deepToString(deadlockedThreads));

    }

    static boolean skipDeadlockTest()
    {
        if ("true".equals(System.getProperty("runDeadlockTests")))
        {
            return false;
        }
        System.err.println("SKIPPING THREAD DEADLOCK TEST");
        return true;
    }

    static Thread createDeadlock() throws InterruptedException
    {
        final String lock1 = "lock1";
        final String lock2 = "lock2";
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread t2 = new Thread(new DeadlockCreator(lock1, lock2, barrier));
        Thread t3 = new Thread(new DeadlockCreator(lock2, lock1, barrier));
        t2.start();
        t3.start();
        Thread.sleep(DeadlockCreator.TIME_WINDOW * 2);
        return t2;
    }
}
