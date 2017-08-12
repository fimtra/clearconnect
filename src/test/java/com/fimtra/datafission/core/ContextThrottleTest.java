/*
 * Copyright (c) 2017 Ramon Servadei
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
package com.fimtra.datafission.core;

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IObserverContext.ISystemRecordNames;

/**
 * Tests for the {@link ContextThrottle}
 * 
 * @author Ramon Servadei
 */
public class ContextThrottleTest
{
    static final int LIMIT = 50;

    ContextThrottle candidate;
    AtomicInteger counter;

    @Before
    public void setUp() throws Exception
    {
        this.counter = new AtomicInteger(0);
        this.candidate = new ContextThrottle(LIMIT, this.counter);
    }

    @Test
    public void testOverLimitThrottle() throws InterruptedException
    {

        final AtomicLong time = new AtomicLong(-1);

        long start = System.currentTimeMillis();
        for (int i = 0; i < LIMIT * 4; i++)
        {
            this.candidate.eventStart("some record name", false);
        }
        time.set(System.currentTimeMillis() - start);

        assertTrue("Was: " + time.get(), time.get() > 1000);
        assertEquals(LIMIT * 4, this.candidate.eventCount.get());
        assertEquals("Got: " + this.candidate.exemptThreads, 1, this.candidate.exemptThreads.size());

        // test popping whilst pushing and the pushCount over the limit but will decrease
        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
        }
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                while (ContextThrottleTest.this.candidate.eventCount.get() > 0)
                {
                    ContextThrottleTest.this.candidate.eventFinish();
                    latch.countDown();
                }
                latch2.countDown();
            }
        }).start();
        latch.await();

        start = System.currentTimeMillis();
        this.candidate.eventStart("some record name", false);
        time.set(System.currentTimeMillis() - start);
        assertTrue("Was: " + time.get(), time.get() < 100);

        latch2.await();
        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
        }
        // should not be an exception thread anymore
        assertTrue("Got: " + this.candidate.exemptThreads, this.candidate.exemptThreads.isEmpty());
    }

    @Test
    public void testUnderLimitNoThrottle() throws InterruptedException
    {
        Runnable task = new Runnable()
        {
            @Override
            public void run()
            {
                for (int i = 0; i < LIMIT; i++)
                {
                    ContextThrottleTest.this.candidate.eventStart("some record name", false);
                }
            }
        };
        Thread t = new Thread(task);
        t.start();

        t.join(1000);
        assertEquals(LIMIT, this.candidate.eventCount.get());
    }

    @Test
    public void testSystemRecordSkipsThrottle() throws InterruptedException
    {
        Runnable task = new Runnable()
        {
            @Override
            public void run()
            {
                for (int i = 0; i < LIMIT * 2; i++)
                {
                    ContextThrottleTest.this.candidate.eventStart(ISystemRecordNames.CONTEXT_CONNECTIONS, false);
                }
            }
        };
        Thread t = new Thread(task);
        t.start();

        t.join(1000);
        assertEquals(LIMIT * 2, this.candidate.eventCount.get());
    }

    @Test
    public void testForceFlagSkipsThrottle() throws InterruptedException
    {
        Runnable task = new Runnable()
        {
            @Override
            public void run()
            {
                for (int i = 0; i < LIMIT * 2; i++)
                {
                    ContextThrottleTest.this.candidate.eventStart("some record name", true);
                }
            }
        };
        Thread t = new Thread(task);
        t.start();

        t.join(1000);
        assertEquals(LIMIT * 2, this.candidate.eventCount.get());
    }

    @Test
    public void testFrameworkThreadSkipsThrottle() throws InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(1);
        Runnable task = new Runnable()
        {
            @Override
            public void run()
            {
                for (int i = 0; i < LIMIT * 2; i++)
                {
                    ContextThrottleTest.this.candidate.eventStart("some record name", false);
                }
                latch.countDown();
            }
        };
        ContextUtils.CORE_EXECUTOR.execute(task);

        latch.await(1000, TimeUnit.MILLISECONDS);

        assertEquals(LIMIT * 2, this.candidate.eventCount.get());
    }

}
