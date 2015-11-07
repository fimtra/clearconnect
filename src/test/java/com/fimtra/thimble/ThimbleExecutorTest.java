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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fimtra.thimble.ICoalescingRunnable;
import com.fimtra.thimble.ISequentialRunnable;
import com.fimtra.thimble.ThimbleExecutor;

/**
 * Tests for the {@link ThimbleExecutor}
 * 
 * @author Ramon Servadei
 */
public class ThimbleExecutorTest
{

    private static final int SIZE = Runtime.getRuntime().availableProcessors();

    static class CoalescingTestingRunnable implements ICoalescingRunnable
    {
        private final AtomicInteger runCount;
        private final AtomicInteger current;
        private final int myCount;
        private final int maxCount;
        private final CountDownLatch latch;

        CoalescingTestingRunnable(AtomicInteger current, int myCount, int maxCount, CountDownLatch latch,
            AtomicInteger runCount)
        {
            this.current = current;
            this.myCount = myCount;
            this.maxCount = maxCount;
            this.latch = latch;
            this.runCount = runCount;
        }

        @Override
        public void run()
        {
            this.runCount.incrementAndGet();
            if (this.current.get() > this.myCount)
            {
                throw new RuntimeException("Expected " + this.myCount + " should be greater than " + this.current);
            }
            this.current.set(this.myCount);
            if (this.maxCount == this.myCount)
            {
                // finished
                this.latch.countDown();
            }
        }

        @Override
        public Object context()
        {
            return "CoalescigngTestingRunnable";
        }

        @Override
        public String toString()
        {
            return "CoalescigngTestingRunnable [myCount=" + this.myCount + "]";
        }
    }

    static class SequentialTestingRunnable implements ISequentialRunnable
    {
        private final AtomicInteger counter;
        private final int expectedCount;
        private final CountDownLatch latch;
        private final String context;

        SequentialTestingRunnable(AtomicInteger counter, int expectedCount, CountDownLatch latch)
        {
            this(counter, expectedCount, latch, "SequentialTestingRunnable");
        }

        SequentialTestingRunnable(AtomicInteger counter, int expectedCount, CountDownLatch latch, String context)
        {
            this.counter = counter;
            this.expectedCount = expectedCount;
            this.latch = latch;
            this.context = context;
        }

        @Override
        public void run()
        {
            int current = this.counter.getAndIncrement();
            if (current == this.expectedCount)
            {
                this.latch.countDown();
            }
            else
            {
                throw new RuntimeException("Expected " + this.expectedCount + ", but was " + current);
            }
        }

        @Override
        public Object context()
        {
            return this.context;
        }

        @Override
        public String toString()
        {
            return "SequenceTestingRunnable [counter=" + this.counter + ", expectedCount=" + this.expectedCount
                + ", latch=" + this.latch + "]";
        }
    }

    ThimbleExecutor candidate;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        this.candidate = new ThimbleExecutor(SIZE);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testExecuteSequential() throws InterruptedException
    {
        final int count = 50000;
        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(count);

        for (int i = 0; i < count; i++)
        {
            this.candidate.execute(new SequentialTestingRunnable(counter, i, latch));
        }
        assertTrue("Not all sequential runnables were run, remaining is " + latch.getCount(),
            latch.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void testExecuteCoalescing() throws InterruptedException
    {
        final int count = 50000;
        final AtomicInteger runCount = new AtomicInteger();
        final AtomicInteger current = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);
        int maxCount = count - 1;

        for (int i = 0; i < count; i++)
        {
            this.candidate.execute(new CoalescingTestingRunnable(current, i, maxCount, latch, runCount));
        }
        assertTrue("Not all coalescing runnables were run", latch.await(2, TimeUnit.SECONDS));
        assertTrue(runCount.get() < count / 2);
    }

    @Test
    public void testExecuteSequentialWith2Contexts() throws InterruptedException
    {
        final int count = 50000;
        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(count);
        final AtomicInteger counter2 = new AtomicInteger();
        final CountDownLatch latch2 = new CountDownLatch(count);

        for (int i = 0; i < count; i++)
        {
            this.candidate.execute(new SequentialTestingRunnable(counter, i, latch));
            this.candidate.execute(new SequentialTestingRunnable(counter2, i, latch2, "SequentialTestingRunnable2"));
        }
        assertTrue("Not all sequential runnables were run, remaining is " + latch.getCount(),
            latch.await(2, TimeUnit.SECONDS));
        assertTrue("Not all sequential runnables were run (2), remaining is " + latch2.getCount(),
            latch.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void testExecuteCoalescingAndSequential() throws InterruptedException
    {
        final int count = 50000;
        final AtomicInteger runCount = new AtomicInteger();
        final AtomicInteger sequentialCounter = new AtomicInteger();
        final CountDownLatch sequentialLatch = new CountDownLatch(count);
        final AtomicInteger coalescingCounter = new AtomicInteger();
        final CountDownLatch coalesingLatch = new CountDownLatch(1);
        int maxCount = count - 1;

        for (int i = 0; i < count; i++)
        {
            this.candidate.execute(new SequentialTestingRunnable(sequentialCounter, i, sequentialLatch));
            this.candidate.execute(new CoalescingTestingRunnable(coalescingCounter, i, maxCount, coalesingLatch,
                runCount));
            for (int j = 0; j < SIZE; j++)
            {
                this.candidate.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                    }
                });
            }
            if ((i % 1000) == 0)
            {
                System.err.println(this.candidate.getCoalescingTaskStatistics());
                System.err.println(this.candidate.getSequentialTaskStatistics());
            }
        }
        assertTrue("Not all sequential runnables were run, remaining is " + sequentialLatch.getCount(),
            sequentialLatch.await(2, TimeUnit.SECONDS));
        assertTrue("Not all coalescing runnables were run", coalesingLatch.await(2, TimeUnit.SECONDS));
        assertTrue(runCount.toString(), runCount.get() < count - 5);
        System.err.println(this.candidate.getCoalescingTaskStatistics());
        System.err.println(this.candidate.getSequentialTaskStatistics());
        System.err.println(this.candidate.getExecutorStatistics());
    }

    @Test
    public void testExecuteSequentialWithStandardRunnables() throws InterruptedException
    {
        final int count = 50000;
        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(count);

        for (int i = 0; i < count; i++)
        {
            this.candidate.execute(new SequentialTestingRunnable(counter, i, latch));
            for (int j = 0; j < SIZE; j++)
            {
                this.candidate.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                    }
                });
            }
        }
        assertTrue("Not all sequential runnables were run, remaining is " + latch.getCount(),
            latch.await(2, TimeUnit.SECONDS));
    }

}
