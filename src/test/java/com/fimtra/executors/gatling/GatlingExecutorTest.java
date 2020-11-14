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
package com.fimtra.executors.gatling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.fimtra.executors.ICoalescingRunnable;
import com.fimtra.executors.ISequentialRunnable;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests for the {@link GatlingExecutor}
 *
 * @author Ramon Servadei
 */
public class GatlingExecutorTest {

    private static final int SIZE = Runtime.getRuntime().availableProcessors();
    public static final int DELAY = 250;
    public static final int WAIT_TIME = 500;

    @Rule
    public TestName name = new TestName();

    static class CoalescingTestingRunnable implements ICoalescingRunnable {
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
                throw new RuntimeException(
                        "Expected " + this.myCount + " should be greater than " + this.current);
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

    static class SequentialTestingRunnable implements ISequentialRunnable {
        private final AtomicInteger counter;
        private final int expectedCount;
        private final CountDownLatch latch;
        private final String context;

        SequentialTestingRunnable(AtomicInteger counter, int expectedCount, CountDownLatch latch)
        {
            this(counter, expectedCount, latch, "SequentialTestingRunnable");
        }

        SequentialTestingRunnable(AtomicInteger counter, int expectedCount, CountDownLatch latch,
                String context)
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
            return "SequenceTestingRunnable [counter=" + this.counter + ", expectedCount="
                    + this.expectedCount + ", latch=" + this.latch + "]";
        }
    }

    GatlingExecutor candidate;

    /**
     * @throws Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    }

    /**
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception
    {
        this.candidate = new GatlingExecutor(name.getMethodName(), SIZE);
    }

    /**
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception
    {
        this.candidate.destroy();
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
        assertTrue("got: " + runCount.get(), runCount.get() < count);
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
            this.candidate.execute(
                    new SequentialTestingRunnable(counter2, i, latch2, "SequentialTestingRunnable2"));
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
            this.candidate.execute(
                    new CoalescingTestingRunnable(coalescingCounter, i, maxCount, coalesingLatch, runCount));
            for (int j = 0; j < SIZE; j++)
            {
                this.candidate.execute(new Runnable() {
                    @Override
                    public void run()
                    {
                    }
                });
            }
        }
        assertTrue("Not all sequential runnables were run, remaining is " + sequentialLatch.getCount(),
                sequentialLatch.await(2, TimeUnit.SECONDS));
        assertTrue("Not all coalescing runnables were run", coalesingLatch.await(2, TimeUnit.SECONDS));
        assertTrue(runCount.toString(), runCount.get() < count - 5);
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
                this.candidate.execute(new Runnable() {
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

    @Test
    public void testAddRemoveThreadId()
    {
        assertTrue(Arrays.binarySearch(this.candidate.tids, 100) < 0);
        assertTrue(Arrays.binarySearch(this.candidate.tids, 1) < 0);
        assertTrue(Arrays.binarySearch(this.candidate.tids, 2) < 0);

        this.candidate.addThreadId(2);
        this.candidate.addThreadId(1);
        this.candidate.addThreadId(100);

        assertTrue(Arrays.binarySearch(this.candidate.tids, 100) > -1);
        assertTrue(Arrays.binarySearch(this.candidate.tids, 1) > -1);
        assertTrue(Arrays.binarySearch(this.candidate.tids, 2) > -1);

        this.candidate.removeThreadId(1);

        assertTrue(Arrays.binarySearch(this.candidate.tids, 100) > -1);
        assertTrue(Arrays.binarySearch(this.candidate.tids, 1) < 0);
        assertTrue(Arrays.binarySearch(this.candidate.tids, 2) > -1);

        this.candidate.removeThreadId(2);

        assertTrue(Arrays.binarySearch(this.candidate.tids, 100) > -1);
        assertTrue(Arrays.binarySearch(this.candidate.tids, 1) < 0);
        assertTrue(Arrays.binarySearch(this.candidate.tids, 2) < 0);

        this.candidate.removeThreadId(100);

        assertTrue(Arrays.binarySearch(this.candidate.tids, 100) < 0);
        assertTrue(Arrays.binarySearch(this.candidate.tids, 1) < 0);
        assertTrue(Arrays.binarySearch(this.candidate.tids, 2) < 0);
    }

    @Test
    public void testAllCoreThreadsBlocked() throws InterruptedException
    {

        CountDownLatch latch = new CountDownLatch(2);
        Runnable trigger = latch::countDown;
        for (int i = 0; i < SIZE; i++)
        {
            this.candidate.execute(() -> {
                synchronized ("")
                {
                    try
                    {
                        "".wait(10000);
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
            });
        }
        this.candidate.execute(trigger);
        this.candidate.execute(trigger);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testScheduleRunnable() throws InterruptedException, ExecutionException
    {
        final AtomicLong counter = new AtomicLong();

        ScheduledFuture<?> schedule;
        schedule = this.candidate.schedule((Runnable) counter::incrementAndGet, DELAY, TimeUnit.MILLISECONDS);
        assertTrue(schedule.cancel(false));
        Thread.sleep(WAIT_TIME);
        assertEquals(0, counter.get());
        assertFalse(schedule.cancel(false));

        schedule = this.candidate.schedule((Runnable) counter::incrementAndGet, DELAY, TimeUnit.MILLISECONDS);

        Thread.sleep(WAIT_TIME);
        assertEquals(1, counter.get());
        assertNull(schedule.get());
        assertFalse(schedule.cancel(false));
    }

    @Test
    public void testScheduleCallable() throws InterruptedException, ExecutionException
    {
        final AtomicLong counter = new AtomicLong();

        ScheduledFuture<Long> schedule;
        schedule = this.candidate.schedule(counter::incrementAndGet, DELAY, TimeUnit.MILLISECONDS);
        assertTrue(schedule.cancel(false));
        Thread.sleep(WAIT_TIME);
        assertEquals(0, counter.get());
        assertFalse(schedule.cancel(false));

        schedule = this.candidate.schedule(counter::incrementAndGet, DELAY, TimeUnit.MILLISECONDS);

        Thread.sleep(WAIT_TIME);
        assertEquals(1, counter.get());
        assertEquals(Long.valueOf(1), (Long) schedule.get());
        assertFalse(schedule.cancel(false));
        assertTrue(schedule.isDone());
    }

    @Test
    public void testSubmitRunnable() throws InterruptedException, ExecutionException
    {
        final AtomicLong counter = new AtomicLong();

        Future<?> schedule;

        schedule = this.candidate.submit((Runnable) counter::incrementAndGet);
        Thread.sleep(WAIT_TIME);
        assertEquals(1, counter.get());
        assertNull(schedule.get());
        assertFalse(schedule.cancel(false));

        schedule = this.candidate.submit(counter::incrementAndGet, 0L);
        Thread.sleep(WAIT_TIME);
        assertEquals(2, counter.get());
        assertEquals(Long.valueOf(0), (Long) schedule.get());
        assertFalse(schedule.cancel(false));
    }

    @Test
    public void testSubmitCallable() throws InterruptedException, ExecutionException
    {
        final AtomicLong counter = new AtomicLong();

        Future<Long> schedule;
        schedule = this.candidate.submit(counter::incrementAndGet);
        Thread.sleep(WAIT_TIME);
        assertEquals(1, counter.get());
        assertEquals(Long.valueOf(1), (Long) schedule.get());
        assertFalse(schedule.cancel(false));
    }

    @Test
    public void testShutdownNow()
    {
        assertTrue(this.candidate.active);
        Runnable r = () -> {
            synchronized ("")
            {
                try
                {
                    "".wait(10000);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        };
        for (int i = 0; i < 1000; i++)
        {
            this.candidate.execute(r);
        }
        final int size = this.candidate.shutdownNow().size();
        assertTrue("Got " + size, size > 0);
        assertFalse(this.candidate.active);
    }

    @Test
    public void testInvokeAll() throws ExecutionException, InterruptedException
    {
        final AtomicLong counter = new AtomicLong();

        Collection<Callable<Long>> tasks = new LinkedList<>();
        final int MAX = 1000;
        for (int i = 0; i < MAX; i++)
        {
            tasks.add(counter::getAndIncrement);
        }
        final List<Future<Long>> futures = this.candidate.invokeAll(tasks);

        long val = counter.get();
        assertEquals(MAX, val);
        assertEquals(MAX, futures.size());

        Set<Long> vals = new HashSet<>();
        for (int i = 0; i < MAX; i++)
        {
            vals.add(Long.valueOf(i));
        }

        for (Future<Long> future : futures)
        {
            assertTrue("Got: " + future.get(), vals.remove(future.get()));
        }

        assertEquals(0, vals.size());
    }

    @Test
    public void testInvokeAny() throws ExecutionException, InterruptedException
    {
        final AtomicLong counter = new AtomicLong();

        Collection<Callable<Long>> tasks = new LinkedList<>();
        for (int i = 0; i < 1000; i++)
        {
            tasks.add(counter::incrementAndGet);
        }
        final Long result = this.candidate.invokeAny(tasks);

        long val = counter.get();
        assertTrue("Got: " + val, val > 0);
        assertTrue("Got: " + result, result > 0);
    }

    @Test
    public void test_awaitTermination() throws InterruptedException
    {
        assertFalse(this.candidate.awaitTermination(1, TimeUnit.MICROSECONDS));

        final AtomicLong counter = new AtomicLong();

        Collection<Callable<Long>> tasks = new LinkedList<>();
        final int MAX = 10000;
        for (int i = 0; i < MAX; i++)
        {
            this.candidate.execute(counter::getAndIncrement);
        }
        candidate.shutdown();
        assertTrue(this.candidate.awaitTermination(1000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void test_isTerminated()
    {
        this.candidate.shutdown();
        assertTrue(this.candidate.isTerminated());
    }
}
