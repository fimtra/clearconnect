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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fimtra.executors.ICoalescingRunnable;
import com.fimtra.executors.IContextExecutor;
import com.fimtra.executors.ISequentialRunnable;
import org.junit.Before;
import org.junit.Test;

/**
 * Simple performance test for the {@link GatlingExecutor}
 *
 * @author Ramon Servadei
 */
@SuppressWarnings({ "boxing", "unused" })
public class GatlingPerfTest {

    @Before
    public void setUp() throws Exception
    {
    }

    static long getGatlingTime(int LOOP_MAX, int contextCount) throws InterruptedException
    {
        CountDownLatch gatlingLatch = new CountDownLatch(1);
        AtomicLong[] gatlingCounters = new AtomicLong[contextCount];
        ISequentialRunnable[] gatlingRunnables = new ISequentialRunnable[contextCount];
        IContextExecutor gatling = new GatlingExecutor("test-vs-exec", 1);

        // create gatling components
        for (int i = 0; i < contextCount; i++)
        {
            int finalI = i;
            gatlingCounters[i] = new AtomicLong(0);
            gatlingRunnables[i] = new ISequentialRunnable() {
                @Override
                public void run()
                {
                    // simulate some work
                    AtomicLong someWork = new AtomicLong();
                    for (int j = 0; j < 1000; j++)
                    {
                        someWork.incrementAndGet();
                    }
                    if (gatlingCounters[finalI].incrementAndGet() == LOOP_MAX)
                    {
                        gatlingLatch.countDown();
                    }
                }

                @Override
                public Object context()
                {
                    return finalI;
                }
            };
        }

        long gatlingTime = System.nanoTime();
        for (int j = 0; j < LOOP_MAX; j++)
        {
            for (int i = 0; i < contextCount; i++)
            {
                gatling.execute(gatlingRunnables[i]);
            }
        }
        System.err.println("Gatling loop done, waiting...");
        gatlingLatch.await();
        gatlingTime = System.nanoTime() - gatlingTime;
        System.err.println("Gatling finished");
        gatling.destroy();
        return gatlingTime;
    }

    static long getExecutorTime(int LOOP_MAX, int contextCount) throws InterruptedException
    {
        CountDownLatch executorLatch = new CountDownLatch(1);
        AtomicLong[] executorCounters = new AtomicLong[contextCount];
        ExecutorService[] executors = new ExecutorService[contextCount];
        Runnable[] executorRunnables = new Runnable[contextCount];

        // create components
        for (int i = 0; i < contextCount; i++)
        {
            executorCounters[i] = new AtomicLong(0);
            executors[i] = Executors.newSingleThreadExecutor();
            int finalI = i;
            executorRunnables[i] = () -> {
                // simulate some work
                AtomicLong someWork = new AtomicLong();
                for (int j = 0; j < 1000; j++)
                {
                    someWork.incrementAndGet();
                }
                if (executorCounters[finalI].incrementAndGet() == LOOP_MAX)
                {
                    executorLatch.countDown();
                }
            };
        }

        // run
        long executorTime = System.nanoTime();
        for (int j = 0; j < LOOP_MAX; j++)
        {
            for (int i = 0; i < contextCount; i++)
            {
                executors[i].execute(executorRunnables[i]);
            }
        }
        System.err.println("executors loop done, waiting...");
        executorLatch.await();
        executorTime = System.nanoTime() - executorTime;
        System.err.println("executors finished");
        for (int i = 0; i < contextCount; i++)
        {
            executors[i].shutdownNow();
        }
        return executorTime;
    }

    @Test
    public void testPerformance() throws InterruptedException
    {
        final int eventCount = 500;
        final int procCount = 5;
        long arrtime = 0, gatlingTime = 0, gatlingSequentialTime = 0, gatlingCoalesceTime = 0, executorTime =
                0;
        double factor = 4;
        for (int j = 1; j < procCount; j++)
        {
            arrtime = runBlockingQueue(eventCount, 1 << j, 1 << j);
            gatlingTime = runGatling(null, eventCount, 1 << j);
            gatlingSequentialTime = runGatling(Boolean.TRUE, eventCount, 1 << j);
            gatlingCoalesceTime = runGatling(Boolean.FALSE, eventCount, 1 << j);
            executorTime = runExecutor(eventCount, 1 << j, 1 << j);
            final String results =
                    "events=" + eventCount + ", threads=" + (1 << j) + ", arrT=" + +arrtime + ", gatT="
                            + gatlingTime + ", seqT=" + gatlingSequentialTime + ", coaT="
                            + gatlingCoalesceTime + ", exeT=" + executorTime;
            System.err.println(results);
            assertTrue("Got " + results, gatlingTime < executorTime * factor);
            assertTrue("Got " + results, gatlingCoalesceTime < executorTime);
            assertTrue("Got " + results, gatlingSequentialTime < eventCount * factor);
        }
    }

    static void doSomething()
    {
        try
        {
            Thread.sleep(1);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private static long runBlockingQueue(final int eventCount, final int size, final int procCount)
            throws InterruptedException
    {
        long start;
        // final BlockingQueue<AtomicLong> queue = new ArrayBlockingQueue<AtomicLong>(1000);
        final BlockingDeque<AtomicLong> queue = new LinkedBlockingDeque<AtomicLong>();
        final CountDownLatch queueLatch = new CountDownLatch(eventCount);
        ExecutorService threadPool = Executors.newCachedThreadPool();
        for (int i = 0; i < procCount; i++)
        {
            threadPool.execute(createQueueConsumer(queue, queueLatch));
        }
        start = System.currentTimeMillis();
        for (int i = 0; i < eventCount; i++)
        {
            queue.put(new AtomicLong(i));
        }
        awaitLatch(queueLatch);
        start = System.currentTimeMillis() - start;
        threadPool.shutdownNow();
        return start;
    }

    private static long runExecutor(final int eventCount, final int size, final int procCount)
            throws InterruptedException
    {
        long start;
        final CountDownLatch queueLatch = new CountDownLatch(eventCount);
        ExecutorService threadPool = Executors.newFixedThreadPool(procCount);
        start = System.currentTimeMillis();
        for (int i = 0; i < eventCount; i++)
        {
            threadPool.execute(createRunnableConsumer(queueLatch, new AtomicLong(i)));
        }
        awaitLatch(queueLatch);
        start = System.currentTimeMillis() - start;
        threadPool.shutdownNow();
        return start;
    }

    private static void awaitLatch(final CountDownLatch queueLatch) throws InterruptedException
    {
        queueLatch.await(5, TimeUnit.SECONDS);
    }

    private static long runGatling(Boolean sequential, final int eventCount, final int procCount)
            throws InterruptedException
    {
        long start;
        GatlingExecutor executor = new GatlingExecutor("Gatling-testPerformance", procCount);
        final CountDownLatch queueLatch;
        start = System.currentTimeMillis();
        if (sequential == null)
        {
            queueLatch = new CountDownLatch(eventCount);
            for (int i = 0; i < eventCount; i++)
            {
                executor.execute(createRunnableConsumer(queueLatch, new AtomicLong(i)));
            }
        }
        else
        {
            if (sequential)
            {
                queueLatch = new CountDownLatch(eventCount);
                for (int i = 0; i < eventCount; i++)
                {
                    executor.execute(createSequentialGatlingConsumer(queueLatch, new AtomicLong(i)));
                }
            }
            else
            {
                queueLatch = new CountDownLatch(1);
                for (int i = 0; i < eventCount; i++)
                {
                    executor.execute(
                            createCoalescingGatlingConsumer(queueLatch, new AtomicLong(i), eventCount));
                }
            }
        }
        awaitLatch(queueLatch);
        final long time = System.currentTimeMillis() - start;
        // don't take destroy time into account for timings!
        executor.destroy();
        return time;
    }

    private static Runnable createQueueConsumer(final BlockingQueue<AtomicLong> queue,
            final CountDownLatch queueLatch)
    {
        return new Runnable() {
            @Override
            public void run()
            {
                while (true)
                {
                    try
                    {
                        queue.take();
                        doSomething();
                        queueLatch.countDown();
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        };
    }

    private static Runnable createRunnableConsumer(final CountDownLatch queueLatch,
            final AtomicLong atomicLong)
    {
        return new Runnable() {
            @Override
            public void run()
            {
                doSomething();
                queueLatch.countDown();
                // System.err.println(queueLatch.getCount() + " for sequence:" + atomicLong);
            }
        };
    }

    private static Runnable createSequentialGatlingConsumer(final CountDownLatch queueLatch,
            final AtomicLong atomicLong)
    {
        return new ISequentialRunnable() {
            @Override
            public void run()
            {
                doSomething();
                queueLatch.countDown();
                // System.err.println(queueLatch.getCount() + " for sequence:" + atomicLong);
            }

            @Override
            public Object context()
            {
                return "lasers";
            }
        };
    }

    private static Runnable createCoalescingGatlingConsumer(final CountDownLatch queueLatch,
            final AtomicLong atomicLong, final long eventCount)
    {
        final long end = eventCount - 1;
        return new ICoalescingRunnable() {
            @Override
            public void run()
            {
                doSomething();
                if (atomicLong.get() == end)
                {
                    queueLatch.countDown();
                }
                // System.err.println(queueLatch.getCount() + " for sequence:" + atomicLong);
            }

            @Override
            public Object context()
            {
                return "lasers";
            }
        };
    }

}
