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

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.thimble.ICoalescingRunnable;
import com.fimtra.thimble.ISequentialRunnable;
import com.fimtra.thimble.ThimbleExecutor;

/**
 * Simple performance test for the {@link ThimbleExecutor}
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings({ "boxing", "unused" })
public class ThimblePerfTest
{

    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testPerformance() throws InterruptedException
    {
        final int eventCount = 500;
        final int procCount = 5;
        System.err.println("Events, Consumers, arrayTime, thimbleTime, thimbleSeq, thimbleCoalesce, executorsTime");
        long arrtime = 0, thimbleTime = 0, thimSequentialTime = 0, thimbleCoalesceTime = 0, executorTime = 0;
        double factor = 4;
        for (int j = 1; j < procCount; j++)
        {
            arrtime = runBlockingQueue(eventCount, 1 << j, 1 << j);
            thimbleTime = runThimble(null, eventCount, 1 << j);
            thimSequentialTime = runThimble(Boolean.TRUE, eventCount, 1 << j);
            thimbleCoalesceTime = runThimble(Boolean.FALSE, eventCount, 1 << j);
            executorTime = runExecutor(eventCount, 1 << j, 1 << j);
            final String results =
                eventCount + ", " + (1 << j) + ", " + +arrtime + ", " + thimbleTime + ", " + thimSequentialTime + ", "
                    + thimbleCoalesceTime + ", " + executorTime;
            System.err.println(results);
            assertTrue("Got " + results, thimbleTime < executorTime * factor);
            assertTrue("Got " + results, thimbleCoalesceTime < executorTime);
            assertTrue("Got " + results, thimSequentialTime < eventCount * factor);
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
        threadPool.shutdown();
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
        threadPool.shutdown();
        return start;
    }

    private static void awaitLatch(final CountDownLatch queueLatch) throws InterruptedException
    {
        queueLatch.await(5, TimeUnit.SECONDS);
    }

    private static long runThimble(Boolean sequential, final int eventCount, final int procCount)
        throws InterruptedException
    {
        long start;

        ThimbleExecutor executor = new ThimbleExecutor(procCount);
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
                    executor.execute(createSequentialThimbleConsumer(queueLatch, new AtomicLong(i)));
                }
            }
            else
            {
                queueLatch = new CountDownLatch(1);
                for (int i = 0; i < eventCount; i++)
                {
                    executor.execute(createCoalescingThimbleConsumer(queueLatch, new AtomicLong(i), eventCount));
                }
            }
        }
        awaitLatch(queueLatch);
        executor.destroy();
        return (System.currentTimeMillis() - start);
    }

    private static Runnable createQueueConsumer(final BlockingQueue<AtomicLong> queue, final CountDownLatch queueLatch)
    {
        return new Runnable()
        {
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

    private static Runnable createRunnableConsumer(final CountDownLatch queueLatch, final AtomicLong atomicLong)
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                doSomething();
                queueLatch.countDown();
                // System.err.println(queueLatch.getCount() + " for sequence:" + atomicLong);
            }
        };
    }

    private static Runnable createSequentialThimbleConsumer(final CountDownLatch queueLatch, final AtomicLong atomicLong)
    {
        return new ISequentialRunnable()
        {
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

    private static Runnable createCoalescingThimbleConsumer(final CountDownLatch queueLatch,
        final AtomicLong atomicLong, final long eventCount)
    {
        final long end = eventCount - 1;
        return new ICoalescingRunnable()
        {
            @Override
            public void run()
            {
                doSomething();
                if (atomicLong.get() == end)
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

}
