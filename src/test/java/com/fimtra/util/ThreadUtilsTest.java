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
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.fimtra.util.ThreadUtils;

/**
 * Tests for {@link ThreadUtils}
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings("boxing")
public class ThreadUtilsTest
{

    @Test
    public void testGetDirectCallingClass()
    {
        assertEquals(ThreadUtilsTest.class.getCanonicalName(), ThreadUtils.getDirectCallingClass());
    }
    
    @Test
    public void testGetDirectCallingClassSimpleName()
    {
        assertEquals(ThreadUtilsTest.class.getSimpleName(), ThreadUtils.getDirectCallingClassSimpleName());
    }
    
    /**
     * Test method for
     * {@link com.fimtra.util.ThreadUtils#newDaemonThreadFactory(java.lang.String)} .
     * 
     * @throws InterruptedException
     */
    @Test
    public void testCreateDaemonThreadFactory() throws InterruptedException
    {
        ThreadFactory factory = ThreadUtils.newDaemonThreadFactory("test");
        final AtomicBoolean started = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        Runnable r = new Runnable()
        {
            @Override
            public void run()
            {
                started.set(true);
                latch.countDown();
            }
        };
        Thread thread1 = factory.newThread(r);
        Thread thread2 = factory.newThread(r);
        assertEquals("test-0", thread1.getName());
        assertEquals("test-1", thread2.getName());
        // check starting
        assertFalse(started.get());
        thread1.start();
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testGetThread()
    {
        String threadName = "test-thread";
        Thread thread = ThreadUtils.newThread(new TestRunnable(), threadName);
        assertEquals(threadName, thread.getName());
        assertEquals(false, thread.isDaemon());
    }

    @Test
	public void testGetDaemonThread() {
		String threadName = "test-daemon-thread";
		Thread thread = ThreadUtils.newDaemonThread(new TestRunnable(), threadName);
		assertEquals(threadName, thread.getName());
		assertEquals(true, thread.isDaemon());
	}

    private class TestRunnable implements Runnable
    {
        TestRunnable()
        {
        }

        @Override
        public void run()
        {
            // No op
        }
    }
}
