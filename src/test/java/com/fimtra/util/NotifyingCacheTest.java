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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.util.NotifyingCache;

/**
 * Tests for the {@link NotifyingCache}
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings({ "serial", "boxing" })
public class NotifyingCacheTest
{
    NotifyingCache<List<String>, String> candidate;

    @Before
    public void setUp()
    {
        createAsynchronousCandidate();
    }

    void createAsynchronousCandidate()
    {
        this.candidate = new NotifyingCache<List<String>, String>(Executors.newSingleThreadExecutor())
        {
            @Override
            protected void notifyListenerDataRemoved(List<String> listener, String key, String data)
            {
                listener.remove(data);
            }

            @Override
            protected void notifyListenerDataAdded(List<String> listener, String key, String data)
            {
                listener.add(data);
            }
        };
    }

    void createSynchronousCandidate()
    {
        this.candidate = new NotifyingCache<List<String>, String>()
        {
            @Override
            protected void notifyListenerDataRemoved(List<String> listener, String key, String data)
            {
                listener.remove(data);
            }

            @Override
            protected void notifyListenerDataAdded(List<String> listener, String key, String data)
            {
                listener.add(data);
            }
        };
    }

    @Test
    public void testNoMultipleNotificationOnJoinSynchronousCache() throws InterruptedException
    {
        createSynchronousCandidate();
        testNoMultipleNotificationOnJoin();
    }

    @Test
    public void testNoMultipleNotificationOnJoin() throws InterruptedException
    {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch latch = new CountDownLatch(2);
        final List<List<String>> listeners = new ArrayList<List<String>>();
        final int COUNT = 100;
        for (int i = 0; i < COUNT; i++)
        {
            listeners.add(new ArrayList<String>(COUNT)
            {

                @Override
                public boolean equals(Object o)
                {
                    return o == this;
                }

                @Override
                public int hashCode()
                {
                    return System.identityHashCode(this);
                }
            });
        }

        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    barrier.await();
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                for (int i = 0; i < COUNT; i++)
                {
                    NotifyingCacheTest.this.candidate.addListener(listeners.get(i));
                }
                latch.countDown();
            }
        }).start();

        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    barrier.await();
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }

                final CountDownLatch finished = new CountDownLatch(COUNT);
                final ArrayList<String> listener = new ArrayList<String>(COUNT)
                {
                    @Override
                    public boolean equals(Object o)
                    {
                        return o == this;
                    }

                    @Override
                    public int hashCode()
                    {
                        return System.identityHashCode(this);
                    }

                    @Override
                    public boolean add(String e)
                    {
                        final boolean add = super.add(e);
                        finished.countDown();
                        return add;
                    }
                };
                NotifyingCacheTest.this.candidate.addListener(listener);
                for (int i = 0; i < COUNT; i++)
                {
                    NotifyingCacheTest.this.candidate.notifyListenersDataAdded("data " + i, "data " + i);
                }
                try
                {
                    finished.await(10, TimeUnit.SECONDS);
                }
                catch (Exception e)
                {
                }
                latch.countDown();
            }
        }).start();

        assertTrue("Got: " + latch.getCount(), latch.await(5, TimeUnit.SECONDS));

        Thread.sleep(100);

        Map<Integer, Integer> fails = new HashMap<Integer, Integer>();
        for (int i = 0; i < COUNT; i++)
        {
            final int size = listeners.get(i).size();
            if (size != COUNT)
            {
                fails.put(i, size);
            }
        }
        if (fails.size() > 0)
        {
            fail("Each listener should only get " + COUNT + " updates, got failures (listener number=update count) "
                + fails);
        }
    }

    @Test
    public void testgetCacheSnapshot()
    {
        assertTrue(this.candidate.notifyListenersDataAdded("1", "1"));
        assertTrue(this.candidate.notifyListenersDataAdded("2", "2"));
        assertTrue(this.candidate.notifyListenersDataRemoved("1", "1"));
        assertEquals(this.candidate.cache, this.candidate.getCacheSnapshot());
    }

    @Test
    public void testAddDuplicatesData()
    {
        assertFalse(this.candidate.notifyListenersDataAdded(null, null));
        assertFalse(this.candidate.notifyListenersDataRemoved(null, null));
        assertFalse(this.candidate.notifyListenersDataAdded("null", null));
        assertFalse(this.candidate.notifyListenersDataRemoved("null", null));
        
        assertTrue(this.candidate.notifyListenersDataAdded(null, "1"));
        assertFalse(this.candidate.notifyListenersDataAdded(null, "1"));
        assertTrue(this.candidate.notifyListenersDataRemoved(null, "1"));
        assertFalse(this.candidate.notifyListenersDataRemoved(null, "1"));
        

        assertTrue(this.candidate.notifyListenersDataAdded("1", "1"));
        assertFalse(this.candidate.notifyListenersDataAdded("1", "1"));
        assertTrue(this.candidate.notifyListenersDataRemoved("1", "1"));
        assertFalse(this.candidate.notifyListenersDataRemoved("1", "1"));
        
        assertTrue(this.candidate.notifyListenersDataAdded("1", "1"));
        assertTrue(this.candidate.notifyListenersDataAdded("1", null));
        assertFalse(this.candidate.notifyListenersDataAdded("1", null));
        assertFalse(this.candidate.notifyListenersDataRemoved("1", null));
    }

    @Test
    public void testListenerSynchronousCache() throws InterruptedException
    {
        createSynchronousCandidate();
        testListenerCache();
    }

    @Test
    public void testListenerCache() throws InterruptedException
    {
        final AtomicReference<CountDownLatch> removed1 = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> added1 = new AtomicReference<CountDownLatch>(new CountDownLatch(2));
        List<String> listener1 = new ArrayList<String>()
        {
            @Override
            public boolean equals(Object o)
            {
                return o == this;
            }

            @Override
            public int hashCode()
            {
                return System.identityHashCode(this);
            }

            @Override
            public boolean remove(Object o)
            {
                final boolean remove = super.remove(o);
                removed1.get().countDown();
                return remove;
            }

            @Override
            public boolean add(String e)
            {
                final boolean add = super.add(e);
                added1.get().countDown();
                return add;
            }
        };
        assertTrue(this.candidate.addListener(listener1));
        assertTrue(this.candidate.notifyListenersDataAdded("1", "1"));
        assertTrue(this.candidate.notifyListenersDataAdded("2", "2"));
        final AtomicReference<CountDownLatch> removed2 = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> added2 = new AtomicReference<CountDownLatch>(new CountDownLatch(2));
        List<String> listener2 = new ArrayList<String>()
        {

            @Override
            public boolean equals(Object o)
            {
                return o == this;
            }

            @Override
            public int hashCode()
            {
                return System.identityHashCode(this);
            }

            @Override
            public boolean remove(Object o)
            {
                final boolean remove = super.remove(o);
                removed2.get().countDown();
                return remove;
            }

            @Override
            public boolean add(String e)
            {
                final boolean add = super.add(e);
                added2.get().countDown();
                return add;
            }

        };
        ArrayList<String> currentData = new ArrayList<String>();
        currentData.add("1");
        currentData.add("2");
        assertTrue(this.candidate.addListener(listener2));
        assertFalse(this.candidate.addListener(listener1));
        assertTrue(added1.get().await(1, TimeUnit.SECONDS));
        assertTrue(added2.get().await(1, TimeUnit.SECONDS));

        assertTrue("Got: " + listener1, listener1.contains("1"));
        assertTrue("Got: " + listener1, listener1.contains("2"));
        assertTrue("Got: " + listener2, listener2.contains("1"));
        assertTrue("Got: " + listener2, listener2.contains("2"));

        this.candidate.notifyListenersDataRemoved("2", "2");
        assertTrue(removed1.get().await(1, TimeUnit.SECONDS));
        assertTrue(removed2.get().await(1, TimeUnit.SECONDS));

        assertTrue("Got: " + listener1, listener1.contains("1"));
        assertFalse("Got: " + listener1, listener1.contains("2"));
        assertTrue("Got: " + listener2, listener2.contains("1"));
        assertFalse("Got: " + listener2, listener2.contains("2"));

        assertTrue(this.candidate.removeListener(listener2));

        removed1.set(new CountDownLatch(1));
        this.candidate.notifyListenersDataRemoved("1", "1");
        assertTrue(removed1.get().await(1, TimeUnit.SECONDS));

        assertEquals("Got: " + listener1, 0, listener1.size());
        assertTrue("Got: " + listener2, listener2.contains("1"));
        assertFalse("Got: " + listener2, listener2.contains("2"));
    }

    @Test
    public void testNullListener()
    {
        assertFalse(this.candidate.addListener(null));
    }

    @Test
    public void testListenerNotificationOrder()
    {
        final List<Object> notifiedAdded = new ArrayList<Object>();
        final List<Object> notifiedRemoved = new ArrayList<Object>();
        Object o1 = new Object();
        Object o2 = new Object();
        NotifyingCache<Object, String> candidate = new NotifyingCache<Object, String>()
        {
            @Override
            protected void notifyListenerDataRemoved(Object listener, String key, String data)
            {
                notifiedRemoved.add(listener);
            }

            @Override
            protected void notifyListenerDataAdded(Object listener, String key, String data)
            {
                notifiedAdded.add(listener);
            }
        };
        final List<Object> expectedNotifiedAdded = new ArrayList<Object>();
        final List<Object> expectedNotifiedRemoved = new ArrayList<Object>();
        expectedNotifiedAdded.add(o1);
        expectedNotifiedAdded.add(o2);
        expectedNotifiedRemoved.add(o1);
        expectedNotifiedRemoved.add(o2);
        assertTrue(candidate.addListener(o1));
        assertTrue(candidate.addListener(o2));

        assertTrue(candidate.notifyListenersDataAdded("1", "1"));
        assertTrue(candidate.notifyListenersDataRemoved("1", "1"));

        assertEquals(expectedNotifiedAdded, notifiedAdded);
        assertEquals(expectedNotifiedRemoved, notifiedRemoved);
    }
}
