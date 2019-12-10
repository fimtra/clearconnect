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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Vector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.util.TestUtils.EventChecker;
import com.fimtra.util.TestUtils.EventCheckerWithFailureReason;
import com.fimtra.util.TestUtils.EventFailedException;

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
        final List<List<String>> listeners = new Vector<List<String>>();
        final int COUNT = 100;
        for (int i = 0; i < COUNT; i++)
        {
            listeners.add(new Vector<String>(COUNT)
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
                final Vector<String> listener = new Vector<String>(COUNT)
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

        TestUtils.waitForEvent(new EventCheckerWithFailureReason()
        {
            Map<Integer, Integer> fails = new HashMap<Integer, Integer>();

            @Override
            public Object got()
            {
                this.fails.clear();
                for (int i = 0; i < COUNT; i++)
                {
                    final int size = listeners.get(i).size();
                    if (size != COUNT)
                    {
                        this.fails.put(i, size);
                        if (size > 0)
                        {
                            final String x = "For " + i + ", first update was: " + listeners.get(i).get(0)
                                + ", last update was: " + listeners.get(i).get(size - 1);
                        }
                    }
                }
                return this.fails.size();
            }

            @Override
            public Object expect()
            {
                return 0;
            }

            @Override
            public String getFailureReason()
            {
                return "Each listener should only get " + COUNT
                    + " updates, got failures (listener number=update count) " + this.fails;
            }
        });
    }

    @Test
    public void testContainsKeyAndKeySet()
    {
        assertTrue(this.candidate.notifyListenersDataAdded("1", "1"));
        assertTrue(this.candidate.notifyListenersDataAdded("2", "2"));
        assertTrue(this.candidate.containsKey("1"));
        assertEquals("1", this.candidate.get("1"));
        assertNull(this.candidate.get("11"));
        assertFalse(this.candidate.containsKey("11"));
        Set<String> expected = new HashSet<String>();
        expected.add("2");
        expected.add("1");
        assertEquals(expected, this.candidate.keySet());
    }

    @Test
    public void testgetCacheSnapshot()
    {
        assertTrue(this.candidate.notifyListenersDataAdded("1", "1"));
        assertTrue(this.candidate.notifyListenersDataAdded("2", "2"));
        assertTrue(this.candidate.notifyListenersDataRemoved("1"));
        assertEquals(this.candidate.cache, this.candidate.getCacheSnapshot());
    }

    @Test
    public void testAddDuplicateListener()
    {
        List<String> listener = new ArrayList<>();
        assertTrue(this.candidate.addListener(listener));
        long start = System.nanoTime();
        final boolean secondAttempt = this.candidate.addListener(listener);
        long end = System.nanoTime();
        assertFalse(secondAttempt);
        assertTrue((end - start) < 1_000_000_000 );
    }

    @Test
    public void testAddDuplicatesData()
    {
        assertFalse(this.candidate.notifyListenersDataAdded(null, null));
        assertFalse(this.candidate.notifyListenersDataRemoved(null));
        assertFalse(this.candidate.notifyListenersDataAdded("null", null));
        assertFalse(this.candidate.notifyListenersDataRemoved("null"));

        assertTrue(this.candidate.notifyListenersDataAdded(null, "1"));
        assertFalse(this.candidate.notifyListenersDataAdded(null, "1"));
        assertTrue(this.candidate.notifyListenersDataRemoved(null));
        assertFalse(this.candidate.notifyListenersDataRemoved(null));

        assertTrue(this.candidate.notifyListenersDataAdded("1", "1"));
        assertFalse(this.candidate.notifyListenersDataAdded("1", "1"));
        assertTrue(this.candidate.notifyListenersDataRemoved("1"));
        assertFalse(this.candidate.notifyListenersDataRemoved("1"));

        assertTrue(this.candidate.notifyListenersDataAdded("1", "1"));
        assertTrue(this.candidate.notifyListenersDataAdded("1", null));
        assertFalse(this.candidate.notifyListenersDataAdded("1", null));
        assertFalse(this.candidate.notifyListenersDataRemoved("1"));
    }

    @Test
    public void testNotifiedOnDestroy() throws EventFailedException, InterruptedException
    {
        this.candidate.notifyListenersDataAdded("key1", "data1");
        final List<String> listener = new Vector<String>();
        this.candidate.addListener(listener);
        TestUtils.waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                return listener.size();
            }

            @Override
            public Object expect()
            {
                return Integer.valueOf(1);
            }
        });
        assertEquals(1, listener.size());

        // destroy, check we get notified (count should be 0 as its removed)
        this.candidate.destroy();
        TestUtils.waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                return listener.size();
            }

            @Override
            public Object expect()
            {
                return Integer.valueOf(0);
            }
        });
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
        List<String> listener1 = new Vector<String>()
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
        List<String> listener2 = new Vector<String>()
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
        Vector<String> currentData = new Vector<String>();
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

        this.candidate.notifyListenersDataRemoved("2");
        assertTrue(removed1.get().await(1, TimeUnit.SECONDS));
        assertTrue(removed2.get().await(1, TimeUnit.SECONDS));

        assertTrue("Got: " + listener1, listener1.contains("1"));
        assertFalse("Got: " + listener1, listener1.contains("2"));
        assertTrue("Got: " + listener2, listener2.contains("1"));
        assertFalse("Got: " + listener2, listener2.contains("2"));

        assertTrue(this.candidate.removeListener(listener2));

        removed1.set(new CountDownLatch(1));
        this.candidate.notifyListenersDataRemoved("1");
        assertTrue(removed1.get().await(1, TimeUnit.SECONDS));

        assertEquals("Got: " + listener1, 0, listener1.size());
        assertTrue("Got: " + listener2, listener2.contains("1"));
        assertFalse("Got: " + listener2, listener2.contains("2"));

        assertEquals(0, this.candidate.listenersToNotifyWithInitialImages.size());
        assertEquals(0, this.candidate.listenersBeingNotifiedWithInitialImages);
    }

    @Test
    public void testNullListener()
    {
        assertFalse(this.candidate.addListener(null));
    }

    @Test
    public void testNoDeadlock() throws InterruptedException
    {
        final NotifyingCache<Observer, String> candidate = new NotifyingCache<Observer, String>()
        {
            @Override
            protected void notifyListenerDataRemoved(Observer listener, String key, String data)
            {
                listener.update(null, data);
            }

            @Override
            protected void notifyListenerDataAdded(Observer listener, String key, String data)
            {
                listener.update(null, data);
            }
        };
        candidate.notifyListenersDataAdded("key", "data1");

        doDeadlockTest(candidate);
    }

    @Test
    public void testNoDeadlockAsync() throws InterruptedException
    {
        final NotifyingCache<Observer, String> candidate =
            new NotifyingCache<Observer, String>(ThreadUtils.newSingleThreadExecutorService("testNoDeadlockAsync"))
            {
                @Override
                protected void notifyListenerDataRemoved(Observer listener, String key, String data)
                {
                    listener.update(null, data);
                }

                @Override
                protected void notifyListenerDataAdded(Observer listener, String key, String data)
                {
                    listener.update(null, data);
                }
            };
        candidate.notifyListenersDataAdded("key", "data1");

        doDeadlockTest(candidate);
    }

    static void doDeadlockTest(final NotifyingCache<Observer, String> candidate) throws InterruptedException
    {
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);

        candidate.addListener(new Observer()
        {
            @Override
            public void update(Observable o, Object arg)
            {
                latch1.countDown();
                try
                {
                    latch2.await();
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        });

        assertTrue(latch1.await(1, TimeUnit.SECONDS));

        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                candidate.addListener(new Observer()
                {
                    @Override
                    public void update(Observable o, Object arg)
                    {
                        latch2.countDown();
                    }
                });
            }
        }).start();

        assertTrue(latch2.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testListenerNotificationOrder() throws InterruptedException
    {
        final List<Object> notifiedAdded = new Vector<Object>();
        final List<Object> notifiedRemoved = new Vector<Object>();
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
        final List<Object> expectedNotifiedAdded = new Vector<Object>();
        final List<Object> expectedNotifiedRemoved = new Vector<Object>();
        expectedNotifiedAdded.add(o1);
        expectedNotifiedAdded.add(o2);
        expectedNotifiedRemoved.add(o1);
        expectedNotifiedRemoved.add(o2);
        assertTrue(candidate.addListener(o1));
        assertTrue(candidate.addListener(o2));

        assertTrue(candidate.notifyListenersDataAdded("1", "1"));
        assertTrue(candidate.notifyListenersDataRemoved("1"));

        assertEquals(expectedNotifiedAdded, notifiedAdded);
        assertEquals(expectedNotifiedRemoved, notifiedRemoved);
    }
}
