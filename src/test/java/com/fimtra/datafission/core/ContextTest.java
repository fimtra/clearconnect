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
package com.fimtra.datafission.core;

import static com.fimtra.util.TestUtils.waitForEvent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IPermissionFilter;
import com.fimtra.datafission.IPublisherContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IValidator;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.util.TestUtils.EventChecker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author Ramon Servadei
 */
public class ContextTest {
    private final static String name = "test";
    private static final String K1 = "1";
    private static final String K2 = "2";
    private final static String K5 = "5";
    private static final IValue V1 = new DoubleValue(1);
    private static final IValue V1n = new DoubleValue(1.2);
    private static final IValue V2 = new DoubleValue(2);
    private static final IValue V2p = new DoubleValue(2.1);
    private final static IValue V5 = new DoubleValue(5);

    Context candidate;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new Context("testContext");
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testResubscribe() throws InterruptedException
    {
        final String name = "sdf1";
        final String key = "Kmy1";
        final TextValue v1 = TextValue.valueOf("value1");

        final TestCachingAtomicChangeObserver listener = new TestCachingAtomicChangeObserver();
        listener.latch = new CountDownLatch(2);
        this.candidate.addObserver(listener, name);

        createRecordWaitForUpdate(name);
        this.candidate.getRecord(name).put(key, v1);
        this.candidate.publishAtomicChange(name);
        assertTrue(listener.latch.await(1, TimeUnit.SECONDS));
        assertEquals(v1, listener.getLatestImage().get(key));

        listener.latch = new CountDownLatch(1);
        this.candidate.resubscribe(name);
        assertTrue(listener.latch.await(1, TimeUnit.SECONDS));
        assertEquals(v1, listener.getLatestImage().get(key));
    }

    @Test
    public void testAddAllEntriesRemovedToAtomicChange()
    {
        final Record instance = createRecordWaitForUpdate(name);
        instance.put(K1, V1);
        instance.put(K2, V2);
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(observer, name);
        instance.clear();
        final IRecordChange changes = instance.getPendingAtomicChange();
        assertNotNull(changes);
        assertEquals(0, changes.getPutEntries().size());
        assertEquals(0, changes.getOverwrittenEntries().size());
        assertEquals(2, changes.getRemovedEntries().size());
    }

    @Test
    public void testAddEntryRemovedToAtomicChange()
    {
        final Record instance = createRecordWaitForUpdate(name);
        instance.put(K1, V1);
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(observer, name);
        instance.remove(K1);
        final IRecordChange changes = instance.getPendingAtomicChange();
        assertNotNull(changes);
        assertEquals(0, changes.getPutEntries().size());
        assertEquals(0, changes.getOverwrittenEntries().size());
        assertEquals(1, changes.getRemovedEntries().size());
    }

    @Test
    public void testAddEntryUpdatedToAtomicChange()
    {
        final Record instance = createRecordWaitForUpdate(name);
        instance.put(K1, V1);
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(observer, name);
        instance.put(K1, V1n);
        final IRecordChange changes = instance.getPendingAtomicChange();
        assertNotNull(changes);
        assertEquals(1, changes.getPutEntries().size());
        assertEquals(1, changes.getOverwrittenEntries().size());
        assertEquals(0, changes.getRemovedEntries().size());
    }

    @Test
    public void testGetSubscribedRecords()
    {
        assertEquals(0, this.candidate.getSubscribedRecords().size());
        IRecordListener observer = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(observer, "one", "two", "three");
        assertEquals(3, this.candidate.getSubscribedRecords().size());

        // check no double-counting
        this.candidate.addObserver(observer, "one", "two", "three");
        assertEquals(3, this.candidate.getSubscribedRecords().size());

        this.candidate.removeObserver(observer, "one", "three");
        assertEquals(1, this.candidate.getSubscribedRecords().size());

        this.candidate.removeObserver(observer, "one", "three");
        assertEquals(1, this.candidate.getSubscribedRecords().size());

        this.candidate.addObserver(observer, "one", "two", "three");
        assertEquals(3, this.candidate.getSubscribedRecords().size());

        this.candidate.removeObserver(observer, "one", "two", "three");
        assertEquals(0, this.candidate.getSubscribedRecords().size());
    }

    @SuppressWarnings("boxing")
    @Test
    public void testAddObserver_permission() throws Exception
    {
        String name2 = "duff2";
        String permissionToken = "pt_sdf2";
        IPermissionFilter filter = Mockito.mock(IPermissionFilter.class);
        when(filter.accept(eq(permissionToken), eq(name))).thenReturn(true);
        when(filter.accept(eq(permissionToken), eq(name2))).thenReturn(false);

        this.candidate.setPermissionFilter(filter);

        // create the record without verifying the first update because of the permission filter
        Map<String, IValue> instance = this.candidate.createRecord(name);
        instance.put(K1, V1);
        instance.put(K2, V2);
        this.candidate.publishAtomicChange(name).await();

        instance = this.candidate.createRecord(name2);
        instance.put(K5, V2p);
        this.candidate.publishAtomicChange(name2).await();

        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();

        // add the first observer, wait for update
        observer.latch = new CountDownLatch(1);
        Map<String, Boolean> result =
                this.candidate.addObserver(permissionToken, observer, name, name2).get();
        assertEquals(2, result.size());
        assertTrue("Got: " + result, result.get(name));
        assertFalse("Got: " + result, result.get(name2));
        assertTrue(observer.latch.await(1, TimeUnit.SECONDS));
        assertEquals(this.candidate.getRecord(name), observer.first.get());

        Mockito.verify(filter).accept(eq(permissionToken), eq(name));
        Mockito.verify(filter).accept(eq(permissionToken), eq(name2));
        verifyNoMoreInteractions(filter);
    }

    @Test
    public void testAddObserverAfterCreating() throws InterruptedException
    {
        final Record instance = createRecordWaitForUpdate(name);
        assertNotNull(instance);
        instance.put(K1, V1);
        instance.put(K2, V2);
        this.candidate.publishAtomicChange(name).await();

        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        final TestCachingAtomicChangeObserver observer2 = new TestCachingAtomicChangeObserver();

        // add the first observer, wait for update
        observer.latch = new CountDownLatch(1);
        this.candidate.addObserver(observer, name);
        assertTrue(observer.latch.await(1, TimeUnit.SECONDS));

        // check image notified to observer added after creation
        Map<String, IValue> expectedImage = new HashMap<String, IValue>();
        expectedImage.put(K1, V1);
        expectedImage.put(K2, V2);
        assertEquals("changes: " + observer.changes, 1, observer.changes.size());
        assertEquals(expectedImage, observer.first.get());
        assertEquals(2, observer.changes.get(0).getPutEntries().size());
        assertEquals(0, observer.changes.get(0).getOverwrittenEntries().size());
        assertEquals(0, observer.changes.get(0).getRemovedEntries().size());

        observer.reset();
        CountDownLatch countDownLatch = new CountDownLatch(2);
        observer.latch = countDownLatch;
        observer2.latch = countDownLatch;

        // change the instance and publish the change
        instance.put(K1, V1n);
        instance.put(K5, V5);
        expectedImage.put(K1, V1n);
        expectedImage.put(K5, V5);
        this.candidate.publishAtomicChange(name).await();

        // add observer2 AFTER publishing the last change
        this.candidate.addObserver(observer2, name);

        assertTrue(countDownLatch.await(1, TimeUnit.SECONDS));

        // check both observers have the same image
        Map<String, IValue> putEntries = new HashMap<String, IValue>();
        putEntries.put(K1, V1n);
        putEntries.put(K5, V5);
        assertEquals(expectedImage, observer.first.get());
        assertEquals("changes: " + observer.changes, 1, observer.changes.size());
        assertEquals(putEntries, observer.changes.get(0).getPutEntries());
        assertEquals(1, observer.changes.get(0).getOverwrittenEntries().size());
        assertEquals(0, observer.changes.get(0).getRemovedEntries().size());

        assertEquals(expectedImage, observer2.first.get());
        assertEquals("changes: " + observer2.changes, 1, observer2.changes.size());
        assertEquals(new HashMap<String, IValue>(instance), observer2.changes.get(0).getPutEntries());
        assertEquals(0, observer2.changes.get(0).getOverwrittenEntries().size());
        assertEquals(0, observer2.changes.get(0).getRemovedEntries().size());
        assertEquals(0, this.candidate.listenersBeingNotifiedWithInitialImages);
    }

    @Test
    public void testAddObserverIdempotent() throws InterruptedException
    {
        final Record instance = createRecordWaitForUpdate(name);
        assertNotNull(instance);
        instance.put(K1, V1);
        instance.put(K2, V2);
        this.candidate.publishAtomicChange(name).await();

        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();

        // add the first observer, wait for update
        observer.latch = new CountDownLatch(1);
        this.candidate.addObserver(observer, name);
        assertTrue(observer.latch.await(1, TimeUnit.SECONDS));

        // check image notified to observer added after creation
        Map<String, IValue> expectedImage = new HashMap<String, IValue>();
        expectedImage.put(K1, V1);
        expectedImage.put(K2, V2);
        assertEquals("changes: " + observer.changes, 1, observer.changes.size());
        assertEquals(expectedImage, observer.first.get());
        assertEquals(2, observer.changes.get(0).getPutEntries().size());
        assertEquals(0, observer.changes.get(0).getOverwrittenEntries().size());
        assertEquals(0, observer.changes.get(0).getRemovedEntries().size());

        observer.reset();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        observer.latch = countDownLatch;

        // re-add the same listener - checks IDEMPOTENCY
        this.candidate.addObserver(observer, name);

        // change the instance and publish the change
        instance.put(K1, V1n);
        instance.put(K5, V5);
        expectedImage.put(K1, V1n);
        expectedImage.put(K5, V5);
        this.candidate.publishAtomicChange(name).await();

        assertTrue(countDownLatch.await(1, TimeUnit.SECONDS));

        // check both observers have the same image
        Map<String, IValue> putEntries = new HashMap<String, IValue>();
        putEntries.put(K1, V1n);
        putEntries.put(K5, V5);
        assertEquals(expectedImage, observer.first.get());
        assertEquals("changes: " + observer.changes, 1, observer.changes.size());
        assertEquals(putEntries, observer.changes.get(0).getPutEntries());
        assertEquals(1, observer.changes.get(0).getOverwrittenEntries().size());
        assertEquals(0, observer.changes.get(0).getRemovedEntries().size());

        assertEquals(0, this.candidate.listenersBeingNotifiedWithInitialImages);
    }

    @Test
    public void testAddObserverBeforeCreating() throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(2);
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver(latch);
        final TestCachingAtomicChangeObserver observer2 = new TestCachingAtomicChangeObserver(latch);
        this.candidate.addObserver(observer, name);
        this.candidate.addObserver(observer2, name);

        Map<String, IValue> expectedMap = new HashMap<String, IValue>();
        expectedMap.put(K1, V1);
        expectedMap.put(K2, V2);
        Record instance = createRecordWaitForUpdate(name, expectedMap);
        assertNotNull(instance);
        assertTrue("Did not get notified on creation", latch.await(1, TimeUnit.SECONDS));

        final int size = observer.changes.size();
        assertEquals("Got: " + observer.changes, 1, size);
        assertEquals(expectedMap, observer.first.get());
        assertEquals(2, observer.changes.get(0).getPutEntries().size());
        assertEquals(0, observer.changes.get(0).getOverwrittenEntries().size());
        assertEquals(0, observer.changes.get(0).getRemovedEntries().size());

        final int size2 = observer2.changes.size();
        assertEquals("Got: " + observer.changes, 1, size2);
        assertEquals(expectedMap, observer2.first.get());
        assertEquals(2, observer2.changes.get(0).getPutEntries().size());
        assertEquals(0, observer2.changes.get(0).getOverwrittenEntries().size());
        assertEquals(0, observer2.changes.get(0).getRemovedEntries().size());

        observer.reset();
        observer2.reset();
        latch = new CountDownLatch(2);
        observer.latch = latch;
        observer2.latch = latch;
        instance.put(K2, V2p);
        expectedMap.put(K2, V2p);

        this.candidate.publishAtomicChange(name).await();
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        assertEquals(1, size);
        assertEquals(expectedMap, observer.first.get());
        Map<String, IValue> expectedPuts = new HashMap<String, IValue>();
        expectedPuts.put(K2, V2p);
        assertEquals(expectedPuts, observer.changes.get(0).getPutEntries());
        assertEquals(1, observer.changes.get(0).getOverwrittenEntries().size());
        assertEquals(0, observer.changes.get(0).getRemovedEntries().size());

        assertEquals(1, size2);
        assertEquals(expectedMap, observer2.first.get());
        assertEquals(expectedPuts, observer2.changes.get(0).getPutEntries());
        assertEquals(1, observer2.changes.get(0).getOverwrittenEntries().size());
        assertEquals(0, observer2.changes.get(0).getRemovedEntries().size());
        assertEquals(0, this.candidate.listenersBeingNotifiedWithInitialImages);
    }

    @Test
    public void testAddObserverForSameObserver()
    {
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        final TestCachingAtomicChangeObserver observer2 = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(observer, name);
        this.candidate.addObserver(observer, name);
        this.candidate.addObserver(observer2, name);
        this.candidate.addObserver(observer2, name);
        assertEquals(2, this.candidate.recordObservers.getSubscribersFor(name).length);
        assertEquals(0, observer.changes.size());
        assertEquals(0, observer2.changes.size());
    }

    @Test
    public void testAddObserverForSameObserverAfterCreating() throws InterruptedException
    {
        final Record instance = createRecordWaitForUpdate(name);
        assertNotNull(instance);
        instance.put(K1, V1);
        instance.put(K2, V2);
        this.candidate.publishAtomicChange(name).await();

        CountDownLatch latch = new CountDownLatch(2);
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver(latch);
        final TestCachingAtomicChangeObserver observer2 = new TestCachingAtomicChangeObserver(latch);
        this.candidate.addObserver(observer, name);
        this.candidate.addObserver(observer, name);
        this.candidate.addObserver(observer2, name);
        this.candidate.addObserver(observer2, name);
        assertEquals(2, this.candidate.recordObservers.getSubscribersFor(name).length);

        assertTrue(latch.await(1, TimeUnit.SECONDS));

        assertEquals("Got: " + observer.changes, 1, observer.changes.size());
        assertEquals("Got: " + observer2.changes, 1, observer2.changes.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotCreateMapWithSameNameAsContextConnections()
    {
        createRecordWaitForUpdate(ISystemRecordNames.CONTEXT_CONNECTIONS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testContextConnectionsCannotBeRemoved()
    {
        this.candidate.removeRecord(ISystemRecordNames.CONTEXT_CONNECTIONS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testContextConnectionsIsUnmodifiable()
    {
        this.candidate.getRecord(ISystemRecordNames.CONTEXT_CONNECTIONS).clear();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotCreateMapWithSameNameAsContextRpcs()
    {
        createRecordWaitForUpdate(ISystemRecordNames.CONTEXT_RPCS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testContextRpcsCannotBeRemoved()
    {
        this.candidate.removeRecord(ISystemRecordNames.CONTEXT_RPCS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testContextRpcsIsUnmodifiable()
    {
        this.candidate.getRecord(ISystemRecordNames.CONTEXT_RPCS).clear();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotCreateMapWithSameNameAsContextRecords()
    {
        createRecordWaitForUpdate(ISystemRecordNames.CONTEXT_RECORDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testContextRecordsCannotBeRemoved()
    {
        this.candidate.removeRecord(ISystemRecordNames.CONTEXT_RECORDS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testContextRecordsIsUnmodifiable()
    {
        this.candidate.getRecord(ISystemRecordNames.CONTEXT_RECORDS).clear();
    }

    @Test
    public void testContextRecordsRemovesEntryWhenRemovingMap() throws InterruptedException
    {
        createRecordWaitForUpdate(name);
        final TestCachingAtomicChangeObserver registryObserver = new TestCachingAtomicChangeObserver(true);
        this.candidate.addObserver(registryObserver, ISystemRecordNames.CONTEXT_RECORDS);

        final Set<String> expected = new HashSet<String>(
                Arrays.asList("ContextConnections", "ContextSubscriptions", "ContextRecords", "ContextRpcs",
                        "ContextStatus", "test"));
        waitForEvent(new EventChecker() {
            @Override
            public Object got()
            {
                IRecord latestImage = registryObserver.getLatestImage();
                if (latestImage == null)
                {
                    return null;
                }
                return latestImage.keySet();
            }

            @Override
            public Object expect()
            {
                return expected;
            }
        });

        // remove the instance
        this.candidate.removeRecord(name);
        final Set<String> expected2 = new HashSet<String>(
                Arrays.asList("ContextConnections", "ContextSubscriptions", "ContextRecords", "ContextRpcs",
                        "ContextStatus"));
        waitForEvent(new EventChecker() {
            @Override
            public Object got()
            {
                IRecord latestImage = registryObserver.getLatestImage();
                if (latestImage == null)
                {
                    return null;
                }
                return latestImage.keySet();
            }

            @Override
            public Object expect()
            {
                return expected2;
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotCreateMapWithSameNameAsContextStatus()
    {
        createRecordWaitForUpdate(ISystemRecordNames.CONTEXT_STATUS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testContextStatusCannotBeRemoved()
    {
        this.candidate.removeRecord(ISystemRecordNames.CONTEXT_STATUS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testContextStatusIsUnmodifiable()
    {
        this.candidate.getRecord(ISystemRecordNames.CONTEXT_STATUS).clear();
    }

    @Test
    public void testContextStatusCreated()
    {
        assertNotNull(this.candidate.getRecord(ISystemRecordNames.CONTEXT_STATUS));
        Map<String, IValue> registry = this.candidate.getRecord(ISystemRecordNames.CONTEXT_RECORDS);
        assertEquals(0, registry.get(ISystemRecordNames.CONTEXT_STATUS).longValue());
    }

    @Test
    public void testContextSubscriptionRemovedWhenRemovingMap() throws InterruptedException
    {
        createRecordWaitForUpdate(name);

        TestCachingAtomicChangeObserver subscriptionsObserver = new TestCachingAtomicChangeObserver();
        // NOTE: when subscribing for the 'context subscriptions' we get 2 updates
        subscriptionsObserver.latch = new CountDownLatch(2);
        this.candidate.addObserver(subscriptionsObserver, ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        assertTrue(subscriptionsObserver.latch.await(1, TimeUnit.SECONDS));
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1");

        TestCachingAtomicChangeObserver observer2 = new TestCachingAtomicChangeObserver();
        subscriptionsObserver.latch = new CountDownLatch(1);
        this.candidate.addObserver(observer2, name);
        assertTrue(subscriptionsObserver.latch.await(1, TimeUnit.SECONDS));
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1", "test=L1");

        // remove the instance
        this.candidate.removeRecord(name);
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1");
    }

    static void verify(final String name_context, final TestCachingAtomicChangeObserver observer,
            final String... items) throws InterruptedException
    {
        waitForEvent(new EventChecker() {
            @Override
            public Object got()
            {
                final IRecord record = observer.getLatestImage();
                if (record == null)
                {
                    return null;
                }
                String string = record.toString();
                if (!string.startsWith(name_context))
                {
                    return Boolean.FALSE;
                }
                for (String item : items)
                {
                    if (!string.contains(item))
                    {
                        return Boolean.FALSE;
                    }
                }
                return Boolean.TRUE;
            }

            @Override
            public Object expect()
            {
                return Boolean.TRUE;
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testContextSubscriptionsCannotBeRemoved()
    {
        this.candidate.removeRecord(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testContextSubscriptionsIsUnmodifiable()
    {
        this.candidate.getRecord(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS).clear();
    }

    @Test
    public void testContextSubscriptionsListsObserversForMap() throws InterruptedException
    {
        createRecordWaitForUpdate(name);

        TestCachingAtomicChangeObserver subscriptionsObserver = new TestCachingAtomicChangeObserver();
        // NOTE: when subscribing for the 'context subscriptions' we get 2 updates
        subscriptionsObserver.latch = new CountDownLatch(2);
        this.candidate.addObserver(subscriptionsObserver, ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        assertTrue(subscriptionsObserver.latch.await(1, TimeUnit.SECONDS));
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1");

        subscriptionsObserver.latch = new CountDownLatch(1);
        subscriptionsObserver.reset();
        TestCachingAtomicChangeObserver observer2 = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(observer2, name);
        this.candidate.addObserver(observer2, name);
        assertTrue(subscriptionsObserver.latch.await(1, TimeUnit.SECONDS));
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1", "test=L1");

        // add second observer to the name2 record
        subscriptionsObserver.latch = new CountDownLatch(1);
        TestCachingAtomicChangeObserver observer3 = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(observer3, name);
        assertTrue(subscriptionsObserver.latch.await(1, TimeUnit.SECONDS));
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1", "test=L2");

        subscriptionsObserver.latch = new CountDownLatch(1);
        subscriptionsObserver.reset();
        this.candidate.removeObserver(observer2, name);
        assertTrue(subscriptionsObserver.latch.await(1, TimeUnit.SECONDS));
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1", "test=L1");

        subscriptionsObserver.latch = new CountDownLatch(1);
        subscriptionsObserver.reset();
        this.candidate.removeObserver(observer3, name);
        assertTrue(subscriptionsObserver.latch.await(1, TimeUnit.SECONDS));
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1");

        // attempt duff removes
        this.candidate.removeObserver(observer3, name);
        this.candidate.removeObserver(observer3, name);
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1");
    }

    @Test
    public void testContextSubscriptionsListsObserversForMapWhenAddingObserverBerforeCreatingMap()
            throws InterruptedException
    {
        Map<String, IValue> subscriptions =
                this.candidate.getRecord(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);

        TestCachingAtomicChangeObserver subscriptionsObserver = new TestCachingAtomicChangeObserver();
        // NOTE: when subscribing for the 'context subscriptions' we get 2 updates
        subscriptionsObserver.latch = new CountDownLatch(2);
        this.candidate.addObserver(subscriptionsObserver, ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        assertTrue(subscriptionsObserver.latch.await(1, TimeUnit.SECONDS));
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1");

        TestCachingAtomicChangeObserver observer2 = new TestCachingAtomicChangeObserver();
        subscriptionsObserver.latch = new CountDownLatch(2);
        this.candidate.addObserver(subscriptionsObserver, name);
        this.candidate.addObserver(observer2, name);
        assertTrue(subscriptionsObserver.latch.await(1, TimeUnit.SECONDS));
        assertEquals(subscriptions.toString(), 2, subscriptions.size());
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1", "test=L2");
    }

    @Test
    public void testContextSubscriptionsShowsExistingSubscriptions() throws InterruptedException
    {
        TestCachingAtomicChangeObserver observer1 = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(observer1, name);

        TestCachingAtomicChangeObserver subscriptionsObserver = new TestCachingAtomicChangeObserver();
        // NOTE: when subscribing for the 'context subscriptions' we get 2 updates
        subscriptionsObserver.latch = new CountDownLatch(2);
        this.candidate.addObserver(subscriptionsObserver, ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        assertTrue(subscriptionsObserver.latch.await(1, TimeUnit.SECONDS));

        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1", "test=L1");

        subscriptionsObserver.latch = new CountDownLatch(1);
        this.candidate.addObserver(subscriptionsObserver, name);
        assertTrue(subscriptionsObserver.latch.await(1, TimeUnit.SECONDS));
        verify("(ImmutableSnapshot)testContext|ContextSubscriptions|", subscriptionsObserver,
                "ContextSubscriptions=L1", "test=L2");
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateInstanceString()
    {
        final Map<String, IValue> createInstance = createRecordWaitForUpdate(name);
        assertNotNull(createInstance);
        // try creating a second duplicate
        createRecordWaitForUpdate(name);
    }

    @Test
    public void testCreateInstanceStringMapOfStringIValue()
    {
        final HashMap<String, IValue> record = new HashMap<String, IValue>();
        final Map<String, IValue> createInstance = createRecordWaitForUpdate(name, record);
        assertNotNull(createInstance);
        assertNotSame(record, ((Record) createInstance).data);
    }

    @Test
    public void testGetInstance()
    {
        assertNull(this.candidate.getRecord("sdf"));
        final Map<String, IValue> createInstance = createRecordWaitForUpdate(name);
        assertEquals(createInstance, this.candidate.getRecord(name));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetInstanceForContextRecords()
    {
        Map<String, IValue> registry = this.candidate.getRecord(ISystemRecordNames.CONTEXT_RECORDS);
        assertNotNull(registry);
        registry.clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetInstanceForContextStatus()
    {
        Map<String, IValue> registry = this.candidate.getRecord(ISystemRecordNames.CONTEXT_STATUS);
        assertNotNull(registry);
        registry.clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetInstanceForContextSubscriptions()
    {
        Map<String, IValue> registry = this.candidate.getRecord(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        assertNotNull(registry);
        registry.clear();
    }

    @Test
    public void testGetInstanceNames()
    {
        final String name2 = name + "1";
        createRecordWaitForUpdate(name);
        createRecordWaitForUpdate(name2);
        Set<String> expected = new HashSet<String>();
        expected.add(name);
        expected.add(name2);
        expected.add(ISystemRecordNames.CONTEXT_RECORDS);
        expected.add(ISystemRecordNames.CONTEXT_STATUS);
        expected.add(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        expected.add(ISystemRecordNames.CONTEXT_RPCS);
        expected.add(ISystemRecordNames.CONTEXT_CONNECTIONS);
        assertEquals(expected, this.candidate.getRecordNames());
    }

    @Test
    public void testInitialObserverForBlankMap() throws InterruptedException
    {
        @SuppressWarnings("unused") final Record instance = createRecordWaitForUpdate(name);

        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        observer.latch = new CountDownLatch(1);
        this.candidate.addObserver(observer, name);
        assertTrue(observer.latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testPublishAtomicChange() throws InterruptedException
    {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
        // notify a lot of observers, check updates occur sequentially
        final int instanceCount = 100;
        for (int i = 0; i < instanceCount; i++)
        {
            final Record instance = createRecordWaitForUpdate(name + i);
            executor.scheduleAtFixedRate(new Runnable() {
                long i;

                @Override
                public void run()
                {
                    instance.put(K1, LongValue.valueOf(this.i++));
                }
            }, 0, 1, TimeUnit.MILLISECONDS);
        }

        // notify ALL every 10ms
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run()
            {
                for (int i = 0; i < instanceCount; i++)
                {
                    ContextTest.this.candidate.publishAtomicChange(name + i);
                }
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

        // wait for 1000 update counts per instance
        final CountDownLatch latch = new CountDownLatch(instanceCount * 10);
        for (int i = 0; i < instanceCount; i++)
        {
            TestLongValueSequenceCheckingAtomicChangeObserver observer =
                    new TestLongValueSequenceCheckingAtomicChangeObserver();
            observer.latch = latch;
            this.candidate.addObserver(observer, name + i);
        }

        // wait for
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        executor.shutdown();
    }

    @Test
    public void testPublishAtomicChangesForNoChange() throws InterruptedException
    {
        final Record instance = createRecordWaitForUpdate(name);

        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        observer.latch = new CountDownLatch(1);
        this.candidate.addObserver(observer, name);
        assertTrue(observer.latch.await(1, TimeUnit.SECONDS));

        observer.latch = new CountDownLatch(1);
        instance.put(K1, V1);
        instance.put(K2, V2);
        this.candidate.publishAtomicChange(name).await();
        assertTrue(observer.latch.await(1, TimeUnit.SECONDS));

        observer.latch = new CountDownLatch(1);
        this.candidate.publishAtomicChange(name).await();
        assertEquals(1, observer.latch.getCount());
    }

    @Test
    public void testPublishChangesWhenNoObserversRegistered() throws InterruptedException
    {
        this.candidate.publishAtomicChange(name).await();

        final Record instance = createRecordWaitForUpdate(name);
        assertNotNull(instance);
        assertNull(instance.atomicChange);
        this.candidate.publishAtomicChange(name).await();
        assertNull(instance.atomicChange);
    }

    @Test
    public void testRemoveInstance() throws InterruptedException
    {
        assertNull(this.candidate.removeRecord("sdf"));
        final Record instance = createRecordWaitForUpdate(name);
        final TestCachingAtomicChangeObserver observer =
                new TestCachingAtomicChangeObserver(new CountDownLatch(1));
        this.candidate.addObserver(observer, name);
        assertTrue(observer.latch.await(1, TimeUnit.SECONDS));
        assertNotNull(this.candidate.imageCache.images.get(name));
        this.candidate.removeObserver(observer, name);

        // add stuff to the instance
        this.candidate.addObserver(new TestCachingAtomicChangeObserver(), name);
        this.candidate.addObserver(new TestCachingAtomicChangeObserver(), name);
        this.candidate.addObserver(new TestCachingAtomicChangeObserver(), name);
        instance.put(K1, V1);
        assertNotNull(instance.getPendingAtomicChange());
        assertEquals(3, this.candidate.recordObservers.getSubscribersFor(name).length);

        // remove and check
        assertEquals(instance, this.candidate.removeRecord(name));
        assertNull(this.candidate.records.get(name));
        assertNull(this.candidate.imageCache.images.get(name));
        // NOTE: subscribers are INDEPENDENT of record existence
        assertEquals(3, this.candidate.recordObservers.getSubscribersFor(name).length);

        // check double remove
        assertNull(this.candidate.removeRecord(name));
        assertNull(this.candidate.imageCache.images.get(name));
    }

    @Test
    public void testRemoveObserver() throws InterruptedException
    {
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        CountDownLatch latch = new CountDownLatch(1);
        final TestCachingAtomicChangeObserver observer2 = new TestCachingAtomicChangeObserver(latch);
        this.candidate.removeObserver(observer, name);
        this.candidate.removeObserver(observer2, name);
        this.candidate.addObserver(observer, name);
        this.candidate.addObserver(observer2, name);
        assertEquals(2, this.candidate.recordObservers.getSubscribersFor(name).length);
        this.candidate.removeObserver(observer, name);
        this.candidate.removeObserver(observer, name);
        assertEquals(1, this.candidate.recordObservers.getSubscribersFor(name).length);

        // now we only have observer2
        Map<String, IValue> record = new HashMap<String, IValue>();
        record.put(K1, V1);
        record.put(K2, V2);
        final Record instance = createRecordWaitForUpdate(name, record);
        assertNotNull(instance);
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        // only observer2 is notified
        final int size = observer.changes.size();
        assertEquals("Got: " + observer.changes, 0, size);

        assertEquals(record, observer2.getLatestImage());
        final IRecordChange latestChanges = observer2.changes.get(observer2.changes.size() - 1);
        assertEquals(2, latestChanges.getPutEntries().size());
        assertEquals(0, latestChanges.getOverwrittenEntries().size());
        assertEquals(0, latestChanges.getRemovedEntries().size());

        this.candidate.removeObserver(observer2, name);
        assertEquals(0, this.candidate.recordObservers.getSubscribersFor(name).length);
        assertNull(instance.atomicChange);
    }

    @Test
    public void testRemoveObserverAfterMapRemoved()
    {
        createRecordWaitForUpdate(name);
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(observer, name);
        assertNotNull(this.candidate.removeRecord(name));
        this.candidate.removeObserver(observer, name);
    }

    @Test
    public void testGetLastPublishedImage() throws InterruptedException
    {
        assertNull(this.candidate.getLastPublishedImage_callInRecordContext(name));

        createRecordWaitForUpdate(name);
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        observer.latch = new CountDownLatch(1);

        this.candidate.addObserver(observer, name);
        assertTrue(observer.latch.await(1, TimeUnit.SECONDS));

        observer.latch = new CountDownLatch(1);
        this.candidate.getRecord(name).put(K1, 1);
        this.candidate.publishAtomicChange(name);

        assertTrue(observer.latch.await(1, TimeUnit.SECONDS));

        // check the last published vs received
        final IRecord lastPublishedImage = this.candidate.getLastPublishedImage_callInRecordContext(name);
        assertEquals(observer.getLatestImage(), lastPublishedImage);
        assertTrue(lastPublishedImage instanceof ImmutableRecord);
    }

    @Test
    public void testActive()
    {
        assertTrue(this.candidate.isActive());
        this.candidate.destroy();
        assertFalse(this.candidate.isActive());
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotCreateDuplicateRpc()
    {
        this.candidate.createRpc(new RpcInstance(TypeEnum.TEXT, "rpc1"));
        this.candidate.createRpc(new RpcInstance(TypeEnum.TEXT, "rpc1", TypeEnum.TEXT));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateIllegalNameRecord_ACK()
    {
        createRecordWaitForUpdate(ProxyContext.ACK);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateIllegalNameRecord_NOK()
    {
        createRecordWaitForUpdate(ProxyContext.NOK);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateIllegalNameRecord_RPC_RECORD_RESULT_PREFIX()
    {
        createRecordWaitForUpdate(RpcInstance.RPC_RECORD_RESULT_PREFIX);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateIllegalNameRecord_2()
    {
        createRecordWaitForUpdate("ContextRecords");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateIllegalNameRecord_3()
    {
        createRecordWaitForUpdate(AtomicChangeTeleporter.PART_INDEX_PREFIX + "somthing");
    }

    @Test
    public void testCreateRemoveCreateDuplicateRpc()
    {
        this.candidate.createRpc(new RpcInstance(TypeEnum.TEXT, "rpc1"));
        this.candidate.removeRpc("rpc1");
        this.candidate.createRpc(new RpcInstance(TypeEnum.TEXT, "rpc1", TypeEnum.TEXT));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetAllRpcUnmodifiable()
    {
        this.candidate.createRpc(new RpcInstance(TypeEnum.TEXT, "rpc1"));
        final Map<String, IRpcInstance> allRpcs = this.candidate.getAllRpcs();
        assertEquals(this.candidate.rpcInstances, allRpcs);
        // will fail
        allRpcs.clear();
    }

    @Test
    public void testAddRemoveUpdateValidator() throws InterruptedException
    {
        // DONT USE MOCKITO - ITS RUBBISH AT MULTI-THREAD
        final IRecord record = createRecordWaitForUpdate(name);

        final CountDownLatch validator1OnRegistration = new CountDownLatch(1);
        final CountDownLatch validator2OnRegistration = new CountDownLatch(1);
        final CountDownLatch validator1OnDeregistration = new CountDownLatch(1);
        final CountDownLatch validator2OnDeregistration = new CountDownLatch(1);

        final AtomicReference<CountDownLatch> validate1 = new AtomicReference<CountDownLatch>();
        final AtomicReference<CountDownLatch> validate2 = new AtomicReference<CountDownLatch>();

        final List<IRecord> validateCalls1 = new ArrayList<IRecord>();
        final List<IRecord> validateCalls2 = new ArrayList<IRecord>();

        IValidator validator1 = new IValidator() {
            @Override
            public void validate(IRecord record, IRecordChange change)
            {
                System.err.println("validator1: " + record);
                if (record.getName().equals(name))
                {
                    validateCalls1.add(record);
                    validate1.get().countDown();
                }
            }

            @Override
            public void onRegistration(IPublisherContext context)
            {
                validator1OnRegistration.countDown();
            }

            @Override
            public void onDeregistration(IPublisherContext context)
            {
                validator1OnDeregistration.countDown();
            }
        };

        IValidator validator2 = new IValidator() {
            @Override
            public void validate(IRecord record, IRecordChange change)
            {
                System.err.println("validator2: " + record);
                if (record.getName().equals(name))
                {
                    validateCalls2.add(record);
                    validate2.get().countDown();
                }
            }

            @Override
            public void onRegistration(IPublisherContext context)
            {
                validator2OnRegistration.countDown();
            }

            @Override
            public void onDeregistration(IPublisherContext context)
            {
                validator2OnDeregistration.countDown();
            }
        };

        validate1.set(new CountDownLatch(2));
        validate2.set(new CountDownLatch(2));

        assertTrue(this.candidate.addValidator(validator1));
        assertFalse(this.candidate.addValidator(validator1));
        assertTrue(this.candidate.addValidator(validator2));
        assertFalse(this.candidate.addValidator(validator2));

        assertTrue(validator1OnRegistration.await(1, TimeUnit.SECONDS));
        assertTrue(validator2OnRegistration.await(1, TimeUnit.SECONDS));

        // update a record and do a force validate call too
        record.put(K1, V1);
        IRecord recordImage = ImmutableSnapshotRecord.create(record);
        this.candidate.publishAtomicChange(record);
        this.candidate.updateValidator(validator1);
        this.candidate.updateValidator(validator2);

        // check validate got called
        assertTrue(validate1.get().await(1, TimeUnit.SECONDS));
        assertTrue(validate2.get().await(1, TimeUnit.SECONDS));
        assertEquals(recordImage, validateCalls1.get(0));
        assertEquals(recordImage, validateCalls1.get(1));
        assertEquals(recordImage, validateCalls2.get(0));
        assertEquals(recordImage, validateCalls2.get(1));

        // remove validator1 and check onDeregistration is called
        assertTrue(this.candidate.removeValidator(validator1));
        assertFalse(this.candidate.removeValidator(validator1));
        assertTrue(validator1OnDeregistration.await(1, TimeUnit.SECONDS));

        // prepare for another update - only the first should get this
        validate1.set(new CountDownLatch(1));
        validate2.set(new CountDownLatch(1));

        // this update should not be picked up by the validator but by validator2
        record.put(K1, V2);
        recordImage = ImmutableSnapshotRecord.create(record);
        this.candidate.publishAtomicChange(record);

        assertTrue(validate2.get().await(1, TimeUnit.SECONDS));
        assertEquals(2, validateCalls1.size());
        assertEquals(recordImage, validateCalls2.get(2));

    }

    private Record createRecordWaitForUpdate(String name)
    {
        return createRecordWaitForUpdate(name, null);
    }

    private Record createRecordWaitForUpdate(String name, Map<String, IValue> data)
    {
        // wait for the first record image
        CountDownLatch latch = new CountDownLatch(1);
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver(latch);
        this.candidate.addObserver(observer, name);

        final Record record = (Record) (data == null ? this.candidate.createRecord(name) :
                this.candidate.createRecord(name, data));
        try
        {
            assertTrue(latch.await(1, TimeUnit.SECONDS));
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        this.candidate.removeObserver(observer, name);
        return record;
    }
}
