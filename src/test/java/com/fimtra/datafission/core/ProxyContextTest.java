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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IPermissionFilter;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.IStatusAttribute.Connection;
import com.fimtra.datafission.core.ProxyContext.IRemoteSystemRecordNames;
import com.fimtra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.Log;
import com.fimtra.util.TestUtils;
import com.fimtra.util.TestUtils.EventChecker;
import com.fimtra.util.TestUtils.EventCheckerWithFailureReason;
import com.fimtra.util.TestUtils.EventFailedException;

/**
 * Tests the {@link ProxyContext} and {@link Publisher}
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings({ "boxing", "unused" })
public class ProxyContextTest
{
    private static final int REMOTE_RECORD_GET_TIMEOUT_MILLIS = 1000;

    static List<TestLongValueSequenceCheckingAtomicChangeObserver> observers =
        new ArrayList<TestLongValueSequenceCheckingAtomicChangeObserver>();

    private String contextName = "TestContext";
    private int PORT;
    private static final String LOCALHOST = "localhost";
    private static final String KEY_PREFIX = "K";
    final static String record1 = "record1";
    final static String record2 = "record2";
    final static String record3 = "record3";
    final static int KEY_COUNT = 6;
    final static int UPDATE_COUNT = 10;
    private static final int ATOMIC_CHANGE_PERIOD_MILLIS = 20;

    private static final int TIMEOUT = 5;

    Map<String, Map<String, Long>> recordData;

    ProxyContext candidate;
    Publisher publisher;
    Context context;

    ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception
    {
        // no way to test this really
        System.getProperties().put(DataFissionProperties.Names.IGNORE_LOGGING_RX_COMMANDS_WITH_PREFIX, "rpc|concat2");
        ChannelUtils.WATCHDOG.configure(200, 10);
        observers.clear();
        // cycle the ports for each test
        this.PORT = getNextFreePort();
    }

    void createComponents(String name) throws IOException
    {
        this.contextName = getClass().getSimpleName() + "-" + name;
        System.err.println(this.contextName);
        Log.log(this, ">>> START: " + this.contextName);
        this.context = new Context(this.contextName);
        doSetup();
        this.publisher = new Publisher(this.context, getProtocolCodec(), LOCALHOST, this.PORT);
        this.candidate = new ProxyContext(this.contextName, getProtocolCodec(), LOCALHOST, this.PORT);
        this.candidate.setReconnectPeriodMillis(200);
    }

    protected StringProtocolCodec getProtocolCodec()
    {
        return new StringProtocolCodec();
    }

    static int START_PORT = 10000;
    static int END_PORT = 10100;

    int getNextFreePort()
    {
        return START_PORT++;
        // port scanning disabled to speed up tests
        // return TcpChannelUtils.getNextFreeTcpServerPort(null, START_PORT++, END_PORT++);
    }

    private void doSetup()
    {
        this.executor = Executors.newScheduledThreadPool(4);
        this.recordData = new HashMap<String, Map<String, Long>>();

        createMapAndStartUpdating(record1);
        createMapAndStartUpdating(record2);
        createMapAndStartUpdating(record3);
    }

    private void createMapAndStartUpdating(final String recordName)
    {
        Map<String, Long> values = new ConcurrentHashMap<String, Long>();
        for (int i = 0; i < KEY_COUNT - 1; i++)
        {
            values.put(KEY_PREFIX + i, 0l);
        }
        this.recordData.put(recordName, values);
        final Map<String, IValue> record = this.context.createRecord(recordName);
        for (int i = 0; i < KEY_COUNT; i++)
        {
            record.put(KEY_PREFIX + i, LongValue.valueOf(0));
        }

        startUpdating(recordName, record);
    }

    void startUpdating(final String recordName, final Map<String, IValue> record)
    {
        this.executor.scheduleAtFixedRate(new Runnable()
        {
            Random random = new Random();
            int count = 0;
            boolean remove;
            Map<String, Long> data = ProxyContextTest.this.recordData.get(recordName);

            @Override
            public void run()
            {
                try
                {
                    long next;
                    String key;
                    // one key never updates
                    for (int i = 0; i < KEY_COUNT - 1; i++)
                    {
                        key = KEY_PREFIX + i;
                        if (this.random.nextBoolean())
                        {
                            next = (this.data.get(key).longValue() + this.random.nextInt(5));
                            this.data.put(key, next);
                            record.put(key, LongValue.valueOf(next));
                        }
                        else
                        {
                            record.remove(key);
                        }
                    }
                    // publish every 2nd loop
                    if (this.count++ % 2 == 0)
                    {
                        ProxyContextTest.this.context.publishAtomicChange(recordName);
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }, 0, ATOMIC_CHANGE_PERIOD_MILLIS / 2, TimeUnit.MILLISECONDS);
    }

    @After
    public void tearDown() throws Exception
    {
        Log.log(this, ">>> teardown: " + this.candidate + "  " + this.publisher);

        this.executor.shutdownNow();
        this.publisher.destroy();
        this.candidate.destroy();
        this.context.destroy();

        List<TestLongValueSequenceCheckingAtomicChangeObserver> local =
            new ArrayList<TestLongValueSequenceCheckingAtomicChangeObserver>(observers);
        observers.clear();
        for (TestLongValueSequenceCheckingAtomicChangeObserver observer : local)
        {
            observer.verify();
        }
        // IO sensitive
        Thread.sleep(50);
        ChannelUtils.WATCHDOG.configure(5000);
    }

    @Test
    public void testAddObserverCountDownLatchIsTriggeredWhenAddingListenerDuringDisconnect() throws Exception
    {
        this.contextName =
            getClass().getSimpleName() + "-"
                + "testAddObserverCountDownLatchIsTriggeredWhenAddingListenerDuringDisconnect";
        System.err.println(this.contextName);

        this.context = new Context(this.contextName);
        doSetup();
        this.publisher = new Publisher(this.context, getProtocolCodec(), LOCALHOST, this.PORT);

        this.candidate = new ProxyContext(this.contextName, getProtocolCodec(), LOCALHOST, this.PORT);
        this.candidate.setReconnectPeriodMillis(200);

        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        Future<Map<String, Boolean>> addObserverLatch =
            this.candidate.addObserver(observer, "comma in, record", "double comma,, record");
        assertTrue(addObserverLatch.get(10, TimeUnit.SECONDS).size() == 2);

        this.publisher.destroy();
        Thread.sleep(1200);

        addObserverLatch = this.candidate.addObserver(observer, "lasers, record!");

        this.publisher = new Publisher(this.context, getProtocolCodec(), LOCALHOST, this.PORT);

        assertTrue(addObserverLatch.get(10, TimeUnit.SECONDS).size() == 1);
    }

    @Test
    public void testGetSubscribedRecords() throws Exception
    {
        createComponents("testGetSubscribedRecords");
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

    @Test
    public void testResubscribe() throws Exception
    {
        createComponents("testResubscribe");
        final String name = "sdf1";
        final String key = "Kmy1";
        final TextValue v1 = new TextValue("value1");

        this.context.createRecord(name);
        this.context.getRecord(name).put(key, v1);
        this.context.publishAtomicChange(name);

        final TestCachingAtomicChangeObserver listener = new TestCachingAtomicChangeObserver();
        final int timeout = TIMEOUT;
        listener.latch = new CountDownLatch(1);
        this.candidate.addObserver(listener, name);

        assertTrue(listener.latch.await(timeout, TimeUnit.SECONDS));
        assertEquals(v1, listener.getLatestImage().get(key));

        listener.latch = new CountDownLatch(1);
        this.candidate.resubscribe(name);
        assertTrue(listener.latch.await(timeout, TimeUnit.SECONDS));
        assertEquals(v1, listener.getLatestImage().get(key));
    }

    @Test
    public void testResyncForIncorrectSequence() throws Exception
    {
        createComponents("testResubscribe");
        final String name = "sdf1";
        final String key = "Kmy1";
        final TextValue v1 = new TextValue("value1");
        final TextValue v2 = new TextValue("value2");

        this.context.createRecord(name);
        this.context.getRecord(name).put(key, v1);
        this.context.publishAtomicChange(name);

        final TestCachingAtomicChangeObserver listener = new TestCachingAtomicChangeObserver();
        final int timeout = TIMEOUT;
        listener.latch = new CountDownLatch(1);
        this.candidate.addObserver(listener, name);

        assertTrue(listener.latch.await(timeout, TimeUnit.SECONDS));
        assertEquals(v1, listener.getLatestImage().get(key));

        listener.latch = new CountDownLatch(1);
        // removing the record will allow the sequences to be out thus simulating a missed message
        this.candidate.context.removeRecord(name);

        this.context.getRecord(name).put(key, v2);
        this.context.publishAtomicChange(name);

        assertTrue(listener.latch.await(timeout, TimeUnit.SECONDS));
        assertEquals(v2, listener.getLatestImage().get(key));
    }

    @Test
    public void testSingleRemoteContextSubMap() throws Exception
    {
        createComponents("testSingleRemoteContextSubMap");
        final TestCachingAtomicChangeObserver listener = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(listener, "subMap1");

        listener.latch = new CountDownLatch(1);
        final IRecord createRecord = this.context.createRecord("subMap1");
        this.context.publishAtomicChange("subMap1");
        final int timeout = TIMEOUT;
        assertTrue(listener.latch.await(timeout, TimeUnit.SECONDS));

        listener.latch = new CountDownLatch(1);
        createRecord.getOrCreateSubMap("submap1").put("K1", new TextValue("lasers"));
        this.context.publishAtomicChange("subMap1");
        assertTrue(listener.latch.await(timeout, TimeUnit.SECONDS));
    }

    @Test
    public void testSingleRemoteContext_RemoveFieldFromRecord() throws Exception
    {
        createComponents("testSingleRemoteContext_RemoveFieldFromRecord");
        final String recordName = "record78";
        final IRecord record = this.context.createRecord(recordName);
        record.put("F1", "lasers");

        final CountDownLatch recordSubscribedLatch = new CountDownLatch(1);
        this.candidate.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                if (imageCopy.containsKey(recordName))
                {
                    recordSubscribedLatch.countDown();
                }
            }
        }, IRemoteSystemRecordNames.REMOTE_CONTEXT_SUBSCRIPTIONS);

        final int timeout = TIMEOUT;
        final TestCachingAtomicChangeObserver listener = new TestCachingAtomicChangeObserver();
        listener.latch = new CountDownLatch(2);
        assertTrue("Did not get response for subscription",
            this.candidate.addObserver(listener, recordName).get(timeout, TimeUnit.SECONDS).size() == 1);

        // we must wait until we are sure the listener has been added
        assertTrue(recordSubscribedLatch.await(timeout, TimeUnit.SECONDS));

        this.context.publishAtomicChange(recordName);
        assertTrue("Got: " + listener.images, listener.latch.await(timeout, TimeUnit.SECONDS));
        assertEquals("Got:" + listener.getLatestImage(), 1, listener.getLatestImage().size());

        // remove the field
        listener.latch = new CountDownLatch(1);
        record.clear();
        this.context.publishAtomicChange(recordName);
        assertTrue(listener.latch.await(timeout, TimeUnit.SECONDS));
        assertEquals("Got:" + listener.getLatestImage(), 0, listener.getLatestImage().size());
    }

    @Test
    public void testSingleRemoteContextSingleSubscription() throws Exception
    {
        createComponents("testSingleRemoteContextSingleSubscription");
        CountDownLatch record1Latch = new CountDownLatch(UPDATE_COUNT);
        registerObserverForMap(this.candidate, record1, record1Latch);

        awaitLatch(record1Latch);
    }

    @Test
    public void testSingleRemoteContextSingleSubscription_permission() throws Exception
    {
        createComponents("testSingleRemoteContextSingleSubscription_permission");

        String name2 = "duff2";
        String permissionToken = "pt_sdf2";
        IPermissionFilter filter = Mockito.mock(IPermissionFilter.class);
        when(filter.accept(eq(permissionToken), eq(record1))).thenReturn(true);
        when(filter.accept(eq(permissionToken), eq(record2))).thenReturn(false);

        this.context.setPermissionFilter(filter);

        CountDownLatch record1Latch = new CountDownLatch(UPDATE_COUNT);
        TestLongValueSequenceCheckingAtomicChangeObserver observer =
            new TestLongValueSequenceCheckingAtomicChangeObserver();
        observer.latch = record1Latch;
        final Map<String, Boolean> result =
            this.candidate.addObserver(permissionToken, observer, record1, record2).get(5, TimeUnit.SECONDS);

        assertEquals(2, result.size());
        assertTrue("Got: " + result, result.get(record1));
        assertFalse("Got: " + result, result.get(record2));

        observers.add(observer);

        awaitLatch(record1Latch);

        Mockito.verify(filter).accept(eq(permissionToken), eq(record1));
        Mockito.verify(filter).accept(eq(permissionToken), eq(record2));
        verifyNoMoreInteractions(filter);
    }

    @Test
    public void testSingleRemoteContextSingleSubscription_removeNullObserver() throws Exception
    {
        createComponents("testSingleRemoteContextSingleSubscription_removeNullObserver");
        CountDownLatch record1Latch = new CountDownLatch(UPDATE_COUNT);
        registerObserverForMap(this.candidate, record1, record1Latch);

        this.candidate.removeObserver(null, record1);
        this.candidate.removeObserver(null, record1);

        awaitLatch(record1Latch);
    }

    @Test
    public void testSingleRemoteContextMultipleSubscriptions() throws Exception
    {
        createComponents("testSingleRemoteContextMultipleSubscriptions");
        CountDownLatch record1Latch = new CountDownLatch(UPDATE_COUNT);
        registerObserverForMap(this.candidate, record1, record1Latch);
        CountDownLatch record2Latch = new CountDownLatch(UPDATE_COUNT);
        registerObserverForMap(this.candidate, record2, record2Latch);
        CountDownLatch record3Latch = new CountDownLatch(UPDATE_COUNT);
        registerObserverForMap(this.candidate, record3, record3Latch);

        awaitLatch(record1Latch);
        awaitLatch(record2Latch);
        awaitLatch(record3Latch);
    }

    @Test
    public void testSubscribeForRemoteContextRegistry() throws Exception
    {
        createComponents("testSubscribeForRemoteContextRegistry");
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver(true);
        observer.latch = new CountDownLatch(3);
        this.candidate.addObserver(observer, IRemoteSystemRecordNames.REMOTE_CONTEXT_RECORDS);
        final Set<String> expected =
            new HashSet<String>(Arrays.asList("ContextSubscriptions", "ContextRecords", "record3", "ContextRpcs",
                "record2", "record1", "ContextStatus", "ContextConnections"));

        TestUtils.waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                final IRecord latestImage = observer.getLatestImage();
                if (latestImage == null)
                {
                    return latestImage;
                }
                return latestImage.keySet();
            }

            @Override
            public Object expect()
            {
                return expected;
            }
        });

    }

    @Test
    public void testSingleRemoteContextWithUnsubscribing() throws Exception
    {
        createComponents("testSingleRemoteContextWithUnsubscribing");
        assertFalse(this.candidate.isRecordConnected(record1));
        assertFalse(this.candidate.isRecordConnected(record2));
        assertFalse(this.candidate.isRecordConnected(record3));

        CountDownLatch record1Latch = new CountDownLatch(UPDATE_COUNT);
        TestLongValueSequenceCheckingAtomicChangeObserver observer1 =
            registerObserverForMap(this.candidate, record1, record1Latch);
        this.candidate.removeObserver(observer1, record1);

        CountDownLatch record2Latch = new CountDownLatch(UPDATE_COUNT);
        TestLongValueSequenceCheckingAtomicChangeObserver observer2 =
            registerObserverForMap(this.candidate, record2, record2Latch);
        CountDownLatch record3Latch = new CountDownLatch(UPDATE_COUNT);
        TestLongValueSequenceCheckingAtomicChangeObserver observer3 =
            registerObserverForMap(this.candidate, record3, record3Latch);

        // re-register
        record1Latch = new CountDownLatch(UPDATE_COUNT);
        observer1 = registerObserverForMap(this.candidate, record1, record1Latch);

        awaitLatch(record1Latch);
        awaitLatch(record2Latch);
        awaitLatch(record3Latch);

        checkRecordConnectedStatus(record1, true);
        checkRecordConnectedStatus(record2, true);
        checkRecordConnectedStatus(record3, true);

        this.candidate.removeObserver(observer1, record1);
        this.candidate.removeObserver(observer2, record2);
        this.candidate.removeObserver(observer3, record3);

        checkRecordConnectedStatus(record1, false);
        checkRecordConnectedStatus(record2, false);
        checkRecordConnectedStatus(record3, false);

        observer1.verify();
    }

    void checkRecordConnectedStatus(final String recordName, final Boolean status) throws InterruptedException,
        EventFailedException
    {
        waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                return ProxyContextTest.this.candidate.isRecordConnected(recordName);
            }

            @Override
            public Object expect()
            {
                return status;
            }
        });
    }

    @Test
    public void testMultipleRemoteContextsAndSubscribeForSingleRecord() throws Exception
    {
        createComponents("testMultipleRemoteContextsAndSubscribeForSingleRecord");
        ProxyContext candidate2 = new ProxyContext("testRemote2", getProtocolCodec(), LOCALHOST, this.PORT);
        try
        {
            final CountDownLatch gotAllLatch = new CountDownLatch(1);
            final TestCachingAtomicChangeObserver remoteSubscriptionsObserver = new TestCachingAtomicChangeObserver()
            {
                @Override
                public void onChange(IRecord image, IRecordChange atomicChange)
                {
                    super.onChange(image, atomicChange);
                    if (image.containsKey(record1) && image.get(record1).longValue() == 2)
                    {
                        gotAllLatch.countDown();
                    }
                }
            };

            CountDownLatch rc1record1Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(this.candidate, record1, rc1record1Latch);

            this.candidate.addObserver(remoteSubscriptionsObserver,
                IRemoteSystemRecordNames.REMOTE_CONTEXT_SUBSCRIPTIONS);

            CountDownLatch rc2record1Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(candidate2, record1, rc2record1Latch);

            final int timeout = TIMEOUT;
            boolean await = gotAllLatch.await(timeout, TimeUnit.SECONDS);
            assertTrue("Got: " + remoteSubscriptionsObserver.getLatestImage(), await);

            awaitLatch(rc1record1Latch);
            awaitLatch(rc2record1Latch);
        }
        finally
        {
            candidate2.destroy();
        }
    }

    @Test
    public void testMultipleRemoteContextsAndSubscribeForRemoteContextSubscriptions() throws IOException,
        InterruptedException
    {
        createComponents("testMultipleRemoteContextsAndSubscribeForRemoteContextSubscriptions");
        ProxyContext candidate2 = new ProxyContext("testRemote2", getProtocolCodec(), LOCALHOST, this.PORT);
        try
        {
            final CountDownLatch gotAllLatch = new CountDownLatch(1);
            final TestCachingAtomicChangeObserver remoteSubscriptionsObserver = new TestCachingAtomicChangeObserver()
            {
                @Override
                public void onChange(IRecord image, IRecordChange atomicChange)
                {
                    super.onChange(image, atomicChange);
                    if (image.containsKey(record1) && image.get(record1).longValue() == 2 && image.containsKey(record2)
                        && image.get(record2).longValue() == 2 && image.containsKey(record3)
                        && image.get(record3).longValue() == 2)
                    {
                        gotAllLatch.countDown();
                    }
                }
            };

            CountDownLatch rc1record1Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(this.candidate, record1, rc1record1Latch);
            CountDownLatch rc1record2Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(this.candidate, record2, rc1record2Latch);
            CountDownLatch rc1record3Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(this.candidate, record3, rc1record3Latch);

            this.candidate.addObserver(remoteSubscriptionsObserver,
                IRemoteSystemRecordNames.REMOTE_CONTEXT_SUBSCRIPTIONS);

            CountDownLatch rc2record1Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(candidate2, record1, rc2record1Latch);
            CountDownLatch rc2record2Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(candidate2, record2, rc2record2Latch);
            CountDownLatch rc2record3Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(candidate2, record3, rc2record3Latch);

            final int timeout = TIMEOUT;
            boolean await = gotAllLatch.await(timeout, TimeUnit.SECONDS);
            assertTrue("Got: " + remoteSubscriptionsObserver.getLatestImage(), await);

            awaitLatch(rc1record1Latch);
            awaitLatch(rc1record2Latch);
            awaitLatch(rc1record3Latch);
            awaitLatch(rc2record1Latch);
            awaitLatch(rc2record2Latch);
            awaitLatch(rc2record3Latch);
        }
        finally
        {
            candidate2.destroy();
        }
    }

    @Test
    public void testContextStatusReflectsMultipleRemoteContexts() throws Exception
    {
        createComponents("testContextStatusReflectsMultipleRemoteContexts");
        final int fieldCountForSingleConnection = 12;

        this.publisher.publishContextConnectionsRecordAtPeriod(20);
        final IRecord connectionsRecord = this.context.getRecord(ISystemRecordNames.CONTEXT_CONNECTIONS);

        TestUtils.waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                return connectionsRecord.getSubMapKeys().size();
            }

            @Override
            public Object expect()
            {
                return 1;
            }
        });

        final ProxyContext candidate2 =
            new ProxyContext("testRemote2.2(candidate2)", getProtocolCodec(), LOCALHOST, this.PORT);
        try
        {
            TestUtils.waitForEvent(new EventChecker()
            {
                @Override
                public Object got()
                {
                    return connectionsRecord.getSubMapKeys().size();
                }

                @Override
                public Object expect()
                {
                    return 2;
                }
            });

            TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
            candidate2.addObserver(observer, IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS);

            // verify the publisher connection state
            TestUtils.waitForEvent(new EventCheckerWithFailureReason()
            {
                @Override
                public Object got()
                {
                    int fieldAttributeCount = 0;
                    for (String connection : connectionsRecord.getSubMapKeys())
                    {
                        fieldAttributeCount += connectionsRecord.getOrCreateSubMap(connection).size();
                    }
                    return fieldAttributeCount;
                }

                @Override
                public Object expect()
                {
                    return fieldCountForSingleConnection * 2;
                }

                @Override
                public String getFailureReason()
                {
                    return "Connections are: " + connectionsRecord;
                }
            }, 5000);

            TestUtils.waitForEvent(new EventChecker()
            {
                @Override
                public Object got()
                {
                    int fieldAttributeCount = 0;
                    final IRecord record = candidate2.getRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS);
                    if (record == null)
                    {
                        return null;
                    }
                    for (String connection : record.getSubMapKeys())
                    {
                        fieldAttributeCount += record.getOrCreateSubMap(connection).size();
                    }
                    return fieldAttributeCount;
                }

                @Override
                public Object expect()
                {
                    return fieldCountForSingleConnection * 2;
                }
            });

            this.candidate.destroy();

            TestUtils.waitForEvent(new EventChecker()
            {
                @Override
                public Object got()
                {
                    int fieldAttributeCount = 0;
                    for (String connection : connectionsRecord.getSubMapKeys())
                    {
                        fieldAttributeCount += connectionsRecord.getOrCreateSubMap(connection).size();
                    }
                    return fieldAttributeCount;
                }

                @Override
                public Object expect()
                {
                    return fieldCountForSingleConnection;
                }
            });

            TestUtils.waitForEvent(new EventCheckerWithFailureReason()
            {
                @Override
                public Object got()
                {
                    int fieldAttributeCount = 0;
                    final IRecord record = candidate2.getRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS);
                    if (record == null)
                    {
                        return null;
                    }
                    for (String connection : record.getSubMapKeys())
                    {
                        fieldAttributeCount += record.getOrCreateSubMap(connection).size();
                    }
                    return fieldAttributeCount;
                }

                @Override
                public Object expect()
                {
                    return fieldCountForSingleConnection;
                }

                @Override
                public String getFailureReason()
                {
                    return "REMOTE_CONTEXT_CONNECTIONS record was: "
                        + candidate2.getRecord(IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS);
                }
            });
        }
        finally
        {
            candidate2.destroy();
        }
    }

    @Test
    public void testDuplicateSubscriptions() throws Exception
    {
        createComponents("testDuplicateSubscriptions");
        CountDownLatch record1Latch = new CountDownLatch(UPDATE_COUNT);
        registerObserverForMap(this.candidate, record1, record1Latch);

        // duplicate subscription here
        TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        observer.latch = new CountDownLatch(1);
        this.candidate.addObserver(observer, record1);

        awaitLatch(record1Latch);
        final int timeout = TIMEOUT;
        assertTrue(observer.latch.await(timeout, TimeUnit.SECONDS));
    }

    @Test
    public void testDuplicateSubscriptionsFromDifferentRemoteContexts() throws Exception
    {
        createComponents("testDuplicateSubscriptionsFromDifferentRemoteContexts");
        ProxyContext candidate2 = new ProxyContext("c2", getProtocolCodec(), LOCALHOST, this.PORT);
        try
        {
            CountDownLatch record1Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(this.candidate, record1, record1Latch);

            // duplicate subscription here from the second remote context
            TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
            observer.latch = new CountDownLatch(1);
            candidate2.addObserver(observer, record1);

            awaitLatch(record1Latch);
            final int timeout = TIMEOUT;
            assertTrue(observer.latch.await(timeout, TimeUnit.SECONDS));

        }
        finally
        {
            candidate2.destroy();
        }
    }

    @Test
    public void testSubscriptionsBeforeMapCreation() throws Exception
    {
        createComponents("testSubscriptionsBeforeMapCreation");
        this.executor.shutdownNow();
        this.candidate.destroy();
        this.publisher.destroy();
        this.context.destroy();

        this.PORT = getNextFreePort();
        this.context = new Context("test");

        this.publisher = new Publisher(this.context, getProtocolCodec(), LOCALHOST, this.PORT);
        this.candidate = new ProxyContext(this.contextName, getProtocolCodec(), LOCALHOST, this.PORT);

        CountDownLatch record1Latch = new CountDownLatch(UPDATE_COUNT);
        registerObserverForMap(this.candidate, record1, record1Latch);
        CountDownLatch record2Latch = new CountDownLatch(UPDATE_COUNT);
        registerObserverForMap(this.candidate, record2, record2Latch);
        CountDownLatch record3Latch = new CountDownLatch(UPDATE_COUNT);
        registerObserverForMap(this.candidate, record3, record3Latch);

        doSetup();

        awaitLatch(record1Latch);
        awaitLatch(record2Latch);
        awaitLatch(record3Latch);
    }

    @Test
    public void testPublisherWithSingleSubscriptionAndTcpConnectionInterrupted() throws Exception
    {
        createComponents("testPublisherWithSingleSubscriptionAndTcpConnectionInterrupted");
        final int timeout = TIMEOUT;
        final AtomicReference<CountDownLatch> connected = new AtomicReference<CountDownLatch>(new CountDownLatch(1));

        TestCachingAtomicChangeObserver statusObserver = new TestCachingAtomicChangeObserver()
        {
            @Override
            public void onChange(IRecord image, IRecordChange atomicChange)
            {
                super.onChange(image, atomicChange);
                if ((IStatusAttribute.Utils.getStatus(Connection.class, image)).equals(Connection.CONNECTED))
                {
                    connected.get().countDown();
                }
            }
        };
        this.candidate.addObserver(statusObserver, ISystemRecordNames.CONTEXT_STATUS);

        assertTrue(connected.get().await(timeout, TimeUnit.SECONDS));

        TestLongValueSequenceCheckingAtomicChangeObserver observer1 =
            new TestLongValueSequenceCheckingAtomicChangeObserver()
            {
                @Override
                public void onChange(IRecord source, IRecordChange atomicChange)
                {
                    super.onChange(source, atomicChange);
                }
            };
        observer1.latch = new CountDownLatch(UPDATE_COUNT);
        this.candidate.addObserver(observer1, record1);

        awaitLatch(observer1.latch);

        IRecord connectionStatusRecord = this.candidate.getRecord(ProxyContext.RECORD_CONNECTION_STATUS_NAME);
        assertEquals(ProxyContext.RECORD_CONNECTED, connectionStatusRecord.get(record1));

        final CountDownLatch recordsDisconnected = new CountDownLatch(1);
        this.candidate.addObserver(new IRecordListener()
        {
            boolean record1disconnected;

            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                if (ProxyContext.RECORD_DISCONNECTED.equals(imageCopy.get(record1)))
                {
                    this.record1disconnected = true;
                }
                if (this.record1disconnected)
                {
                    recordsDisconnected.countDown();
                }
            }
        }, ProxyContext.RECORD_CONNECTION_STATUS_NAME);

        statusObserver.reset();
        connected.set(new CountDownLatch(1));

        // INTERRUPT!
        this.candidate.channel.destroy("unit test");
        // we expect connected->disconnected->reconnecting->connected
        assertTrue(recordsDisconnected.await(timeout, TimeUnit.SECONDS));
        assertTrue(connected.get().await(timeout, TimeUnit.SECONDS));

        observer1.latch = new CountDownLatch(UPDATE_COUNT);

        awaitLatch(observer1.latch);

        assertEquals(Connection.CONNECTED, IStatusAttribute.Utils.getStatus(Connection.class, this.candidate));

        assertEquals(ProxyContext.RECORD_CONNECTED, connectionStatusRecord.get(record1));

        observer1.verify();
    }

    @Test
    public void testPublisherWithMultipleSubscriptionsAndTcpConnectionInterrupted() throws Exception
    {
        createComponents("testPublisherWithMultipleSubscriptionsAndTcpConnectionInterrupted");
        final int timeout = TIMEOUT;
        final AtomicReference<CountDownLatch> connected = new AtomicReference<CountDownLatch>(new CountDownLatch(1));

        TestCachingAtomicChangeObserver statusObserver = new TestCachingAtomicChangeObserver()
        {
            @Override
            public void onChange(IRecord image, IRecordChange atomicChange)
            {
                super.onChange(image, atomicChange);
                if ((IStatusAttribute.Utils.getStatus(Connection.class, image)).equals(Connection.CONNECTED))
                {
                    connected.get().countDown();
                }
            }
        };
        this.candidate.addObserver(statusObserver, ISystemRecordNames.CONTEXT_STATUS);

        assertTrue(connected.get().await(timeout, TimeUnit.SECONDS));

        TestLongValueSequenceCheckingAtomicChangeObserver observer1 =
            new TestLongValueSequenceCheckingAtomicChangeObserver()
            {
                @Override
                public void onChange(IRecord source, IRecordChange atomicChange)
                {
                    super.onChange(source, atomicChange);
                }
            };
        observer1.latch = new CountDownLatch(UPDATE_COUNT);
        this.candidate.addObserver(observer1, record1);

        TestLongValueSequenceCheckingAtomicChangeObserver observer2 =
            new TestLongValueSequenceCheckingAtomicChangeObserver()
            {
                @Override
                public void onChange(IRecord source, IRecordChange atomicChange)
                {
                    super.onChange(source, atomicChange);
                }
            };
        observer2.latch = new CountDownLatch(UPDATE_COUNT);
        this.candidate.addObserver(observer2, record2);

        TestLongValueSequenceCheckingAtomicChangeObserver observer3 =
            new TestLongValueSequenceCheckingAtomicChangeObserver()
            {
                @Override
                public void onChange(IRecord source, IRecordChange atomicChange)
                {
                    super.onChange(source, atomicChange);
                }
            };
        observer3.latch = new CountDownLatch(UPDATE_COUNT);
        this.candidate.addObserver(observer3, record3);

        awaitLatch(observer1.latch);
        awaitLatch(observer2.latch);
        awaitLatch(observer3.latch);

        IRecord connectionStatusRecord = this.candidate.getRecord(ProxyContext.RECORD_CONNECTION_STATUS_NAME);
        assertEquals(ProxyContext.RECORD_CONNECTED, connectionStatusRecord.get(record1));
        assertEquals(ProxyContext.RECORD_CONNECTED, connectionStatusRecord.get(record2));
        assertEquals(ProxyContext.RECORD_CONNECTED, connectionStatusRecord.get(record3));

        final CountDownLatch recordsDisconnected = new CountDownLatch(1);
        this.candidate.addObserver(new IRecordListener()
        {
            boolean record1disconnected, record2disconnected, record3disconnected;

            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                if (ProxyContext.RECORD_DISCONNECTED.equals(imageCopy.get(record1)))
                {
                    this.record1disconnected = true;
                }
                if (ProxyContext.RECORD_DISCONNECTED.equals(imageCopy.get(record2)))
                {
                    this.record2disconnected = true;
                }
                if (ProxyContext.RECORD_DISCONNECTED.equals(imageCopy.get(record3)))
                {
                    this.record3disconnected = true;
                }
                if (this.record1disconnected && this.record2disconnected && this.record3disconnected)
                {
                    recordsDisconnected.countDown();
                }
            }
        }, ProxyContext.RECORD_CONNECTION_STATUS_NAME);

        statusObserver.reset();
        connected.set(new CountDownLatch(1));

        // INTERRUPT!
        this.candidate.channel.destroy("unit test interrupting!");
        // we expect connected->disconnected->reconnecting->connected
        assertTrue(recordsDisconnected.await(timeout, TimeUnit.SECONDS));
        assertTrue(connected.get().await(timeout, TimeUnit.SECONDS));

        observer1.latch = new CountDownLatch(UPDATE_COUNT);
        observer2.latch = new CountDownLatch(UPDATE_COUNT);
        observer3.latch = new CountDownLatch(UPDATE_COUNT);

        awaitLatch(observer1.latch);
        awaitLatch(observer2.latch);
        awaitLatch(observer3.latch);

        assertEquals(Connection.CONNECTED, IStatusAttribute.Utils.getStatus(Connection.class, this.candidate));

        assertEquals(ProxyContext.RECORD_CONNECTED, connectionStatusRecord.get(record1));
        assertEquals(ProxyContext.RECORD_CONNECTED, connectionStatusRecord.get(record2));
        assertEquals(ProxyContext.RECORD_CONNECTED, connectionStatusRecord.get(record3));

        observer1.verify();
        observer2.verify();
        observer3.verify();
    }

    @Test
    public void testPublisherDestroyed() throws Exception
    {
        createComponents("testPublisherDestroyed");
        TestCachingAtomicChangeObserver contextStatusObserver = new TestCachingAtomicChangeObserver(true);
        this.candidate.addObserver(contextStatusObserver, ISystemRecordNames.CONTEXT_STATUS);
        TestCachingAtomicChangeObserver recordConnectionObserver = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(recordConnectionObserver, ProxyContext.RECORD_CONNECTION_STATUS_NAME);

        TestLongValueSequenceCheckingAtomicChangeObserver observer1 =
            new TestLongValueSequenceCheckingAtomicChangeObserver();
        observer1.latch = new CountDownLatch(UPDATE_COUNT);
        this.candidate.addObserver(observer1, record1);

        TestLongValueSequenceCheckingAtomicChangeObserver observer2 =
            new TestLongValueSequenceCheckingAtomicChangeObserver();
        observer2.latch = new CountDownLatch(UPDATE_COUNT);
        this.candidate.addObserver(observer2, record2);

        TestLongValueSequenceCheckingAtomicChangeObserver observer3 =
            new TestLongValueSequenceCheckingAtomicChangeObserver();
        observer3.latch = new CountDownLatch(UPDATE_COUNT);
        this.candidate.addObserver(observer3, record3);

        awaitLatch(observer1.latch);
        awaitLatch(observer2.latch);
        awaitLatch(observer3.latch);

        IRecord connectionStatusRecord = this.candidate.getRecord(ProxyContext.RECORD_CONNECTION_STATUS_NAME);
        assertEquals(ProxyContext.RECORD_CONNECTED, connectionStatusRecord.get(record1));
        assertEquals(ProxyContext.RECORD_CONNECTED, connectionStatusRecord.get(record2));
        assertEquals(ProxyContext.RECORD_CONNECTED, connectionStatusRecord.get(record3));

        contextStatusObserver.reset();
        contextStatusObserver.latch = new CountDownLatch(2);
        this.publisher.destroy();

        final int timeout = TIMEOUT;
        assertTrue("Remaining count is " + contextStatusObserver.latch.getCount(),
            contextStatusObserver.latch.await(timeout, TimeUnit.SECONDS));

        final CountDownLatch recordsDisconnected = new CountDownLatch(1);
        this.candidate.addObserver(new IRecordListener()
        {
            boolean record1disconnected, record2disconnected, record3disconnected;
            boolean record1reconnecting, record2reconnecting, record3reconnecting;

            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                if (ProxyContext.RECORD_DISCONNECTED.equals(imageCopy.get(record1)))
                {
                    this.record1disconnected = true;
                }
                if (ProxyContext.RECORD_DISCONNECTED.equals(imageCopy.get(record2)))
                {
                    this.record2disconnected = true;
                }
                if (ProxyContext.RECORD_DISCONNECTED.equals(imageCopy.get(record3)))
                {
                    this.record3disconnected = true;
                }
                if (ProxyContext.RECORD_CONNECTING.equals(imageCopy.get(record1)))
                {
                    this.record1reconnecting = true;
                }
                if (ProxyContext.RECORD_CONNECTING.equals(imageCopy.get(record2)))
                {
                    this.record2reconnecting = true;
                }
                if (ProxyContext.RECORD_CONNECTING.equals(imageCopy.get(record3)))
                {
                    this.record3reconnecting = true;
                }
                if (this.record1disconnected && this.record2disconnected && this.record3disconnected
                    && this.record1reconnecting && this.record2reconnecting && this.record3reconnecting)
                {
                    recordsDisconnected.countDown();
                }
            }
        }, ProxyContext.RECORD_CONNECTION_STATUS_NAME);
        assertTrue(recordsDisconnected.await(timeout, TimeUnit.SECONDS));

        int size = contextStatusObserver.images.size();
        IRecord last = contextStatusObserver.images.get(size - 1);
        IRecord lastButOne = contextStatusObserver.images.get(size - 2);
        String message = "Got: " + contextStatusObserver.images;
        Connection lastStatus = IStatusAttribute.Utils.getStatus(Connection.class, last);
        Connection lastButOneStatus = IStatusAttribute.Utils.getStatus(Connection.class, lastButOne);
        if (Connection.RECONNECTING.equals(lastStatus))
        {
            assertEquals(message, Connection.DISCONNECTED, lastButOneStatus);
        }
        else
        {
            if (Connection.DISCONNECTED.equals(lastStatus))
            {
                assertEquals(message, Connection.RECONNECTING, lastButOneStatus);
            }
            else
            {
                fail("Did not get correct status " + last + " evaluates to " + lastStatus + ", full messages="
                    + message);
            }
        }

        observer1.verify();
        observer2.verify();
        observer3.verify();
    }

    @Test
    public void testTextValueWithCRLF() throws Exception
    {
        createComponents("testTextValueWithCRLF");
        TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        observer.latch = new CountDownLatch(1);
        String crlfMapName = "crlfMap";
        this.candidate.addObserver(observer, crlfMapName);

        Map<String, IValue> crlfMap = this.context.createRecord(crlfMapName);
        final int timeout = TIMEOUT;
        assertTrue(observer.latch.await(timeout, TimeUnit.SECONDS));

        observer.reset();
        observer.latch = new CountDownLatch(UPDATE_COUNT);
        String text = "the value \r\n has a terminator";
        String key = "crlf k1";
        for (int i = 0; i < UPDATE_COUNT; i++)
        {
            String value = text + i;
            crlfMap.put(key, new TextValue(value));
            crlfMap.put("crlf value2", new TextValue(value));
            this.context.publishAtomicChange(crlfMapName).await();
        }

        assertTrue("Triggered " + (UPDATE_COUNT - observer.latch.getCount()) + " times",
            observer.latch.await(timeout, TimeUnit.SECONDS));
        assertEquals(UPDATE_COUNT, observer.images.size());
        int max = UPDATE_COUNT - 1;
        Map<String, IValue> image = observer.images.get(max);
        assertEquals("Image: " + image, text + max, image.get(key).textValue());
    }

    @Test
    public void testGetRemoteRecordImage() throws Exception
    {
        createComponents("testGetRemoteRecordImage");
        Map<?, ?> remoteRecordImage = this.candidate.getRemoteRecordImage(record1, REMOTE_RECORD_GET_TIMEOUT_MILLIS);
        assertNotNull(remoteRecordImage);
        assertFalse("Got: " + remoteRecordImage, remoteRecordImage.size() == 0);

        // now try when we already have a subscription
        TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        observer.latch = new CountDownLatch(1);
        this.candidate.addObserver(observer, record1);
        final int timeout = TIMEOUT;
        assertTrue(observer.latch.await(timeout, TimeUnit.SECONDS));

        remoteRecordImage = this.candidate.getRemoteRecordImage(record1, REMOTE_RECORD_GET_TIMEOUT_MILLIS);
        assertNotNull(remoteRecordImage);
        assertFalse("Got: " + remoteRecordImage, remoteRecordImage.size() == 0);
    }

    @Test
    public void testRemoteRecordUpdateSequenceTracksLocal() throws Exception
    {
        createComponents("testRemoteRecordUpdateSequenceTracksLocal");
        String recordName = "sdf3";
        IRecord record = this.context.createRecord(recordName);

        final CountDownLatch latch = new CountDownLatch(3);
        final AtomicReference<IRecord> image = new AtomicReference<IRecord>();
        this.context.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
            {
                if (atomicChange.getSequence() > 0)
                {
                    image.set(imageValidInCallingThreadOnly);
                    latch.countDown();
                }
            }
        }, recordName);

        record.put("t1", LongValue.valueOf(1));
        this.context.publishAtomicChange(record);

        record.put("t1", LongValue.valueOf(2));
        this.context.publishAtomicChange(record);

        record.put("t1", LongValue.valueOf(3));
        this.context.publishAtomicChange(record);

        awaitLatch(latch);

        assertEquals(3, image.get().getSequence());
        assertEquals("Got: " + record, 3, record.getSequence());

        IRecord remoteRecordImage = this.candidate.getRemoteRecordImage(recordName, REMOTE_RECORD_GET_TIMEOUT_MILLIS);
        assertNotNull(remoteRecordImage);
        assertEquals(3, remoteRecordImage.getSequence());

        final CountDownLatch latch2 = new CountDownLatch(1);
        this.candidate.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
            {
                if (atomicChange.getSequence() == 4)
                {
                    latch2.countDown();
                }
            }
        }, recordName);

        // update the record again, check we get sequence (version) 4
        record.put("t1", LongValue.valueOf(4));
        this.context.publishAtomicChange(record);
        awaitLatch(latch2);
        assertEquals(4, record.getSequence());
        assertEquals(4, this.candidate.getRecord(recordName).getSequence());
    }

    @Test
    public void test2Publishers() throws Exception
    {
        createComponents("test2Publishers");
        this.candidate.destroy();

        this.candidate = new ProxyContext(this.contextName, getProtocolCodec(), LOCALHOST, this.PORT);

        this.PORT = getNextFreePort();
        // attach a 2nd publisher to the context
        Publisher publisher2 = new Publisher(this.context, getProtocolCodec(), LOCALHOST, this.PORT);
        ProxyContext candidate2 = new ProxyContext(this.contextName + "2", getProtocolCodec(), LOCALHOST, this.PORT);

        try
        {
            CountDownLatch record1Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(this.candidate, record1, record1Latch);
            CountDownLatch record2Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(this.candidate, record2, record2Latch);
            CountDownLatch record3Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(this.candidate, record3, record3Latch);

            CountDownLatch record1_2Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(candidate2, record1, record1_2Latch);
            CountDownLatch record2_2Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(candidate2, record2, record2_2Latch);
            CountDownLatch record3_2Latch = new CountDownLatch(UPDATE_COUNT);
            registerObserverForMap(candidate2, record3, record3_2Latch);

            awaitLatch(record1Latch);
            awaitLatch(record2Latch);
            awaitLatch(record3Latch);

            awaitLatch(record1_2Latch);
            awaitLatch(record2_2Latch);
            awaitLatch(record3_2Latch);
        }
        finally
        {
            candidate2.destroy();
            publisher2.destroy();
        }
    }

    @Test
    public void testInitialObserverForBlankMap() throws Exception
    {
        createComponents("testInitialObserverForBlankMap");
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        observer.latch = new CountDownLatch(1);
        this.candidate.addObserver(observer, "name");

        this.context.createRecord("name");
        final int timeout = TIMEOUT;
        assertTrue(observer.latch.await(timeout, TimeUnit.SECONDS));
    }

    @Test
    public void testSubscribeForRpcBeforeRpcIsCreated() throws Exception
    {
        createComponents("testSubscribeForRpcBeforeRpcIsCreated");
        assertNull(this.candidate.getRpc("getTime"));
        CountDownLatch latch = new CountDownLatch(1);
        TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver(latch);
        this.candidate.addObserver(observer, IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
        final int timeout = TIMEOUT;
        assertTrue(latch.await(timeout, TimeUnit.SECONDS));

        observer.latch = new CountDownLatch(1);
        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                return LongValue.valueOf(System.currentTimeMillis());
            }
        }, TypeEnum.LONG, "getTime");
        this.context.createRpc(rpc);

        assertTrue(observer.latch.await(timeout, TimeUnit.SECONDS));
        assertNotNull(this.candidate.getRpc("getTime"));
    }

    @Test
    public void testSubscribeForRpcAfterRpcIsCreated() throws Exception
    {
        createComponents("testSubscribeForRpcAfterRpcIsCreated");
        assertNull(this.candidate.getRpc("getTime"));

        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                return LongValue.valueOf(System.currentTimeMillis());
            }
        }, TypeEnum.LONG, "getTime");
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);
        assertNotNull(this.candidate.getRpc("getTime"));
    }

    @Test
    public void testSubscribeForRpcAfterRpcIsCreatedAndDestroyed() throws TimeOutException, ExecutionException,
        InterruptedException, IOException
    {
        createComponents("testSubscribeForRpcAfterRpcIsCreatedAndDestroyed");
        assertNull(this.candidate.getRpc("getTime"));

        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                return LongValue.valueOf(System.currentTimeMillis());
            }
        }, TypeEnum.LONG, "getTime");
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);
        assertNotNull(this.candidate.getRpc("getTime"));

        this.context.removeRpc("getTime");
        waitForRpcToBeRemoved("getTime");

        assertNull(this.candidate.getRpc("getTime"));
    }

    void waitForRpcToBePublished(final IRpcInstance rpc) throws InterruptedException
    {
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(observer, IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
        waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                IRecord latestImage = observer.getLatestImage();
                if (latestImage == null)
                {
                    return -1;
                }
                return latestImage.keySet().contains(rpc.getName());
            }

            @Override
            public Object expect()
            {
                return true;
            }
        });
    }

    void waitForRpcToBeRemoved(final String rpcName) throws InterruptedException
    {
        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        this.candidate.addObserver(observer, IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
        waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                IRecord latestImage = observer.getLatestImage();
                if (latestImage == null)
                {
                    return -1;
                }
                return !latestImage.keySet().contains(rpcName);
            }

            @Override
            public Object expect()
            {
                return true;
            }
        });
    }

    @Test
    public void testRpcNoArgs() throws TimeOutException, ExecutionException, InterruptedException, IOException
    {
        createComponents("testRpcNoArgs");
        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                return LongValue.valueOf(System.currentTimeMillis());
            }
        }, TypeEnum.LONG, "getTime");
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);

        long then = System.currentTimeMillis();
        IValue result = this.candidate.getRpc("getTime").execute();
        long now = System.currentTimeMillis();
        assertTrue(result.longValue() >= then);
        assertTrue(result.longValue() <= now);
    }

    @Test
    public void testRpcNoArgsNoResponse() throws TimeOutException, ExecutionException, InterruptedException,
        IOException
    {
        createComponents("testRpcNoArgs");
        final CountDownLatch latch = new CountDownLatch(1);
        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                latch.countDown();
                return LongValue.valueOf(System.currentTimeMillis());
            }
        }, TypeEnum.LONG, "getTime");
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);

        long then = System.currentTimeMillis();
        this.candidate.getRpc("getTime").executeNoResponse();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRpcWithSimpleArgs() throws TimeOutException, ExecutionException, InterruptedException, IOException
    {
        createComponents("testRpcWithSimpleArgs");
        this.executor.shutdownNow();
        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                StringBuilder sb = new StringBuilder();
                for (IValue iValue : args)
                {
                    sb.append(iValue.textValue()).append(",");
                }
                return new TextValue(sb.toString());
            }
        }, TypeEnum.TEXT, "concat", TypeEnum.TEXT, TypeEnum.DOUBLE, TypeEnum.LONG, TypeEnum.TEXT);
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);

        IValue result =
            this.candidate.getRpc("concat").execute(new TextValue("someValue1"), new DoubleValue(Double.NaN),
                LongValue.valueOf(2345), new TextValue("anotherText value here!"));
        assertEquals("someValue1,NaN,2345,anotherText value here!,", result.textValue());
    }

    @Test
    public void testMultithreadRpcWithSimpleArgs() throws TimeOutException, ExecutionException, InterruptedException,
        IOException
    {
        createComponents("testMultithreadRpcWithSimpleArgs");
        this.executor.shutdownNow();
        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                StringBuilder sb = new StringBuilder();
                for (IValue iValue : args)
                {
                    sb.append(iValue.textValue()).append(",");
                }
                return new TextValue(sb.toString());
            }
        }, TypeEnum.TEXT, "concat", TypeEnum.TEXT, TypeEnum.DOUBLE, TypeEnum.LONG, TypeEnum.TEXT);
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);

        int count = 50;
        final CountDownLatch completed = new CountDownLatch(count);
        final CyclicBarrier barrier = new CyclicBarrier(count);
        final List<String> errors = new ArrayList<String>();
        for (int i = 0; i < count; i++)
        {
            new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    final String random = "randomValue:" + System.nanoTime();
                    final IRpcInstance concatRpc = ProxyContextTest.this.candidate.getRpc("concat");

                    // wait for all threads to be ready...
                    try
                    {
                        barrier.await();
                    }
                    catch (Exception e)
                    {
                    }
                    try
                    {
                        IValue result =
                            concatRpc.execute(new TextValue(random), new DoubleValue(Double.NaN),
                                LongValue.valueOf(2345), new TextValue("anotherText value here!"));
                        final String expected = random + ",NaN,2345,anotherText value here!,";
                        if (!result.textValue().equals(expected))
                        {
                            errors.add("Expected: " + expected + ", but got: " + result.textValue());
                        }
                        completed.countDown();
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        assertTrue(completed.await(TIMEOUT, TimeUnit.SECONDS));
        assertEquals("Got errors: " + errors, 0, errors.size());
    }

    @Test
    public void testRpcWithSimpleArgsNoLogging() throws TimeOutException, ExecutionException, InterruptedException,
        IOException
    {
        createComponents("testRpcWithSimpleArgsNoLogging");
        this.executor.shutdownNow();
        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                StringBuilder sb = new StringBuilder();
                for (IValue iValue : args)
                {
                    sb.append(iValue.textValue()).append(",");
                }
                return new TextValue(sb.toString());
            }
        }, TypeEnum.TEXT, "concat2", TypeEnum.TEXT, TypeEnum.DOUBLE, TypeEnum.LONG, TypeEnum.TEXT);
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);

        IValue result =
            this.candidate.getRpc("concat2").execute(new TextValue("someValue1"), new DoubleValue(Double.NaN),
                LongValue.valueOf(2345), new TextValue("anotherText value here!"));
        assertEquals("someValue1,NaN,2345,anotherText value here!,", result.textValue());
        Log.banner(this, "There should be no occurrences of rpc|concat2");
    }

    @Test
    public void testRpcWithSimpleArgs_noResponse() throws TimeOutException, ExecutionException, InterruptedException,
        IOException
    {
        createComponents("testRpcWithSimpleArgs");
        this.executor.shutdownNow();
        final StringBuilder sb = new StringBuilder();
        final CountDownLatch latch = new CountDownLatch(1);
        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                for (IValue iValue : args)
                {
                    sb.append(iValue.textValue()).append(",");
                }
                latch.countDown();
                return new TextValue(sb.toString());
            }
        }, TypeEnum.TEXT, "concat", TypeEnum.TEXT, TypeEnum.DOUBLE, TypeEnum.LONG, TypeEnum.TEXT);
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);

        this.candidate.getRpc("concat").executeNoResponse(new TextValue("someValue1"), new DoubleValue(Double.NaN),
            LongValue.valueOf(2345), new TextValue("anotherText value here!"));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals("someValue1,NaN,2345,anotherText value here!,", sb.toString());
    }

    @Test
    public void testRpcReturnsNull() throws TimeOutException, ExecutionException, InterruptedException, IOException
    {
        createComponents("testRpcReturnsNull");
        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                return null;
            }
        }, TypeEnum.TEXT, "getNull");
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);

        IValue result = this.candidate.getRpc("getNull").execute();
        assertNull(result);
    }

    @Test
    public void testRpcWithSpecialCharsInArgs() throws TimeOutException, ExecutionException, InterruptedException,
        IOException
    {
        createComponents("testRpcWithSpecialCharsInArgs");
        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                StringBuilder sb = new StringBuilder();
                for (IValue iValue : args)
                {
                    sb.append(iValue.textValue()).append(",");
                }
                return new TextValue(sb.toString());
            }
        }, TypeEnum.TEXT, "concat", TypeEnum.TEXT, TypeEnum.DOUBLE, TypeEnum.LONG, TypeEnum.TEXT);
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);

        IValue result =
            this.candidate.getRpc("concat").execute(new TextValue("textValue1|\\="), new DoubleValue(Double.NaN),
                LongValue.valueOf(2345), new TextValue("anotherText value here!|+"));
        assertEquals("textValue1|\\=,NaN,2345,anotherText value here!|+,", result.textValue());
    }

    @Test
    public void testRpcExecutionException() throws InterruptedException, TimeOutException, IOException
    {
        createComponents("testRpcExecutionException");
        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                throw new RuntimeException("Something bad happened!");
            }
        }, TypeEnum.TEXT, "rpcException");
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);

        try
        {
            IValue result = this.candidate.getRpc("rpcException").execute();
            fail("should throw ExecutionException");
        }
        catch (ExecutionException e)
        {
        }
    }

    @Test
    public void testRpcTimeOut() throws ExecutionException, InterruptedException, IOException
    {
        createComponents("testRpcTimeOut");
        RpcInstance rpc = new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                try
                {
                    Thread.sleep(500);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                return null;
            }
        }, TypeEnum.TEXT, "wait");
        this.context.createRpc(rpc);

        waitForRpcToBePublished(rpc);

        try
        {
            IRpcInstance rpc2 = this.candidate.getRpc("wait");
            rpc2.setRemoteExecutionDurationTimeoutMillis(100);
            rpc2.execute();
            fail("Should throw TimeOutException");
        }
        catch (TimeOutException e)
        {
        }
    }

    @Test
    public void testPublisherBounced() throws Exception
    {
        createComponents("testPublisherBounced");
        final String rpcName = "RPC_FOR_testPublisherBounced";
        this.context.createRpc(new RpcInstance(TypeEnum.TEXT, rpcName));

        this.candidate.setReconnectPeriodMillis(100);
        TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        observer.latch = new CountDownLatch(UPDATE_COUNT);
        this.candidate.addObserver(observer, record1);
        final int timeout = TIMEOUT;
        assertTrue(observer.latch.await(timeout, TimeUnit.SECONDS));

        TestCachingAtomicChangeObserver statusObserver = new TestCachingAtomicChangeObserver();
        statusObserver.latch = new CountDownLatch(1);
        this.candidate.addObserver(statusObserver, ProxyContext.RECORD_CONNECTION_STATUS_NAME);
        assertTrue(statusObserver.latch.await(timeout, TimeUnit.SECONDS));
        Log.log(this, ">>>>testPublisherBounced " + statusObserver.getLatestImage());
        checkRecordConnected(statusObserver, record1);
        statusObserver.latch = new CountDownLatch(1);

        TestCachingAtomicChangeObserver rpcObserver = new TestCachingAtomicChangeObserver();
        rpcObserver.latch = new CountDownLatch(1);
        this.candidate.addObserver(rpcObserver, ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
        assertTrue(rpcObserver.latch.await(timeout, TimeUnit.SECONDS));
        int i = 0;
        while (rpcObserver.getLatestImage().get(rpcName) == null && (i++ < 200))
            Thread.sleep(20);
        assertNotNull(rpcObserver.getLatestImage().get(rpcName));
        rpcObserver.latch = new CountDownLatch(1);

        this.publisher.destroy();

        // wait for disconnection
        assertTrue(statusObserver.latch.await(timeout, TimeUnit.SECONDS));
        Log.log(this, ">>>>testPublisherBounced " + statusObserver.getLatestImage());
        // todo this can be CONNECTING, not DISCONNECTED
        checkRecordDisconnected(statusObserver, record1);

        // check RPC removed
        assertTrue("RPC should be removed and we should be triggered",
            rpcObserver.latch.await(timeout, TimeUnit.SECONDS));

        // need to do this style of checking for linux...its too fast :)
        waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                return ProxyContextTest.this.candidate.getRpc(rpcName);
            }

            @Override
            public Object expect()
            {
                return null;
            }
        });

        rpcObserver.latch = new CountDownLatch(1);

        observer.latch = new CountDownLatch(UPDATE_COUNT);
        statusObserver.latch = new CountDownLatch(1);

        // need to try until the server socket is dead
        while (true)
        {
            Thread.sleep(10);
            try
            {
                this.publisher = new Publisher(this.context, getProtocolCodec(), LOCALHOST, this.PORT);
                break;
            }
            catch (Exception e)
            {
                Log.log(this, ">>>testPublisherBounced previous server socket still active: " + e);
                continue;
            }
        }

        assertTrue(statusObserver.latch.await(timeout, TimeUnit.SECONDS));
        Log.log(this, ">>>>testPublisherBounced " + statusObserver.getLatestImage());
        checkRecordConnected(statusObserver, record1);
        assertTrue(observer.latch.await(timeout, TimeUnit.SECONDS));

        assertTrue(rpcObserver.latch.await(timeout, TimeUnit.SECONDS));
        i = 0;
        while (rpcObserver.getLatestImage().get(rpcName) == null && (i++ < 200))
            Thread.sleep(20);
        assertNotNull(rpcObserver.getLatestImage().get(rpcName));
    }

    @Test
    public void testPublisherConnectionChanged() throws Exception
    {
        createComponents("testPublisherConnectionChanged");
        TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        observer.latch = new CountDownLatch(UPDATE_COUNT);
        this.candidate.addObserver(observer, record1);
        final int timeout = TIMEOUT;
        assertTrue(observer.latch.await(timeout, TimeUnit.SECONDS));
        this.PORT = getNextFreePort();

        TestCachingAtomicChangeObserver statusObserver = new TestCachingAtomicChangeObserver();
        statusObserver.latch = new CountDownLatch(1);
        this.candidate.addObserver(statusObserver, ProxyContext.RECORD_CONNECTION_STATUS_NAME);
        assertTrue(statusObserver.latch.await(timeout, TimeUnit.SECONDS));
        Log.log(this, ">>>>testPublisherConnectionChanged " + statusObserver.getLatestImage());
        checkRecordConnected(statusObserver, record1);
        statusObserver.latch = new CountDownLatch(1);

        this.publisher.destroy();

        // wait for disconnection
        assertTrue(statusObserver.latch.await(timeout, TimeUnit.SECONDS));
        Log.log(this, ">>>>testPublisherConnectionChanged " + statusObserver.getLatestImage());
        checkRecordDisconnected(statusObserver, record1);

        observer.latch = new CountDownLatch(UPDATE_COUNT);
        statusObserver.latch = new CountDownLatch(1);

        this.publisher = new Publisher(this.context, getProtocolCodec(), LOCALHOST, this.PORT);
        this.candidate.reconnect(LOCALHOST, this.PORT);

        assertTrue(statusObserver.latch.await(timeout, TimeUnit.SECONDS));
        Log.log(this, ">>>>testPublisherConnectionChanged " + statusObserver.getLatestImage());
        checkRecordConnected(statusObserver, record1);
        assertTrue(observer.latch.await(timeout, TimeUnit.SECONDS));
    }

    private static void checkRecordDisconnected(final TestCachingAtomicChangeObserver statusObserver,
        final String recordName) throws InterruptedException
    {
        waitForEvent(new EventChecker()
        {
            @Override
            public Object expect()
            {
                return ProxyContext.RECORD_DISCONNECTED;
            }

            @Override
            public Object got()
            {
                return statusObserver.getLatestImage().get(recordName);
            }
        });
    }

    private static void checkRecordConnected(final TestCachingAtomicChangeObserver statusObserver,
        final String recordName) throws InterruptedException
    {
        waitForEvent(new EventChecker()
        {
            @Override
            public Object expect()
            {
                return ProxyContext.RECORD_CONNECTED;
            }

            @Override
            public Object got()
            {
                return statusObserver.getLatestImage().get(recordName);
            }
        });
    }

    private static void awaitLatch(CountDownLatch record1Latch) throws InterruptedException
    {
        for (int i = 0; i < KEY_COUNT; i++)
        {
            assertTrue("Latch " + i + ", trigger count remaining: " + record1Latch.getCount(),
                record1Latch.await(UPDATE_COUNT * ATOMIC_CHANGE_PERIOD_MILLIS * 20, TimeUnit.MILLISECONDS));
        }
    }

    private static TestLongValueSequenceCheckingAtomicChangeObserver registerObserverForMap(ProxyContext remote,
        String recordName, CountDownLatch latch)
    {
        TestLongValueSequenceCheckingAtomicChangeObserver observer =
            new TestLongValueSequenceCheckingAtomicChangeObserver();
        observer.latch = latch;
        remote.addObserver(observer, recordName);
        observers.add(observer);
        return observer;
    }

    @Test
    public void testActive() throws Exception
    {
        createComponents("testActive");
        assertTrue(this.candidate.isActive());
        this.candidate.destroy();
        assertFalse(this.candidate.isActive());
    }

    @Test
    public void testSimpleRecordName() throws Exception
    {
        createComponents("testSimpleRecordName");
        final String simpleRecord = "sdflasers";
        final IRecord record = this.context.createRecord(simpleRecord);
        record.put(simpleRecord, new TextValue(simpleRecord));
        this.context.publishAtomicChange(record);
        final AtomicReference<Map<String, IValue>> result = new AtomicReference<Map<String, IValue>>();
        final CountDownLatch latch = new CountDownLatch(1);
        this.candidate.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                if (imageCopy.size() > 0)
                {
                    result.set(imageCopy);
                    latch.countDown();
                }
            }
        }, simpleRecord);
        final int timeout = TIMEOUT;
        assertTrue(latch.await(timeout, TimeUnit.SECONDS));

        assertEquals(1, result.get().size());
        assertEquals(simpleRecord, result.get().get(simpleRecord).textValue());
    }

    @Test
    public void testRecordNameWithSpecialChars() throws Exception
    {
        createComponents("testRecordNameWithSpecialChars");
        final String specialChars = "some value \\|| with \r\n | delimiters \\/ |\\ |/ p|sdf|";
        final IRecord record = this.context.createRecord(specialChars);
        record.put(specialChars, new TextValue(specialChars));
        this.context.publishAtomicChange(record);
        final AtomicReference<Map<String, IValue>> result = new AtomicReference<Map<String, IValue>>();
        final CountDownLatch latch = new CountDownLatch(1);
        this.candidate.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                if (imageCopy.size() > 0)
                {
                    result.set(imageCopy);
                    latch.countDown();
                }
            }
        }, specialChars);
        final int timeout = TIMEOUT;
        assertTrue(latch.await(timeout, TimeUnit.SECONDS));

        assertEquals(1, result.get().size());
        assertEquals(specialChars, result.get().get(specialChars).textValue());
    }

    @Test
    public void testCannotRemoveRemoteRecords() throws Exception
    {
        createComponents("testCannotRemoveRemoteRecords");
        doAddThenRemoveObservers(this.candidate, IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
        doAddThenRemoveObservers(this.candidate, IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS);
        doAddThenRemoveObservers(this.candidate, IRemoteSystemRecordNames.REMOTE_CONTEXT_RECORDS);
        doAddThenRemoveObservers(this.candidate, IRemoteSystemRecordNames.REMOTE_CONTEXT_SUBSCRIPTIONS);
        doAddThenRemoveObservers(this.candidate, ProxyContext.RECORD_CONNECTION_STATUS_NAME);
    }

    private static void doAddThenRemoveObservers(ProxyContext candidate, final String recordName)
    {
        candidate.context.getOrCreateRecord(recordName);

        TestLongValueSequenceCheckingAtomicChangeObserver observer =
            new TestLongValueSequenceCheckingAtomicChangeObserver();
        candidate.addObserver(observer, recordName);
        assertNotNull(candidate.getRecord(recordName));

        TestLongValueSequenceCheckingAtomicChangeObserver observer2 =
            new TestLongValueSequenceCheckingAtomicChangeObserver();
        candidate.addObserver(observer2, recordName);
        assertNotNull(candidate.getRecord(recordName));

        candidate.removeObserver(observer, recordName);
        assertNotNull(candidate.getRecord(recordName));

        candidate.removeObserver(observer2, recordName);
        assertNotNull(candidate.getRecord(recordName));
    }
}
