/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
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
package com.fimtra.clearconnect.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.event.IRecordAvailableListener;
import com.fimtra.clearconnect.event.IRecordSubscriptionListener;
import com.fimtra.clearconnect.event.IRpcAvailableListener;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.util.ThreadUtils;

/**
 * Tests for the {@link PlatformServiceInstance}
 * 
 * @author Ramon Servadei
 */
public class PlatformServiceTest
{
    private static final String RPC2 = "rpc2";
    private static final String RPC1 = "rpc1";
    static int PORT = 32000;
    final static String hostName = "localhost";
    final static String record1 = "record1";
    final static String record2 = "record2";
    final static String record3 = "record3";
    PlatformServiceInstance candidate;

    static void waitForContextSubscriptionsToUpdate() throws InterruptedException
    {
        // wait for the CONTEXT_SUBSCRIPTIONS to update with the count of subscribers
        Thread.sleep(100);
    }

    @Before
    public void setUp()
    {
        PORT += 1;
        this.candidate =
            new PlatformServiceInstance(null, "TestPlatformService", "PRIMARY", WireProtocolEnum.STRING,
                RedundancyModeEnum.FAULT_TOLERANT, hostName, PORT, null, null, null,
                TransportTechnologyEnum.getDefaultFromSystemProperty());
    }

    @After
    public void tearDown()
    {
        ThreadUtils.newThread(new Runnable()
        {
            @Override
            public void run()
            {
                PlatformServiceTest.this.candidate.destroy();
            }
        }, "tearDown").start();
    }

    @Test
    public void testGetAllSubscriptions() throws InterruptedException
    {
        waitForContextSubscriptionsToUpdate();

        assertEquals(4, this.candidate.getAllSubscriptions().size());

        IRecordListener changeListener = mock(IRecordListener.class);
        this.candidate.addRecordListener(changeListener, record1);

        waitForContextSubscriptionsToUpdate();

        assertEquals(5, this.candidate.getAllSubscriptions().size());
        assertEquals(1, this.candidate.getAllSubscriptions().get(record1).getCurrentSubscriberCount());
        assertEquals(0, this.candidate.getAllSubscriptions().get(record1).getPreviousSubscriberCount());

        this.candidate.removeRecordListener(changeListener, record1);

        waitForContextSubscriptionsToUpdate();

        assertEquals(4, this.candidate.getAllSubscriptions().size());
        assertNull(this.candidate.getAllSubscriptions().get(record1));
    }

    @Test
    public void testAddAndRemoveRecordAvailableListener() throws InterruptedException
    {
        assertTrue(this.candidate.createRecord(record1));
        assertFalse(this.candidate.createRecord(record1));

        final AtomicReference<String> expected = new AtomicReference<String>(record1);
        final AtomicReference<CountDownLatch> latch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> unavailableLatch =
            new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        IRecordAvailableListener recordListener1 = new IRecordAvailableListener()
        {
            @Override
            public void onRecordUnavailable(String recordName)
            {
                if (expected.get().equals(recordName))
                {
                    unavailableLatch.get().countDown();
                }
            }

            @Override
            public void onRecordAvailable(String recordName)
            {
                if (expected.get().equals(recordName))
                {
                    latch.get().countDown();
                }
            }
        };
        assertTrue(this.candidate.addRecordAvailableListener(recordListener1));
        assertFalse(this.candidate.addRecordAvailableListener(recordListener1));
        assertTrue(latch.get().await(1, TimeUnit.SECONDS));

        assertTrue(this.candidate.deleteRecord(this.candidate.getRecord(record1)));
        assertTrue(unavailableLatch.get().await(1, TimeUnit.SECONDS));

        // add a second listener
        final AtomicReference<CountDownLatch> latch2 = new AtomicReference<CountDownLatch>(new CountDownLatch(5));
        final AtomicReference<CountDownLatch> unavailableLatch2 =
            new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        IRecordAvailableListener recordListener2 = new IRecordAvailableListener()
        {
            @Override
            public void onRecordUnavailable(String recordName)
            {
                if (expected.get().equals(recordName))
                {
                    unavailableLatch2.get().countDown();
                }
            }

            @Override
            public void onRecordAvailable(String recordName)
            {
                if (expected.get().equals(recordName))
                {
                    latch2.get().countDown();
                }
            }
        };
        assertTrue(this.candidate.addRecordAvailableListener(recordListener2));
        assertTrue(latch.get().await(1, TimeUnit.SECONDS));

        // create a record and test it is picked up by both
        expected.set(record2);
        latch.set(new CountDownLatch(1));
        latch2.set(new CountDownLatch(1));
        this.candidate.createRecord(record2);

        assertTrue(latch.get().await(1, TimeUnit.SECONDS));
        assertTrue(latch2.get().await(1, TimeUnit.SECONDS));

        // now test removing a listener
        assertTrue(this.candidate.removeRecordAvailableListener(recordListener2));
        assertFalse(this.candidate.removeRecordAvailableListener(recordListener2));

        unavailableLatch.set(new CountDownLatch(1));
        unavailableLatch2.set(new CountDownLatch(1));
        assertTrue(this.candidate.deleteRecord(this.candidate.getRecord(record2)));

        assertTrue(unavailableLatch.get().await(1, TimeUnit.SECONDS));
        assertFalse("Should not be triggered", unavailableLatch2.get().await(500, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testGetAllRecordNames()
    {
        assertEquals(6, this.candidate.getAllRecordNames().size());
        assertTrue(this.candidate.createRecord(record1));
        assertEquals(7, this.candidate.getAllRecordNames().size());
        assertTrue(this.candidate.createRecord(record2));
        assertEquals(8, this.candidate.getAllRecordNames().size());
    }

    @Test
    public void testGetAllRpcs()
    {
        assertEquals(1, this.candidate.getAllRpcs().size());
        IRpcInstance rpc1 = new RpcInstance(TypeEnum.TEXT, RPC1);
        assertTrue(this.candidate.publishRPC(rpc1));
        assertEquals(2, this.candidate.getAllRpcs().size());
        assertEquals(rpc1, this.candidate.getAllRpcs().get(RPC1));
    }
    
    @Test
    public void testAddRpcAvailableListener()
    {
        IRpcInstance rpc1 = new RpcInstance(TypeEnum.TEXT, RPC1);
        assertTrue(this.candidate.publishRPC(rpc1));
        assertFalse(this.candidate.publishRPC(rpc1));

        IRpcAvailableListener rpcListener1 = mock(IRpcAvailableListener.class);
        assertTrue(this.candidate.addRpcAvailableListener(rpcListener1));
        assertFalse(this.candidate.addRpcAvailableListener(rpcListener1));
        verify(rpcListener1, timeout(500).times(2)).onRpcAvailable(any(IRpcInstance.class));
        verify(rpcListener1).onRpcAvailable(eq(rpc1));
        reset(rpcListener1);

        assertTrue(this.candidate.unpublishRPC(rpc1));
        assertFalse(this.candidate.unpublishRPC(rpc1));
        verify(rpcListener1, timeout(500)).onRpcUnavailable(eq(rpc1));
        reset(rpcListener1);

        assertTrue(this.candidate.publishRPC(rpc1));
        verify(rpcListener1, timeout(500)).onRpcAvailable(eq(rpc1));
        reset(rpcListener1);

        IRpcAvailableListener rpcListener2 = mock(IRpcAvailableListener.class);
        assertTrue(this.candidate.addRpcAvailableListener(rpcListener2));
        verify(rpcListener2, timeout(500).times(2)).onRpcAvailable(any(IRpcInstance.class));
        verify(rpcListener2).onRpcAvailable(eq(rpc1));
        reset(rpcListener2);

        IRpcInstance rpc2 = new RpcInstance(TypeEnum.TEXT, RPC2);
        this.candidate.publishRPC(rpc2);

        verify(rpcListener1, timeout(500)).onRpcAvailable(eq(rpc2));
        verify(rpcListener2, timeout(500)).onRpcAvailable(eq(rpc2));
    }

    @Test
    public void testCreateRecord()
    {
        assertTrue(this.candidate.createRecord(record1));
        assertFalse(this.candidate.createRecord(record1));
    }

    @Test
    public void testGetRecord()
    {
        assertNull(this.candidate.getRecord(record1));
        assertTrue(this.candidate.createRecord(record1));
        assertNotNull(this.candidate.getRecord(record1));
    }
    
    @Test
    public void testUpdateRecord() throws InterruptedException
    {
        IRecordListener listener = mock(IRecordListener.class);
        assertTrue(this.candidate.createRecord(record1));
        IRecord record = this.candidate.getRecord(record1);

        this.candidate.addRecordListener(listener, record1);
        verify(listener, timeout(1000).times(1)).onChange(any(IRecord.class), any(IRecordChange.class));
        
        record.put("key1", TextValue.valueOf("value1"));
        assertTrue(this.candidate.publishRecord(record).await(1, TimeUnit.SECONDS));

        
        record.put("key2", TextValue.valueOf("value1"));
        record.put("key1", TextValue.valueOf("value1"));
        assertTrue(this.candidate.publishRecord(record).await(1, TimeUnit.SECONDS));
        
        // this is not a change
        record.put("key1", TextValue.valueOf("value1"));
        assertTrue(this.candidate.publishRecord(record).await(1, TimeUnit.SECONDS));
        verify(listener, times(3)).onChange(any(IRecord.class), any(IRecordChange.class));
    }

    @Test
    public void testPublishMergeRecord()
    {
        IRecordListener listener = mock(IRecordListener.class);
        assertTrue(this.candidate.createRecord(record1));
        IRecord record = this.candidate.getRecord(record1);

        this.candidate.addRecordListener(listener, record1);
        verify(listener, timeout(1000).times(1)).onChange(any(IRecord.class), any(IRecordChange.class));

        record.put("key1", TextValue.valueOf("value1"));
        this.candidate.publishMergeRecord(record);

        record.put("key2", TextValue.valueOf("value1"));
        record.put("key1", TextValue.valueOf("value1"));
        this.candidate.publishMergeRecord(record);
        
        record.put("key2", TextValue.valueOf("value2"));
        this.candidate.publishMergeRecord(record);

        record.put("key2", TextValue.valueOf("value3"));
        this.candidate.publishMergeRecord(record);

        // this is not a change
        record.put("key1", TextValue.valueOf("value1"));
        this.candidate.publishMergeRecord(record);
        
        verify(listener, atMost(4)).onChange(any(IRecord.class), any(IRecordChange.class));
    }

    @Test
    public void testDeleteRecord()
    {
        assertTrue(this.candidate.createRecord(record1));
        IRecord record = this.candidate.getRecord(record1);
        assertTrue(this.candidate.deleteRecord(record));
        assertFalse(this.candidate.deleteRecord(record));
    }

    @Test
    public void testPublishRPC()
    {
        IRpcInstance rpc1 = new RpcInstance(TypeEnum.TEXT, RPC1);
        assertTrue(this.candidate.publishRPC(rpc1));
        assertFalse(this.candidate.publishRPC(rpc1));
    }

    @Test
    public void testUnpublishRPC()
    {
        IRpcInstance rpc1 = new RpcInstance(TypeEnum.TEXT, RPC1);
        assertTrue(this.candidate.publishRPC(rpc1));
        assertTrue(this.candidate.unpublishRPC(rpc1));
        assertFalse(this.candidate.unpublishRPC(rpc1));
    }

    @Test
    public void testIsActive()
    {
        assertTrue(this.candidate.isActive());
        this.candidate.destroy();
        assertFalse(this.candidate.isActive());
    }

    @Test
    public void testAddAndRemoveRecordSubscriptionListener() throws InterruptedException
    {
        assertTrue(this.candidate.createRecord(record1));
        assertTrue(this.candidate.createRecord(record2));

        IRecordListener changeListener = mock(IRecordListener.class);
        this.candidate.addRecordListener(changeListener, record1);

        final AtomicReference<String> expect = new AtomicReference<String>();
        final AtomicReference<CountDownLatch> latch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> noMoreListenersLatch =
            new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        IRecordSubscriptionListener listener = new IRecordSubscriptionListener()
        {
            @Override
            public void onRecordSubscriptionChange(SubscriptionInfo subscriptionInfo)
            {
                if (expect.get().equals(subscriptionInfo.getRecordName()))
                {
                    if (subscriptionInfo.getCurrentSubscriberCount() > 0)
                    {
                        latch.get().countDown();
                    }
                    else
                    {
                        if (subscriptionInfo.getCurrentSubscriberCount() == 0
                            && subscriptionInfo.getPreviousSubscriberCount() != 0)
                        {
                            noMoreListenersLatch.get().countDown();
                        }
                    }
                }
            }
        };
        expect.set(record1);

        assertTrue(this.candidate.addRecordSubscriptionListener(listener));
        assertFalse(this.candidate.addRecordSubscriptionListener(listener));

        // add the second listener (which we will use as the remove test)
        final AtomicReference<CountDownLatch> latch2 = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> noMoreListenersLatch2 =
            new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        IRecordSubscriptionListener listener2 = new IRecordSubscriptionListener()
        {
            @Override
            public void onRecordSubscriptionChange(SubscriptionInfo subscriptionInfo)
            {
                if (expect.get().equals(subscriptionInfo.getRecordName()))
                {
                    if (subscriptionInfo.getCurrentSubscriberCount() > 0)
                    {
                        latch2.get().countDown();
                    }
                    else
                    {
                        if (subscriptionInfo.getCurrentSubscriberCount() == 0
                            && subscriptionInfo.getPreviousSubscriberCount() != 0)
                        {
                            noMoreListenersLatch2.get().countDown();
                        }
                    }
                }
            }
        };
        assertTrue(this.candidate.addRecordSubscriptionListener(listener2));

        assertTrue(latch.get().await(1, TimeUnit.SECONDS));
        assertTrue(latch2.get().await(1, TimeUnit.SECONDS));

        assertTrue(this.candidate.removeRecordSubscriptionListener(listener2));
        assertFalse(this.candidate.removeRecordSubscriptionListener(listener2));

        // add a second listener for record1
        latch.set(new CountDownLatch(1));
        IRecordListener changeListener2 = mock(IRecordListener.class);
        this.candidate.addRecordListener(changeListener2, record1);

        waitForContextSubscriptionsToUpdate();

        this.candidate.removeRecordListener(changeListener2, record1);
        assertFalse(noMoreListenersLatch.get().await(100, TimeUnit.MILLISECONDS));

        this.candidate.removeRecordListener(changeListener, record1);
        assertTrue(noMoreListenersLatch.get().await(1, TimeUnit.SECONDS));
        assertFalse(noMoreListenersLatch2.get().await(100, TimeUnit.MILLISECONDS));
    }
}
