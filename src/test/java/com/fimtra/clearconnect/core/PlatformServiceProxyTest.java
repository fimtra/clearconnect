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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.event.IRecordAvailableListener;
import com.fimtra.clearconnect.event.IRecordConnectionStatusListener;
import com.fimtra.clearconnect.event.IRecordSubscriptionListener;
import com.fimtra.clearconnect.event.IRecordSubscriptionListener.SubscriptionInfo;
import com.fimtra.clearconnect.event.IRpcAvailableListener;
import com.fimtra.clearconnect.event.IServiceConnectionStatusListener;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.datafission.core.StringProtocolCodec;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ThreadUtils;

/**
 * Tests for the {@link PlatformServiceProxy}
 * 
 * @author Ramon Servadei
 */
public class PlatformServiceProxyTest
{
    @Rule
    public TestName name = new TestName();

    private static final String RPC2 = "rpc2";
    private static final String RPC1 = "rpc1";
    static int PORT = 31000;
    static int registryPort = 31500;
    static String registryHost = TcpChannelUtils.LOCALHOST_IP;
    static String agentHost = TcpChannelUtils.LOCALHOST_IP;
    final static String hostName = "localhost";
    final static String record1 = "record1";
    final static String record2 = "record2";
    final static String record3 = "record3";
    PlatformServiceProxy candidate;
    PlatformServiceInstance service;
    PlatformRegistry registry;
    PlatformRegistryAgent agent;

    static void waitForContextSubscriptionsToUpdate() throws InterruptedException
    {
        // wait for the CONTEXT_SUBSCRIPTIONS to update with the count of subscribers
        Thread.sleep(100);
    }

    @Before
    public void setUp() throws Exception
    {
        System.err.println(this.name.getMethodName());
        registryPort++;
        ChannelUtils.WATCHDOG.configure(200, 10);
        this.registry = new PlatformRegistry(this.name.getMethodName(), registryHost, registryPort);
        this.agent = new PlatformRegistryAgent("Agent", registryHost, registryPort);
        PORT += 1;
        this.service =
            new PlatformServiceInstance(null, "TestPlatformService", "PRIMARY", WireProtocolEnum.STRING,
                RedundancyModeEnum.FAULT_TOLERANT, hostName, PORT, null, null, null,
                TransportTechnologyEnum.getDefaultFromSystemProperty());
        this.candidate =
            new PlatformServiceProxy(this.agent, "TestPlatformService", new StringProtocolCodec(), hostName, PORT,
                TransportTechnologyEnum.getDefaultFromSystemProperty());
        this.candidate.setReconnectPeriodMillis(200);
        this.agent.setRegistryReconnectPeriodMillis(200);
    }

    @After
    public void tearDown()
    {
        ThreadUtils.newThread(new Runnable()
        {
            @Override
            public void run()
            {
                PlatformServiceProxyTest.this.registry.destroy();
                PlatformServiceProxyTest.this.agent.destroy();
                PlatformServiceProxyTest.this.service.destroy();
                PlatformServiceProxyTest.this.candidate.destroy();
            }
        }, "tearDown").start();

        ChannelUtils.WATCHDOG.configure(5000);
    }

    @Test
    public void testServiceConnectionStatusListener()
    {
        IServiceConnectionStatusListener listener = mock(IServiceConnectionStatusListener.class);
        this.candidate.addServiceConnectionStatusListener(listener);
        verify(listener, timeout(1000).atLeastOnce()).onConnected(eq(this.service.getPlatformServiceFamily()), anyInt());
        this.service.destroy();
        verify(listener, timeout(1000).atLeastOnce()).onReconnecting(eq(this.service.getPlatformServiceFamily()),
            anyInt());
        verify(listener, timeout(1000).atLeastOnce()).onDisconnected(eq(this.service.getPlatformServiceFamily()),
            anyInt());
    }

    @Test
    public void testGetAllSubscriptions() throws InterruptedException
    {
        final Map<String, SubscriptionInfo> allSubscriptions = this.service.getAllSubscriptions();
        assertEquals("Got: " + allSubscriptions, 4, allSubscriptions.size());


        IRecordListener changeListener = mock(IRecordListener.class);
        this.service.addRecordListener(changeListener, record1);

        waitForContextSubscriptionsToUpdate();

        assertEquals(5, this.service.getAllSubscriptions().size());
        assertEquals(1, this.service.getAllSubscriptions().get(record1).getCurrentSubscriberCount());
        assertEquals(0, this.service.getAllSubscriptions().get(record1).getPreviousSubscriberCount());

        this.service.removeRecordListener(changeListener, record1);

        waitForContextSubscriptionsToUpdate();

        assertEquals(4, this.service.getAllSubscriptions().size());
        assertNull(this.service.getAllSubscriptions().get(record1));
    }

    @Test
    public void testAddAndRemoveRecordSubscriptionListener() throws InterruptedException
    {
        assertTrue(this.service.createRecord(record1));
        assertTrue(this.service.createRecord(record2));

        IRecordListener changeListener = mock(IRecordListener.class);
        this.service.addRecordListener(changeListener, record1);

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

        // add record1 listener in the proxy
        latch.set(new CountDownLatch(1));
        this.candidate.addRecordListener(changeListener, record1);

        waitForContextSubscriptionsToUpdate();

        // remove record1 listener in the service
        this.service.removeRecordListener(changeListener, record1);
        assertFalse(noMoreListenersLatch.get().await(100, TimeUnit.MILLISECONDS));

        this.candidate.removeRecordListener(changeListener, record1);
        assertTrue(noMoreListenersLatch.get().await(1, TimeUnit.SECONDS));
        assertFalse(noMoreListenersLatch2.get().await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testAddAndRemoveRecordAvailableListener() throws InterruptedException
    {
        assertTrue(this.service.createRecord(record1));
        assertFalse(this.service.createRecord(record1));
        this.candidate.addRecordListener(mock(IRecordListener.class), record1);

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

        assertTrue(this.service.deleteRecord(this.service.getRecord(record1)));
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

        // create a new record and test it is picked up by both
        expected.set(record2);
        latch.set(new CountDownLatch(1));
        latch2.set(new CountDownLatch(1));
        this.service.createRecord(record2);

        assertTrue(latch.get().await(1, TimeUnit.SECONDS));
        assertTrue(latch2.get().await(1, TimeUnit.SECONDS));

        // now test removing a listener
        assertTrue(this.candidate.removeRecordAvailableListener(recordListener2));
        assertFalse(this.candidate.removeRecordAvailableListener(recordListener2));

        unavailableLatch.set(new CountDownLatch(1));
        unavailableLatch2.set(new CountDownLatch(1));
        assertTrue(this.service.deleteRecord(this.service.getRecord(record2)));

        assertTrue(unavailableLatch.get().await(1, TimeUnit.SECONDS));
        assertFalse("Should not be triggered", unavailableLatch2.get().await(1000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testGetAllRecordNames() throws InterruptedException
    {
        try
        {
            Log.log(this, ">>>>>> START testGetAllRecordNames");
            final AtomicReference<CountDownLatch> latch = new AtomicReference<CountDownLatch>(new CountDownLatch(4));
            final List<String> records = new CopyOnWriteArrayList<String>();
            this.candidate.addRecordAvailableListener(new IRecordAvailableListener()
            {

                @Override
                public void onRecordUnavailable(String recordName)
                {
                }

                @Override
                public void onRecordAvailable(String recordName)
                {
                    records.add(recordName);
                    latch.get().countDown();
                }
            });
            assertTrue("GOT: " + records, latch.get().await(5, TimeUnit.SECONDS));
            waitForContextSubscriptionsToUpdate();
            assertEquals("GOT: " + records + ", candidate.getAllRecordNames()=" + this.candidate.getAllRecordNames(),
                6, this.candidate.getAllRecordNames().size());

            latch.set(new CountDownLatch(1));
            assertTrue(this.service.createRecord(record1));
            assertTrue(latch.get().await(1, TimeUnit.SECONDS));
            assertEquals(7, this.candidate.getAllRecordNames().size());

            latch.set(new CountDownLatch(1));
            assertTrue(this.service.createRecord(record2));
            assertTrue(latch.get().await(1, TimeUnit.SECONDS));
            assertEquals(8, this.candidate.getAllRecordNames().size());
        }
        finally
        {
            Log.log(this, ">>>>>> END testGetAllRecordNames");
        }
    }

    @Test
    public void testInvokeRpc() throws TimeOutException, ExecutionException
    {
        RpcInstance rpc1 = new RpcInstance(TypeEnum.TEXT, RPC1);
        final TextValue textValue = TextValue.valueOf("result");
        rpc1.setHandler(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                return textValue;
            }
        });
        assertTrue(this.service.publishRPC(rpc1));
        assertEquals(textValue, this.candidate.executeRpc(1000, RPC1));
    }

    @Test
    public void testGetAllRpcs()
    {
        // compensate for lazy loading
        IRpcAvailableListener rpcListener = mock(IRpcAvailableListener.class);
        this.candidate.addRpcAvailableListener(rpcListener);
        verify(rpcListener, timeout(1000).times(1)).onRpcAvailable(any(IRpcInstance.class));
        
        assertEquals(1, this.candidate.getAllRpcs().size());
        IRpcInstance rpc1 = new RpcInstance(TypeEnum.TEXT, RPC1);
        assertTrue(this.service.publishRPC(rpc1));

        rpcListener = mock(IRpcAvailableListener.class);
        this.candidate.addRpcAvailableListener(rpcListener);
        verify(rpcListener, timeout(1000).times(2)).onRpcAvailable(any(IRpcInstance.class));

        assertEquals(2, this.candidate.getAllRpcs().size());
        assertEquals(rpc1, this.candidate.getAllRpcs().get(RPC1));
    }

    @Test
    public void testAddAndRemoveRpcAvailableListener() throws InterruptedException
    {
        final int timeout = 5;
        IRpcInstance rpc1 = new RpcInstance(TypeEnum.TEXT, RPC1);
        assertTrue(this.service.publishRPC(rpc1));
        assertFalse(this.service.publishRPC(rpc1));

        final AtomicReference<CountDownLatch> latch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> unavailableLatch =
            new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        IRpcAvailableListener rpcListener1 = new IRpcAvailableListener()
        {
            @Override
            public void onRpcUnavailable(IRpcInstance rpc)
            {
                if (rpc.getName().startsWith("rpc"))
                {
                    unavailableLatch.get().countDown();
                }
            }

            @Override
            public void onRpcAvailable(IRpcInstance rpc)
            {
                if (rpc.getName().startsWith("rpc"))
                {
                    latch.get().countDown();
                }
            }
        };
        assertTrue(this.candidate.addRpcAvailableListener(rpcListener1));
        assertFalse(this.candidate.addRpcAvailableListener(rpcListener1));
        assertTrue(latch.get().await(timeout, TimeUnit.SECONDS));

        // unpublish the RPC
        assertTrue(this.service.unpublishRPC(rpc1));
        assertFalse(this.service.unpublishRPC(rpc1));

        // check we are told
        assertTrue(unavailableLatch.get().await(timeout, TimeUnit.SECONDS));

        // republish it
        assertTrue(this.service.publishRPC(rpc1));

        // add a second listener
        final AtomicReference<CountDownLatch> latch2 = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> unavailableLatch2 =
            new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        IRpcAvailableListener rpcListener2 = new IRpcAvailableListener()
        {
            @Override
            public void onRpcUnavailable(IRpcInstance rpc)
            {
                if (rpc.getName().startsWith("rpc"))
                {
                    unavailableLatch2.get().countDown();
                }
            }

            @Override
            public void onRpcAvailable(IRpcInstance rpc)
            {
                if (rpc.getName().startsWith("rpc"))
                {
                    latch2.get().countDown();
                }
            }
        };
        assertTrue(this.candidate.addRpcAvailableListener(rpcListener2));
        assertTrue(latch2.get().await(timeout, TimeUnit.SECONDS));

        latch.set(new CountDownLatch(1));
        latch2.set(new CountDownLatch(1));
        IRpcInstance rpc2 = new RpcInstance(TypeEnum.TEXT, RPC2);
        assertTrue(this.service.publishRPC(rpc2));

        assertTrue(latch.get().await(timeout, TimeUnit.SECONDS));
        assertTrue(latch2.get().await(timeout, TimeUnit.SECONDS));

        // now remove on of the listeners
        assertTrue(this.candidate.removeRpcAvailableListener(rpcListener2));
        assertFalse(this.candidate.removeRpcAvailableListener(rpcListener2));

        unavailableLatch.set(new CountDownLatch(1));
        unavailableLatch2.set(new CountDownLatch(1));
        assertTrue(this.service.unpublishRPC(rpc2));
        assertTrue(unavailableLatch.get().await(timeout, TimeUnit.SECONDS));
        assertFalse("Should not be triggered", unavailableLatch2.get().await(1000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testUpdateRecord() throws InterruptedException
    {
        final AtomicReference<CountDownLatch> latch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        IRecordListener listener = new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                latch.get().countDown();
            }
        };
        this.candidate.addRecordListener(listener, record1);
        assertTrue(this.service.createRecord(record1));
        assertTrue(latch.get().await(1, TimeUnit.SECONDS));

        latch.set(new CountDownLatch(2));
        IRecord record = this.service.getRecord(record1);

        record.put("key1", TextValue.valueOf("value1"));
        assertTrue(this.service.publishRecord(record).await(1, TimeUnit.SECONDS));

        record.put("key2", TextValue.valueOf("value1"));
        record.put("key1", TextValue.valueOf("value1"));
        assertTrue(this.service.publishRecord(record).await(1, TimeUnit.SECONDS));

        // this is not a change
        record.put("key1", TextValue.valueOf("value1"));
        assertTrue(this.service.publishRecord(record).await(1, TimeUnit.SECONDS));

        assertTrue(latch.get().await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testAddAndRemoveRecordConnectionStatusListener() throws InterruptedException
    {

        this.service.createRecord(record1);

        final AtomicReference<String> expected = new AtomicReference<String>(record1);
        final AtomicReference<CountDownLatch> connected = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> reconnecting = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> disconnected = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        IRecordConnectionStatusListener connectionStatusListener = new IRecordConnectionStatusListener()
        {
            @Override
            public void onRecordDisconnected(String recordName)
            {
                if (expected.get().equals(recordName))
                {
                    disconnected.get().countDown();
                }
            }

            @Override
            public void onRecordConnected(String recordName)
            {
                if (expected.get().equals(recordName))
                {
                    connected.get().countDown();
                }
            }

            @Override
            public void onRecordConnecting(String recordName)
            {
                if (expected.get().equals(recordName))
                {
                    reconnecting.get().countDown();
                }
            }
        };
        assertTrue(this.candidate.addRecordConnectionStatusListener(connectionStatusListener));
        assertFalse(this.candidate.addRecordConnectionStatusListener(connectionStatusListener));

        this.candidate.addRecordListener(mock(IRecordListener.class), record1);

        assertTrue(connected.get().await(1, TimeUnit.SECONDS));

        final AtomicReference<CountDownLatch> connected2 = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> reconnecting2 =
            new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> disconnected2 =
            new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        IRecordConnectionStatusListener connectionStatusListener2 = new IRecordConnectionStatusListener()
        {
            @Override
            public void onRecordDisconnected(String recordName)
            {
                if (expected.get().equals(recordName))
                {
                    disconnected2.get().countDown();
                }
            }

            @Override
            public void onRecordConnected(String recordName)
            {
                if (expected.get().equals(recordName))
                {
                    connected2.get().countDown();
                }
            }

            @Override
            public void onRecordConnecting(String recordName)
            {
                if (expected.get().equals(recordName))
                {
                    reconnecting2.get().countDown();
                }
            }
        };
        assertTrue(this.candidate.addRecordConnectionStatusListener(connectionStatusListener2));
        assertTrue(connected2.get().await(1, TimeUnit.SECONDS));

        assertTrue(this.candidate.removeRecordConnectionStatusListener(connectionStatusListener2));
        assertFalse(this.candidate.removeRecordConnectionStatusListener(connectionStatusListener2));

        this.service.destroy();

        assertTrue(disconnected.get().await(1, TimeUnit.SECONDS));
        assertTrue(reconnecting.get().await(1, TimeUnit.SECONDS));
    }
}
