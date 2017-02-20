/*
 * Copyright (c) 2016 Ramon Servadei
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
package com.fimtra.ipcchannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpChannel;
import com.fimtra.tcpchannel.TcpServer;
import com.fimtra.tcpchannel.TestTcpServer.EchoReceiver;
import com.fimtra.tcpchannel.TestTcpServer.NoopReceiver;
import com.fimtra.util.Log;

/**
 * Tests for the {@link IpcService}
 * 
 * @author Ramon Servadei
 */
public class TestIpcService
{
    private static final int CLIENT_COUNT = 20;

    @Rule
    public TestName name = new TestName();

    final static IReceiver noopReceiver = new NoopReceiver();
    private final static int STD_TIMEOUT = 10;
    static int port = 0;

    String node;
    String serviceDir;
    IpcService server;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        port++;
        this.node = "./ipc-tests/" + this.name.getMethodName();
        this.serviceDir = this.node + "/" + port + "/";
    }

    @After
    public void tearDown()
    {
        this.server.destroy();
    }

    @Test
    public void testAttemptConnectionWhenServerIsShutDown() throws IOException, InterruptedException
    {
        this.server = new IpcService(new EndPointAddress(this.node, port), new EchoReceiver());
        this.server.destroy();

        final IReceiver receiver = Mockito.mock(IReceiver.class);
        final IpcChannel client = createChannel(receiver);
        
        Mockito.verify(receiver, Mockito.timeout(6000)).onChannelClosed(Mockito.eq(client));
    }

    @Test
    public void testMultipleConnections() throws IOException, InterruptedException
    {
        this.server = new IpcService(new EndPointAddress(this.node, port), new EchoReceiver());
        final int clientCount = CLIENT_COUNT;
        final CountDownLatch channelConnectedLatch = new CountDownLatch(clientCount);
        final CountDownLatch closedLatch = new CountDownLatch(clientCount);
        List<IpcChannel> clients = new ArrayList<IpcChannel>(clientCount);
        for (int i = 0; i < clientCount; i++)
        {
            final int count = i;
            clients.add(createChannel(new NoopReceiver()
            {
                @Override
                public void onChannelConnected(ITransportChannel tcpChannel)
                {
                    Log.log(this, "### Connected channel #" + count + ", channel=" + tcpChannel);
                    channelConnectedLatch.countDown();
                }

                @Override
                public void onChannelClosed(ITransportChannel tcpChannel)
                {
                    Log.log(this, "### Closing channel #" + count + ", channel=" + tcpChannel);
                    closedLatch.countDown();
                }
            }));
        }
        boolean result = channelConnectedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS);
        assertTrue("Only connected " + ((clientCount) - channelConnectedLatch.getCount()) + " clients", result);
        assertTrue(closedLatch.getCount() == clientCount);

        // Don't destroy the server just yet, give things time to settle - don't remove this!
        Thread.sleep(1000);

        this.server.destroy();
        result = closedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS);
        assertTrue("Only closed " + ((clientCount) - closedLatch.getCount()) + " clients", result);

        // now loop around until we get our callback invoked
        final int MAX_TRIES = 10;
        for (int j = 0; j <= MAX_TRIES; j++)
        {
            boolean connected = false;
            for (int i = 0; i < clientCount; i++)
            {
                final IpcChannel channel = clients.get(i);
                connected |= channel.isConnected();
                if (j == MAX_TRIES)
                {
                    assertFalse("Not CLOSED at index " + i + ", " + channel.toString(), connected);
                }
            }
            if (!connected)
            {
                break;
            }
            Thread.sleep(100);
        }
    }

    @Test
    public void testMultipleConnectionsAndSending() throws IOException, InterruptedException
    {
        final int messageCount = 200;
        final int clientCount = CLIENT_COUNT;
        final CountDownLatch latch = new CountDownLatch(messageCount * clientCount);
        final CountDownLatch channelConnectedLatch = new CountDownLatch(clientCount);
        final CountDownLatch closedLatch = new CountDownLatch(clientCount);

        this.server = new IpcService(new EndPointAddress(this.node, port), new NoopReceiver()
        {
            Map<ITransportChannel, Integer> lastValue = (new HashMap<ITransportChannel, Integer>());

            @Override
            public synchronized void onDataReceived(byte[] data, ITransportChannel source)
            {
                Integer now = Integer.valueOf(new String(data));
                Integer then = this.lastValue.get(source);
                if (then == null)
                {
                    then = Integer.valueOf(-1);
                }
                final int expect = then + 1;
                if (expect != now.intValue())
                {
                    Log.log(this, "Expected: " + expect + ", but got " + now.intValue() + " from " + source);
                    new RuntimeException(
                        "Expected: " + expect + ", but got " + now.intValue() + " from " + source).printStackTrace();
                    System.exit(-99);
                }
                else
                {
                    this.lastValue.put(source, now);
                    latch.countDown();
                }
            }
        });

        List<IpcChannel> clients = new ArrayList<IpcChannel>(clientCount);
        for (int i = 0; i < clientCount; i++)
        {
            clients.add(createChannel(new NoopReceiver()
            {
                @Override
                public void onChannelConnected(ITransportChannel tcpChannel)
                {
                    channelConnectedLatch.countDown();
                }

                @Override
                public void onChannelClosed(ITransportChannel tcpChannel)
                {
                    closedLatch.countDown();
                }
            }));
        }
        boolean result = channelConnectedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS);
        assertTrue("Only connected " + ((clientCount) - channelConnectedLatch.getCount()) + " clients", result);

        for (int j = 0; j < messageCount; j++)
        {
            for (int i = 0; i < clientCount; i++)
            {
                clients.get(i).sendAsync(("" + j).getBytes());
            }
        }
        result = latch.await(STD_TIMEOUT * 3, TimeUnit.SECONDS);
        assertTrue("Only received " + ((messageCount * clientCount) - latch.getCount()) + " correct messages", result);
    }

    IpcChannel createChannel(final IReceiver receiver) throws IOException
    {
        final String uuid = UUID.randomUUID().toString() + System.nanoTime();
        return new IpcChannel(this.serviceDir + uuid + "." + port, this.serviceDir + +port + "." + uuid, receiver);
    }

    @Test
    public void testSimpleClientServerMessageSending() throws IOException, InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(4);
        final List<String> expected1 = new ArrayList<String>();
        final List<String> received1 = new ArrayList<String>();
        final List<String> expected2 = new ArrayList<String>();
        final List<String> received2 = new ArrayList<String>();
        final String message1 = "hello1";
        final String message2 = "hello2";
        expected1.add(message1);
        expected1.add(message2);
        expected2.add(message1);
        expected2.add(message2);
        this.server = new IpcService(new EndPointAddress(this.node, port), new EchoReceiver());

        final IpcChannel client = createChannel(new NoopReceiver()
        {
            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                received1.add(new String(data));
                latch.countDown();
            }
        });
        final IpcChannel client2 = createChannel(new NoopReceiver()
        {
            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                received2.add(new String(data));
                latch.countDown();
            }
        });

        assertTrue(client.sendAsync(message1.getBytes()));
        assertTrue(client.sendAsync(message2.getBytes()));
        assertTrue(client2.sendAsync(message1.getBytes()));
        assertTrue(client2.sendAsync(message2.getBytes()));
        final boolean result = latch.await(STD_TIMEOUT, TimeUnit.SECONDS);
        assertTrue("onDataReceived only called " + (4 - latch.getCount()) + " times", result);
        assertEquals(expected1, received1);
        assertEquals(expected2, received2);
    }

    @Test
    public void testBigMessageClientServerMessageSending() throws IOException, InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(4);
        final List<String> expected1 = new ArrayList<String>();
        final List<String> received1 = new ArrayList<String>();
        final List<String> expected2 = new ArrayList<String>();
        final List<String> received2 = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++)
        {
            sb.append("hello").append(i);
        }
        final String message1 = sb.toString();
        final String message2 = "hello2";
        expected1.add(message1);
        expected1.add(message2);
        expected2.add(message1);
        expected2.add(message2);

        this.server = new IpcService(new EndPointAddress(this.node, port), new EchoReceiver());

        final IpcChannel client = createChannel(new NoopReceiver()
        {
            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                received1.add(new String(data));
                latch.countDown();
            }
        });
        final IpcChannel client2 = createChannel(new NoopReceiver()
        {
            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                received2.add(new String(data));
                latch.countDown();
            }
        });

        assertTrue(client.sendAsync(message1.getBytes()));
        assertTrue(client.sendAsync(message2.getBytes()));
        assertTrue(client2.sendAsync(message1.getBytes()));
        assertTrue(client2.sendAsync(message2.getBytes()));
        final boolean result = latch.await(STD_TIMEOUT, TimeUnit.SECONDS);
        assertTrue("onDataReceived only called " + (4 - latch.getCount()) + " times", result);
        assertEquals(expected1, received1);
        assertEquals(expected2, received2);
    }

    @Test
    public void testHighThroughputClient() throws IOException, InterruptedException
    {
        final int messageCount = 5000;
        final int[] last = new int[] { 0 };
        final CountDownLatch latch = new CountDownLatch(messageCount);

        this.server = new IpcService(new EndPointAddress(this.node, port), new NoopReceiver()
        {
            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                int now = Integer.valueOf(new String(data)).intValue();
                if (last[0] + 1 != now)
                {
                    throw new RuntimeException("Invalid sequence: last=" + last[0] + ", now=" + now);
                }
                last[0] = now;
                latch.countDown();
            }
        });

        final IpcChannel client = createChannel(noopReceiver);
        int i = 0;
        while (i < messageCount)
        {
            client.sendAsync(("" + ++i).getBytes());
        }
        assertTrue("Only received " + (messageCount - latch.getCount()) + " correct messages",
            latch.await(STD_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    public void testMassiveMessage() throws IOException, InterruptedException
    {
        ChannelUtils.WATCHDOG.configure(1000);
        final CountDownLatch channelConnectedLatch = new CountDownLatch(1);
        final CountDownLatch dataLatch = new CountDownLatch(1);
        final AtomicReference<byte[]> dataRef = new AtomicReference<byte[]>();
        EchoReceiver clientSocketReceiver = new EchoReceiver();
        this.server = new IpcService(new EndPointAddress(this.node, port), clientSocketReceiver);
        IpcChannel client = createChannel(new NoopReceiver()
        {
            @Override
            public void onChannelConnected(ITransportChannel tcpChannel)
            {
                channelConnectedLatch.countDown();
            }

            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                dataRef.set(data);
                dataLatch.countDown();
            }

        });
        assertTrue("channel was not connected", channelConnectedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));
        final int MSG_SIZE = 65521;
        client.sendAsync(generateMassiveMessage(MSG_SIZE));
        assertTrue(dataLatch.await(1, TimeUnit.SECONDS));
        assertEquals("data rx", MSG_SIZE, dataRef.get().length);
    }

    private static byte[] generateMassiveMessage(int i)
    {
        final byte[] data = new byte[i];
        Arrays.fill(data, (byte) '0');
        return data;
    }
}
