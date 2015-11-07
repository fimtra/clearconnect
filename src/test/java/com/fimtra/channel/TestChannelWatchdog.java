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
package com.fimtra.channel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.ChannelWatchdog;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpChannel;
import com.fimtra.tcpchannel.TcpServer;
import com.fimtra.tcpchannel.TestTcpServer.EchoReceiver;
import com.fimtra.tcpchannel.TestTcpServer.NoopReceiver;

/**
 * Tests for the {@link ChannelWatchdog}
 * 
 * @author Ramon Servadei
 */
public class TestChannelWatchdog
{
    final static ChannelWatchdog candidate = ChannelUtils.WATCHDOG;

    static final int PERIOD = 10;

    private static final String LOCALHOST = "localhost";

    @Before
    public void setUp() throws Exception
    {
        candidate.configure(PERIOD);
    }

    @After
    public void tearDown() throws Exception
    {
        candidate.configure(5000);
    }

    @Test
    public void testWatchdog() throws InterruptedException, IOException
    {
        int port = 13000;
        final CountDownLatch clientClosedLatch = new CountDownLatch(1);
        final CountDownLatch client2ClosedLatch = new CountDownLatch(1);
        TcpServer server = new TcpServer(LOCALHOST, port, new EchoReceiver());

        final TcpChannel client = new TcpChannel(LOCALHOST, port, new NoopReceiver()
        {
            @Override
            public void onChannelClosed(ITransportChannel tcpChannel)
            {
                clientClosedLatch.countDown();
            }
        });
        final TcpChannel client2 = new TcpChannel(LOCALHOST, port, new NoopReceiver()
        {
            @Override
            public void onChannelClosed(ITransportChannel tcpChannel)
            {
                client2ClosedLatch.countDown();
            }
        });

        assertTrue(candidate.channels.contains(client));
        assertTrue(candidate.channels.contains(client2));

        Thread.sleep(PERIOD + 2);

        client.destroy("unit test shutdown");
        // assertTrue(clientClosedLatch.await(5, TimeUnit.SECONDS));

        // wait for the watchdog to re-run and check the client is not being tracked
        Thread.sleep(PERIOD * 10);
        assertFalse(candidate.channels.contains(client));
        assertTrue("Got: " + candidate.channels, candidate.channels.contains(client2));
        client.destroy("unit test");
        client2.destroy("unit test");
        server.destroy();
    }

}
