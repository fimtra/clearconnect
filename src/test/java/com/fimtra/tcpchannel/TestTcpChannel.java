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
package com.fimtra.tcpchannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;

/**
 * Tests for the {@link TcpChannel}
 * 
 * @author Ramon Servadei
 */
public class TestTcpChannel
{
    static int SERVER_PORT = 20000;
    
    TcpServer server;
    TcpChannel c1;

    @Before
    public void setUp() throws Exception
    {
    }

    @After
    public void tearDown() throws Exception
    {
        if (this.server != null)
        {
            this.server.destroy();
        }
        if (this.c1 != null)
        {
            this.c1.destroy("end test");
        }
    }

    @SuppressWarnings("unused")
    @Test(expected = IOException.class)
    public void testAttemptConnectToNonExistentServer() throws IOException
    {
        IReceiver receiver = mock(IReceiver.class);
        new TcpChannel("localhost", SERVER_PORT, receiver);
    }

    @Test
    public void testOrbitingThePlanetAtMaximumVelocity() throws IOException, InterruptedException
    {
        // the moon with the rebel base will be in range in 30minutes
        // This will be a day long remembered...it has seen the end of Kenobi, it will soon see the
        // end of the rebellion...

        long size = 0;
        final int max = 100000;
        List<String> data = new ArrayList<String>(max);
        for (int i = 0; i < max; i++)
        {
            data.add("index: " + i);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        final List<String> rxData = new ArrayList<String>(max);
        IReceiver receiver = new IReceiver()
        {

            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                rxData.add(new String(data));
                if (rxData.size() == max)
                {
                    latch.countDown();
                }
            }

            @Override
            public void onChannelConnected(ITransportChannel channel)
            {

            }

            @Override
            public void onChannelClosed(ITransportChannel channel)
            {

            }
        };
        this.server = new TcpServer("127.0.0.1", SERVER_PORT, receiver);
        TcpChannel c1 = new TcpChannel("127.0.0.1", SERVER_PORT, mock(IReceiver.class));

        long start = System.nanoTime();
        for (int i = 0; i < max; i++)
        {
            final String e = data.get(i);
            final byte[] bytes = e.getBytes();
            c1.send(bytes);
            size += bytes.length;
        }

        assertTrue("Got: " + rxData.size(), latch.await(10, TimeUnit.SECONDS));
        final long latency = System.nanoTime() - start;
        System.err.println("TCP max velocity: " + (max / (latency / 1000000000)) + " msgs/s, "
            + (size / (latency / 1000000000)) + " b/s");
        assertEquals("", data, rxData);
    }

}
