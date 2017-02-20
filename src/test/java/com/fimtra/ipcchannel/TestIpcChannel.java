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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.util.ThreadUtils;

/**
 * Tests for the {@link IpcChannel}
 * 
 * @author Ramon Servadei
 */
public class TestIpcChannel
{
    private static final int CONNECTION_LOST_TIMEOUT = (int) (IpcChannel.HEARTBEAT_TIMEOUT_NANOS / 1000000);
    
    IpcChannel c1, c2;
    IReceiver receiver1, receiver2;
    ScheduledExecutorService service;

    @Before
    public void setUp() throws Exception
    {
        service = ThreadUtils.newScheduledExecutorService("TestIpcChannel", 1);

        createChannel1();

        createChannel2();

        verify(receiver1, timeout(1000)).onChannelConnected(eq(c1));
        verify(receiver2, timeout(1000)).onChannelConnected(eq(c2));
    }

    private void setupWriter(final IpcChannel channel)
    {
        service.scheduleAtFixedRate(new Runnable()
        {
            int counter = 0;

            @Override
            public void run()
            {
                final String data = new Date().toString() + " " + counter++;
                channel.sendAsync(data.getBytes());
            }
        }, 1, 1, TimeUnit.MILLISECONDS);
    }

    void createChannel2() throws IOException
    {
        receiver2 = mock(IReceiver.class);
        c2 = new IpcChannel("c2.out", "c1.out", receiver2);
    }

    void createChannel1() throws IOException
    {
        receiver1 = mock(IReceiver.class);
        c1 = new IpcChannel("c1.out", "c2.out", receiver1);
    }

    @After
    public void tearDown()
    {
        service.shutdown();
        c1.destroy("end test");
        c2.destroy("end test");
    }

    @Test
    public void testOrbitingThePlanetAtMaximumVelocity() throws IOException, InterruptedException
    {
        // the moon with the rebel base will be in range in 30minutes
        // This will be a day long remembered...it has seen the end of Kenobi, it will soon see the
        // end of the rebellion...

        c1.destroy("testOrbitingThePlanetAtMaximumVelocity");
        c2.destroy("testOrbitingThePlanetAtMaximumVelocity");
        
        long size = 0;
        final int max = 1000000;
        List<String> data = new ArrayList<String>(max);
        for (int i = 0; i < max; i++)
        {
            data.add("index: " + i);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        final List<String> rxData = new ArrayList<String>(max);
        receiver2 = new IReceiver()
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
        c2 = new IpcChannel("c2.out", "c1.out", receiver2);
        createChannel1();

        long start = System.nanoTime();
        for (int i = 0; i < max; i++)
        {
            final String e = data.get(i);
            final byte[] bytes = e.getBytes();
            c1.sendAsync(bytes);
            size += bytes.length;
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        final long latency = System.nanoTime() - start;
        System.err.println("IPC max velocity: " + (max / (latency / 1000000000)) + " msgs/s, " + (size / (latency / 1000000000)) + " b/s");
        assertEquals("", data, rxData);
    }

    @Test
    public void testReadWrite()
    {
        setupWriter(c1);
        setupWriter(c2);
        verify(receiver1, timeout(100).atLeast(20)).onDataReceived((byte[]) any(), eq(c1));
        verify(receiver2, timeout(100).atLeast(20)).onDataReceived((byte[]) any(), eq(c2));
    }
    
    @Test
    public void testReaderDestroyed() throws IOException
    {
        // c1=writer
        // c2=reader
        setupWriter(c1);
        verify(receiver2, timeout(100).atLeast(20)).onDataReceived((byte[]) any(), eq(c2));
        c2.destroy("end!");
        verify(receiver2, timeout(100)).onChannelClosed(eq(c2));
        verify(receiver1, timeout(100)).onChannelClosed(eq(c1));
    }

    @Test
    public void testWriterDestroyed() throws IOException
    {
        // c1=writer
        // c2=reader
        setupWriter(c1);
        verify(receiver2, timeout(100).atLeast(20)).onDataReceived((byte[]) any(), eq(c2));
        c1.destroy("end!");
        verify(receiver2, timeout(100)).onChannelClosed(eq(c2));
        verify(receiver1, timeout(100)).onChannelClosed(eq(c1));
    }   

}
