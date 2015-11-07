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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.tcpchannel.TcpChannelUtils.BufferOverflowException;
import com.fimtra.util.LowGcLinkedList;

/**
 * Tests for the {@link TcpChannelUtils}
 * 
 * @author Ramon Servadei
 */
public class TestTcpChannelUtils
{
    @Before
    public void setUp() throws Exception
    {
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testDecodeUsingTerminator()
    {
        final byte[] terminator = { 0xd, 0xa };
        final String hello = "hello";
        final String world = "world";
        final String unfinished = "unfinished";
        final String finished = " but is now finished!!";

        final ByteBuffer buffer = ByteBuffer.allocate(65535);
        buffer.put(hello.getBytes());
        buffer.put(terminator);
        buffer.put(world.getBytes());
        buffer.put(terminator);
        buffer.put(unfinished.getBytes());

        LowGcLinkedList<byte[]> result = new LowGcLinkedList<byte[]>();
        TcpChannelUtils.decodeUsingTerminator(result, buffer, terminator);
        assertEquals(unfinished.length(), buffer.position());
        assertEquals(2, result.size());
        assertEquals(hello, new String(result.get(0)));
        assertEquals(world, new String(result.get(1)));

        result = new LowGcLinkedList<byte[]>();
        TcpChannelUtils.decodeUsingTerminator(result, buffer, terminator);
        assertEquals(unfinished.length(), buffer.position());
        assertEquals(0, result.size());

        buffer.put(finished.getBytes());
        buffer.put(terminator);
        buffer.put(terminator);
        result = new LowGcLinkedList<byte[]>();
        TcpChannelUtils.decodeUsingTerminator(result, buffer, terminator);
        assertEquals(0, buffer.position());
        assertEquals("result is " + showResult(result), 2, result.size());
        assertEquals(unfinished + finished, new String(result.get(0)));
        assertEquals("", new String(result.get(1)));

        result = new LowGcLinkedList<byte[]>();
        TcpChannelUtils.decodeUsingTerminator(result, buffer, terminator);
        assertEquals(0, buffer.position());
        assertEquals(0, result.size());
    }

    private static String showResult(List<byte[]> result)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (byte[] bs : result)
        {
            sb.append(new String(bs)).append(", ");
        }
        sb.append("]");
        return sb.toString();
    }

    @Test
    public void testDecode()
    {
        int i = 0;
        byte[] data = new byte[12];
        // 0,0,0,2 for getting 5,6
        data[i++] = (byte) 0;
        data[i++] = (byte) 0;
        data[i++] = (byte) 0;
        data[i++] = (byte) 2;
        data[i++] = (byte) 5;
        data[i++] = (byte) 6;
        // note, this test uses a partial buffer - we indicate that there are 3 bytes (0,0,0,3) but
        // only have 2 (7,8)
        data[i++] = (byte) 0;
        data[i++] = (byte) 0;
        data[i++] = (byte) 0;
        data[i++] = (byte) 3;
        data[i++] = (byte) 7;
        data[i++] = (byte) 8;

        final ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.put(data);
        final LowGcLinkedList<byte[]> decode = new LowGcLinkedList<byte[]>();
        TcpChannelUtils.decode(decode, buffer);
        assertEquals(1, decode.size());
        byte[] expected = new byte[2];
        expected[0] = (byte) 5;
        expected[1] = (byte) 6;
        assertTrue(Arrays.equals(expected, decode.get(0)));
        i = 0;
        byte[] expectedRemainder = new byte[6];
        expectedRemainder[i++] = (byte) 0;
        expectedRemainder[i++] = (byte) 0;
        expectedRemainder[i++] = (byte) 0;
        expectedRemainder[i++] = (byte) 3;
        expectedRemainder[i++] = (byte) 7;
        expectedRemainder[i++] = (byte) 8;
        final byte[] remainder = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, remainder, 0, remainder.length);
        assertTrue("Got " + Arrays.toString(remainder), Arrays.equals(expectedRemainder, remainder));
    }

    @Test(expected = BufferOverflowException.class)
    public void testDecodeOverflow()
    {
        int i = 0;
        byte[] data = new byte[6];
        // 0,0,0,9 but its too much data
        data[i++] = (byte) 0;
        data[i++] = (byte) 0;
        data[i++] = (byte) 0;
        data[i++] = (byte) 9;
        data[i++] = (byte) 5;
        data[i++] = (byte) 6;

        final ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.put(data);
        final Deque<byte[]> decode = new LowGcLinkedList<byte[]>();
        TcpChannelUtils.decode(decode, buffer);
    }

    @Test(expected = BufferOverflowException.class)
    public void testDecodeUsingTerminatorOverflow()
    {
        final byte[] terminator = { 0xd, 0xa };
        int i = 0;
        byte[] data = new byte[6];
        // 0,0,0,9 but its too much data
        data[i++] = (byte) 0;
        data[i++] = (byte) 0;
        data[i++] = (byte) 0;
        data[i++] = (byte) 9;
        data[i++] = (byte) 5;
        data[i++] = (byte) 6;

        final ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.put(data);
        final Deque<byte[]> decode = new LowGcLinkedList<byte[]>();
        TcpChannelUtils.decodeUsingTerminator(decode, buffer, terminator);
    }

    @SuppressWarnings("unused")
    @Test
    public void testGetNextFreeTcpServerPort() throws IOException
    {
        final int startPortRangeInclusive = 23214;
        final int endPortRangeExclusive = startPortRangeInclusive + 2;

        final int nextFreeDefaultTcpServerPort =
            TcpChannelUtils.getNextFreeTcpServerPort(TcpChannelUtils.LOCALHOST_IP, startPortRangeInclusive,
                endPortRangeExclusive);
        assertEquals(startPortRangeInclusive, nextFreeDefaultTcpServerPort);
        ServerSocket ssoc = new ServerSocket(nextFreeDefaultTcpServerPort);

        final int nextFreeDefaultTcpServerPort2 =
            TcpChannelUtils.getNextFreeTcpServerPort(TcpChannelUtils.LOCALHOST_IP, startPortRangeInclusive,
                endPortRangeExclusive);
        assertEquals(startPortRangeInclusive + 1, nextFreeDefaultTcpServerPort2);
        ServerSocket ssoc2 = new ServerSocket(nextFreeDefaultTcpServerPort2);

        try
        {
            TcpChannelUtils.getNextFreeTcpServerPort(TcpChannelUtils.LOCALHOST_IP, startPortRangeInclusive,
                endPortRangeExclusive);
            fail("Should throw exception");
        }
        catch (RuntimeException e)
        {
        }
    }
}
