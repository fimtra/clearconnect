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

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.tcpchannel.TcpChannelUtils.BufferOverflowException;
import com.fimtra.util.ByteBufferUtils;

/**
 * Tests for the {@link TcpChannelUtils}
 * 
 * @author Ramon Servadei
 */
public class TestTcpChannelUtils
{
    final int[] readFramesSize = new int[1];

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

        ByteBuffer[] result = new ByteBuffer[2];
        result = TcpChannelUtils.decodeUsingTerminator(result, this.readFramesSize, buffer, buffer.array(), terminator);
        assertEquals(2, this.readFramesSize[0]);
        assertEquals(hello, new String(ByteBufferUtils.asBytes(result[0])));
        assertEquals(world, new String(ByteBufferUtils.asBytes(result[1])));
        buffer.compact();

        result = new ByteBuffer[2];
        result = TcpChannelUtils.decodeUsingTerminator(result, this.readFramesSize, buffer, buffer.array(), terminator);
        assertEquals(0, this.readFramesSize[0]);
        buffer.compact();

        buffer.put(finished.getBytes());
        buffer.put(terminator);
        buffer.put(terminator);
        result = new ByteBuffer[2];
        result = TcpChannelUtils.decodeUsingTerminator(result, this.readFramesSize, buffer, buffer.array(), terminator);
        assertEquals(2, this.readFramesSize[0]);
        assertEquals(unfinished + finished, new String(ByteBufferUtils.asBytes(result[0])));
        assertEquals("", new String(ByteBufferUtils.asBytes(result[1])));
        buffer.compact();

        result = new ByteBuffer[2];
        result = TcpChannelUtils.decodeUsingTerminator(result, this.readFramesSize, buffer, buffer.array(), terminator);
        assertEquals(0, buffer.position());
        assertEquals(0, this.readFramesSize[0]);
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
        ByteBuffer[] decode = new ByteBuffer[2];
        decode = TcpChannelUtils.decode(decode, this.readFramesSize, buffer, data);
        assertEquals(1, this.readFramesSize[0]);
        byte[] expected = new byte[2];
        expected[0] = (byte) 5;
        expected[1] = (byte) 6;
        assertTrue(Arrays.equals(expected, ByteBufferUtils.asBytes(decode[0])));
        buffer.compact();

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
        ByteBuffer[] decode = new ByteBuffer[2];
        decode = TcpChannelUtils.decode(decode, this.readFramesSize, buffer, data);
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
        ByteBuffer[] decode = new ByteBuffer[2];
        decode = TcpChannelUtils.decodeUsingTerminator(decode, this.readFramesSize, buffer, data, terminator);
    }
    
    @Test(expected = IOException.class)
    public void testBindWithinRange() throws IOException
    {
        ServerSocket ssoc1 = new ServerSocket();
        ssoc1.setReuseAddress(true);
        TcpChannelUtils.bindWithinRange(ssoc1, null, 2000, 2001);
        assertEquals(2000, ssoc1.getLocalPort());
        
        ServerSocket ssoc2 = new ServerSocket();
        ssoc2.setReuseAddress(true);
        TcpChannelUtils.bindWithinRange(ssoc2, null, 2000, 2001);
        assertEquals(2001, ssoc2.getLocalPort());
        
        // exception now
        ServerSocket ssoc3 = new ServerSocket();
        ssoc3.setReuseAddress(true);
        TcpChannelUtils.bindWithinRange(ssoc3, null, 2000, 2001);
        assertEquals(2001, ssoc2.getLocalPort());
    }
}
