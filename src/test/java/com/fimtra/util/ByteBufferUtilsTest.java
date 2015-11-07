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
package com.fimtra.util;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.util.ByteBufferUtils;

/**
 * Tests for the {@link ByteBufferUtils}
 * 
 * @author Ramon Servadei
 */
public class ByteBufferUtilsTest
{

    private static final String HELLO = "hello";
    private static final byte[] BYTES = HELLO.getBytes();

    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testCopyBufferIntoBufferTooSmall()
    {
        ByteBuffer buf1 = ByteBuffer.wrap(BYTES);
        ByteBuffer buf2 = ByteBuffer.wrap(new byte[3]);
        final ByteBuffer result = ByteBufferUtils.copyBufferIntoBuffer(buf1, buf2);
        assertEquals(new String(buf1.array()), new String(result.array(), 0, result.position()));
    }

    @Test
    public void testCopyBufferIntoBuffer()
    {
        ByteBuffer buf1 = ByteBuffer.wrap(BYTES);
        ByteBuffer buf2 = ByteBuffer.wrap(new byte[BYTES.length]);
        ByteBufferUtils.copyBufferIntoBuffer(buf1, buf2);
        assertEquals(HELLO, new String(buf2.array()));
    }

    @Test
    public void testAsBytesZeroLength()
    {
        final byte[] result = ByteBufferUtils.asBytes(ByteBuffer.wrap(new byte[0]));
        assertEquals(0, result.length);
    }

    @Test
    public void testAsBytes()
    {
        final byte[] result = ByteBufferUtils.asBytes(ByteBuffer.wrap(BYTES));
        assertEquals(HELLO, new String(result));
    }

    @Test
    public void testCopyBytesIntoBufferTooSmall()
    {
        ByteBuffer buf1 = ByteBuffer.wrap(new byte[0]);
        final ByteBuffer result = ByteBufferUtils.copyBytesIntoBuffer(BYTES, buf1);
        assertEquals(HELLO, new String(result.array(), 0, result.position()));
    }

    @Test
    public void testCopyBytesIntoBuffer()
    {
        ByteBuffer buf1 = ByteBuffer.wrap(new byte[BYTES.length]);
        ByteBufferUtils.copyBytesIntoBuffer(BYTES, buf1);
        assertEquals(HELLO, new String(buf1.array()));
    }

    @Test
    public void testCopyZeroBytesIntoBuffer()
    {
        ByteBuffer buf1 = ByteBuffer.wrap(new byte[BYTES.length]);
        ByteBufferUtils.copyBytesIntoBuffer(BYTES, buf1);
        ByteBufferUtils.copyBytesIntoBuffer(new byte[0], buf1);
        assertEquals(HELLO, new String(buf1.array()));
    }

    @Test
    public void testGetBytes()
    {
        ByteBuffer buf1 = ByteBuffer.wrap(BYTES);
        assertEquals(HELLO, new String(ByteBufferUtils.getBytesFromBuffer(buf1, BYTES.length)));
    }

    @Test
    public void testGetBytesMultiple()
    {
        // check the buffer position is moved
        final String BYE = "BYE!!";
        ByteBuffer buf1 = ByteBuffer.wrap((HELLO + BYE).getBytes());
        assertEquals(HELLO, new String(ByteBufferUtils.getBytesFromBuffer(buf1, HELLO.getBytes().length)));
        assertEquals(BYE, new String(ByteBufferUtils.getBytesFromBuffer(buf1, BYE.getBytes().length)));
    }

    @Test
    public void testPutByteResizes()
    {
        byte b = (byte) 0x4;
        ByteBuffer result = ByteBufferUtils.put(b, ByteBuffer.allocate(0));
        assertEquals(b, result.array()[0]);
        result = ByteBufferUtils.put(b, result);
        assertEquals(b, result.array()[0]);
        assertEquals(b, result.array()[1]);
    }

    @Test
    public void testPutByteArrayResizes()
    {
        byte b = (byte) 0x4;
        ByteBuffer result = ByteBufferUtils.put(new byte[] { b, b }, ByteBuffer.allocate(1));
        assertEquals(b, result.array()[0]);
        assertEquals(b, result.array()[1]);
    }

    @Test
    public void testPutCharResizes()
    {
        char c = (char) 0x4;
        ByteBuffer result = ByteBufferUtils.putChar(c, ByteBuffer.allocate(1));
        assertEquals(0, result.array()[0]);
        assertEquals(4, result.array()[1]);
    }
    
    @Test
    public void testPutIntResizes()
    {
        int i = 4;
        ByteBuffer result = ByteBufferUtils.putInt(i, ByteBuffer.allocate(1));
        assertEquals(0, result.array()[0]);
        assertEquals(0, result.array()[1]);
        assertEquals(0, result.array()[2]);
        assertEquals(4, result.array()[3]);
    }
}
