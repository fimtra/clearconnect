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

import java.nio.CharBuffer;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.util.CharBufferUtils;

/**
 * Tests for the {@link CharBufferUtils}
 * 
 * @author Ramon Servadei
 */
public class CharBufferUtilsTest
{

    private static final String HELLO = "hello";
    private static final char[] CHARS = HELLO.toCharArray();

    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testCopyBufferIntoBufferTooSmall()
    {
        CharBuffer buf1 = CharBuffer.wrap(CHARS);
        CharBuffer buf2 = CharBuffer.wrap(new char[3]);
        final CharBuffer result = CharBufferUtils.copyBufferIntoBuffer(buf1, buf2);
        assertEquals(new String(buf1.array()), new String(result.array(), 0, result.position()));
    }

    @Test
    public void testCopyBufferIntoBuffer()
    {
        CharBuffer buf1 = CharBuffer.wrap(CHARS);
        CharBuffer buf2 = CharBuffer.wrap(new char[CHARS.length]);
        CharBufferUtils.copyBufferIntoBuffer(buf1, buf2);
        assertEquals(HELLO, new String(buf2.array()));
    }

    @Test
    public void testAsCharsZeroLength()
    {
        final char[] result = CharBufferUtils.asChars(CharBuffer.wrap(new char[0]));
        assertEquals(0, result.length);
    }

    @Test
    public void testAsChars()
    {
        final char[] result = CharBufferUtils.asChars(CharBuffer.wrap(CHARS));
        assertEquals(HELLO, new String(result));
    }

    @Test
    public void testCopyCharsIntoBufferTooSmall()
    {
        CharBuffer buf1 = CharBuffer.wrap(new char[0]);
        final CharBuffer result = CharBufferUtils.copyCharsIntoBuffer(CHARS, buf1);
        assertEquals(HELLO, new String(result.array(), 0, result.position()));
    }

    @Test
    public void testCopyCharsIntoBuffer()
    {
        CharBuffer buf1 = CharBuffer.wrap(new char[CHARS.length]);
        CharBufferUtils.copyCharsIntoBuffer(CHARS, buf1);
        assertEquals(HELLO, new String(buf1.array()));
    }

    @Test
    public void testCopyZeroCharsIntoBuffer()
    {
        CharBuffer buf1 = CharBuffer.wrap(new char[CHARS.length]);
        CharBufferUtils.copyCharsIntoBuffer(CHARS, buf1);
        CharBufferUtils.copyCharsIntoBuffer(new char[0], buf1);
        assertEquals(HELLO, new String(buf1.array()));
    }

    @Test
    public void testGetChars()
    {
        CharBuffer buf1 = CharBuffer.wrap(CHARS);
        assertEquals(HELLO, new String(CharBufferUtils.getCharsFromBuffer(buf1, CHARS.length)));
    }

    @Test
    public void testGetCharsFromBufferAndReset()
    {
        CharBuffer buf1 = CharBuffer.allocate(32);
        CharBufferUtils.put(HELLO.toCharArray(), buf1);
        assertEquals(HELLO, new String(CharBufferUtils.getCharsFromBufferAndReset(buf1)));
        CharBufferUtils.put("BYE!!!!".toCharArray(), buf1);
        assertEquals("BYE!!!!", new String(CharBufferUtils.getCharsFromBufferAndReset(buf1)));
    }

    @Test
    public void testGetCharsMultiple()
    {
        // check the buffer position is moved
        final String BYE = "BYE!!";
        CharBuffer buf1 = CharBuffer.wrap((HELLO + BYE).toCharArray());
        assertEquals(HELLO, new String(CharBufferUtils.getCharsFromBuffer(buf1, HELLO.toCharArray().length)));
        assertEquals(BYE, new String(CharBufferUtils.getCharsFromBuffer(buf1, BYE.toCharArray().length)));
    }

    @Test
    public void testPutByteResizes()
    {
        char b = (char) 0x4;
        CharBuffer result = CharBufferUtils.put(b, CharBuffer.allocate(0));
        assertEquals(b, result.array()[0]);
        result = CharBufferUtils.put(b, result);
        assertEquals(b, result.array()[0]);
        assertEquals(b, result.array()[1]);
    }

    @Test
    public void testPutByteArrayResizes()
    {
        char b = (char) 0x4;
        CharBuffer result = CharBufferUtils.put(new char[] { b, b }, CharBuffer.allocate(1));
        assertEquals(b, result.array()[0]);
        assertEquals(b, result.array()[1]);
    }

    @Test
    public void testPutCharResizes()
    {
        char c = (char) 0x4;
        CharBuffer result = CharBufferUtils.put(c, CharBuffer.allocate(0));
        assertEquals(c, result.array()[0]);
    }
}
