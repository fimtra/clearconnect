/*
 * Copyright (c) 2014 Ramon Servadei
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
package com.fimtra.datafission.field;

import static com.fimtra.datafission.field.LongValueTest.checkNormalVsOptimisedResults;
import static com.fimtra.datafission.field.LongValueTest.computeStats;
import static com.fimtra.datafission.field.LongValueTest.max_retry;
import static com.fimtra.datafission.field.LongValueTest.prepareForPerfTestStep;
import static com.fimtra.datafission.field.LongValueTest.saveQuickestTimes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.util.StringAppender;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link BlobValue}
 *
 * @author Ramon Servadei
 */
public class BlobValueTest
{

    private static final String _1AF3416 = "1a0f34160a0b0c0d0e0f";
    BlobValue candidate;
    byte[] bytes = new byte[] { 0x1a, 0xf, 0x34, 0x16, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f };

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new BlobValue(this.bytes);
    }

    @Test
    public void test_constructors()
    {
        assertEquals("B" + _1AF3416, new BlobValue(_1AF3416).toString());
        final char[] charArray = _1AF3416.toCharArray();
        assertEquals("B" + _1AF3416, new BlobValue(charArray, 0, charArray.length).toString());
    }

    @Test
    public void test_appendTo()
    {
        final StringAppender stringAppender = new StringAppender();
        candidate.appendTo(stringAppender);
        assertEquals("B" + _1AF3416, stringAppender.toString());
    }

    @Test
    public void testObjectToFromBlob()
    {
        assertNotNull(BlobValue.toBlob(null));
        assertNull(BlobValue.fromBlob((IValue) null));
        assertNull(BlobValue.fromBlob(null));
        String s = "Lasers";
        assertEquals(s, BlobValue.fromBlob((IValue) BlobValue.toBlob(s)));
        assertEquals(s, BlobValue.fromBlob(BlobValue.toBlob(s)));
        assertNull(BlobValue.fromBlob(BlobValue.toBlob(null)));
    }

    @Test
    public void testHashCodeAndEquals()
    {
        assertEquals(new BlobValue(this.bytes).hashCode(), new BlobValue(this.bytes).hashCode());
        assertNotEquals(new BlobValue().hashCode(), new BlobValue(this.bytes).hashCode());
        assertEquals(new BlobValue(this.bytes), new BlobValue(this.bytes));
        assertFalse(new BlobValue().equals(new BlobValue(this.bytes)));
    }

    @Test
    public void testFromCharCapitals()
    {
        final BlobValue other = new BlobValue();
        char[] charArray = "1a0f34160A0B0C0D0E0F".toCharArray();
        other.fromChars(charArray, 0, charArray.length);
        assertEquals(other, this.candidate);
    }

    @Test
    public void testMod2Optimistaion()
    {
        int max = 100000;
        long t;
        t = System.nanoTime();
        boolean res;
        for (int i = 0; i < max; i++)
        {
            assertEquals("Failed at " + i, i % 2 != 0, (i & 0x1) == 1);
        }
        for (int i = 0; i < max; i++)
        {
            res = i % 2 != 0;
        }
        final long classic = System.nanoTime() - t;

        t = System.nanoTime();
        for (int i = 0; i < max; i++)
        {
            res = (i & 0x1) != 0;
        }
        final long mathUtils = System.nanoTime() - t;
        assertTrue("classic=" + classic + " optimised=" + mathUtils, mathUtils <= classic);

    }

    @Test
    public void testFromChar()
    {
        final BlobValue other = new BlobValue();
        char[] charArray = _1AF3416.toCharArray();
        other.fromChars(charArray, 0, charArray.length);
        assertEquals(other, this.candidate);
    }

    @Test
    public void testGetType()
    {
        assertEquals(TypeEnum.BLOB, this.candidate.getType());
    }

    @Test
    public void testLongValue()
    {
        assertEquals(10, this.candidate.longValue());
    }

    @Test
    public void testDoubleValue()
    {
        assertEquals(10.0, this.candidate.doubleValue(), 1.0);
    }

    @Test
    public void testTextValue()
    {
        assertEquals(_1AF3416, this.candidate.textValue());
    }

    @Test
    public void testSmallToFromString() throws Exception
    {
        final byte[] bytes = new byte[] { 0xf, 0x0, 0x1 };
        this.candidate = new BlobValue(bytes);
        BlobValue other = new BlobValue();
        final char[] chars = this.candidate.textValue()
                .toCharArray();
        other.fromChars(chars, 0, chars.length);
        assertEquals(this.candidate, other);
    }

    @Test
    public void testFullByteRangeToFromString() throws Exception
    {
        final byte[] bytes = new byte[256];
        int i = 0;
        for (int v = -128; v < 128; v++)
        {
            bytes[i++] = (byte) v;
        }
        this.candidate = new BlobValue(bytes);
        BlobValue other = new BlobValue();
        final char[] chars = this.candidate.textValue()
                .toCharArray();
        other.fromChars(chars, 0, chars.length);
        assertEquals(this.candidate, other);
    }

    @Test
    public void testGet()
    {
        byte[] sdf = new byte[1];
        assertSame(sdf, BlobValue.get(BlobValue.valueOf(sdf), null));
        assertNull(BlobValue.get(DoubleValue.valueOf(1), null));
        assertNull(BlobValue.get(LongValue.valueOf(1), null));
        assertNull(BlobValue.get(TextValue.valueOf("1"), null));
    }

    @Test
    public void test_toStringAppender()
    {
        assertEquals("B" + _1AF3416, candidate.toStringAppender()
                .toString());
    }

    @Test
    public synchronized void test_perf_switchVsArray()
    {
        List<long[]> times;
        int tries = 0;
        do
        {
            tries++;
            times = new ArrayList<>();
            testPerf('0', times);
            testPerf('1', times);
            testPerf('2', times);
            testPerf('3', times);
            testPerf('4', times);
            testPerf('5', times);
            testPerf('6', times);
            testPerf('7', times);
            testPerf('8', times);
            testPerf('9', times);
            testPerf('a', times);
            testPerf('b', times);
            testPerf('c', times);
            testPerf('d', times);
            testPerf('e', times);
            testPerf('f', times);
        }
        while (!checkNormalVsOptimisedResults(times, "test_perf_switchVsArray", tries));
    }

    private static void testPerf(char c, List<long[]> times)
    {
        long t;
        long switchTime;
        long arrayTime;
        int arrayVal = -1;
        final int LOOPS = LongValueTest.LOOPS;

        // warmup
        for (int i = 0; i < LOOPS; i++)
        {
            arrayVal = decodeHexLsb(c);
            arrayVal = BlobValue.LSB_HEX_VALS[c];
        }

        prepareForPerfTestStep();

        t = System.nanoTime();
        for (int i = 0; i < LOOPS; i++)
        {
            arrayVal = decodeHexLsb(c);
        }
        switchTime = System.nanoTime() - t;

        prepareForPerfTestStep();

        t = System.nanoTime();
        for (int i = 0; i < LOOPS; i++)
        {
            arrayVal = BlobValue.LSB_HEX_VALS[c];
        }
        arrayTime = System.nanoTime() - t;

        assertEquals(decodeHexLsb(c), BlobValue.LSB_HEX_VALS[c]);

        saveQuickestTimes(times, switchTime, arrayTime);
    }

    // this was the old logic to decode
    private static int decodeHexLsb(char c)
    {
        switch(c)
        {
            case '0':
                return 0x0;
            case '1':
                return 0x1;
            case '2':
                return 0x2;
            case '3':
                return 0x3;
            case '4':
                return 0x4;
            case '5':
                return 0x5;
            case '6':
                return 0x6;
            case '7':
                return 0x7;
            case '8':
                return 0x8;
            case '9':
                return 0x9;
            case 'a':
            case 'A':
                return 0xa;
            case 'b':
            case 'B':
                return 0xb;
            case 'c':
            case 'C':
                return 0xc;
            case 'd':
            case 'D':
                return 0xd;
            case 'e':
            case 'E':
                return 0xe;
            case 'f':
            case 'F':
                return 0xf;
        }
        throw new IllegalArgumentException("Unhandled char:" + c);
    }
}
