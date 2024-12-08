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
package com.fimtra.datafission.field;

import static com.fimtra.datafission.DataFissionProperties.Values.LONG_VALUE_POOL_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.locks.LockSupport;

import com.fimtra.datafission.IValue;
import com.fimtra.util.StringAppender;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IValue.TypeEnum;

/**
 * Tests for the {@link LongValue}
 *
 * @author Ramon Servadei
 */
public class LongValueTest
{
    static final int LOOPS = 1_000;

    static void prepareForPerfTestRun()
    {
        System.gc();
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        LockSupport.parkNanos(1_000_000);
    }

    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testEquals()
    {
        assertEquals(LongValue.valueOf(1), LongValue.valueOf(1));
        assertFalse(LongValue.valueOf(1)
                .equals(LongValue.valueOf(11)));
    }

    @Test
    public void testGetType()
    {
        assertEquals(TypeEnum.LONG, LongValue.valueOf(1)
                .getType());
    }

    @Test
    public void testCache()
    {
        for (int i = -LONG_VALUE_POOL_SIZE; i <= LONG_VALUE_POOL_SIZE; i++)
        {
            assertEquals(i, LongValue.valueOf(i)
                    .longValue());
            assertSame(LongValue.valueOf(i), LongValue.valueOf(i));
            assertSame(LongValue.valueOf(i)
                    .textValue(), LongValue.valueOf(i)
                    .textValue());
            assertSame(LongValue.valueOf(i)
                    .toString(), LongValue.valueOf(i)
                    .toString());
        }
    }

    @Test
    public void test_textValue()
    {
        assertEquals("1234", LongValue.valueOf(1234)
                .textValue());
        assertEquals("-1234", LongValue.valueOf(-1234)
                .textValue());
        assertEquals("1234567", LongValue.valueOf(1234567)
                .textValue());
        assertEquals("-1234567", LongValue.valueOf(-1234567)
                .textValue());
    }
    @Test
    public void test_toString()
    {
        assertEquals("L1234", LongValue.valueOf(1234)
                .toString());
        assertEquals("L-1234", LongValue.valueOf(-1234)
                .toString());
        assertEquals("L1234567", LongValue.valueOf(1234567)
                .toString());
        assertEquals("L-1234567", LongValue.valueOf(-1234567)
                .toString());
    }

    @Test
    public void testBeyondCache()
    {
        int lVal = -LONG_VALUE_POOL_SIZE - 1;
        assertEquals(lVal, LongValue.valueOf(lVal)
                .longValue());
        assertNotSame(LongValue.valueOf(lVal), LongValue.valueOf(lVal));
        assertNotSame(LongValue.valueOf(lVal)
                .textValue(), LongValue.valueOf(lVal)
                .textValue());

        lVal = LONG_VALUE_POOL_SIZE + 1;
        assertEquals(lVal, LongValue.valueOf(lVal)
                .longValue());
        assertNotSame(LongValue.valueOf(lVal), LongValue.valueOf(lVal));
        assertNotSame(LongValue.valueOf(lVal)
                .textValue(), LongValue.valueOf(lVal)
                .textValue());
    }

    @Test
    public void testGet()
    {
        assertEquals(1, LongValue.get(LongValue.valueOf(1), -1));
        assertEquals(-1, LongValue.get(DoubleValue.valueOf(1), -1));
        assertEquals(-1, LongValue.get(TextValue.valueOf("1"), -1));
        assertEquals(-1, LongValue.get(null, -1));
    }

    @Test(expected = NumberFormatException.class)
    public void test_parseLong_exception_nonNumber()
    {
        doExceptionTest("duff");
    }

    @Test(expected = NumberFormatException.class)
    public void test_parseLong_exception_BIG_INT()
    {
        doExceptionTest("12345678901234567890");
    }

    @Test(expected = NumberFormatException.class)
    public void test_parseLong_exception_MAX_VALUE_plus_1()
    {
        // this is Long.MAX_VALUE + 1
        doExceptionTest("9223372036854775808");
    }

    @Test(expected = NumberFormatException.class)
    public void test_parseLong_exception_MIN_VALUE_minus_1()
    {
        // this is Long.MIN_VALUE - 1
        doExceptionTest("-9223372036854775809");
    }

    @Test(expected = NumberFormatException.class)
    public void test_parseLong_exception_just_plus()
    {
        doExceptionTest("+");
    }

    @Test(expected = NumberFormatException.class)
    public void test_parseLong_exception_just_plus_something()
    {
        doExceptionTest("+(");
    }

    @Test(expected = NumberFormatException.class)
    public void test_parseLong_exception_just_minus()
    {
        doExceptionTest("-");
    }

    private static void doExceptionTest(String x)
    {
        final char[] charArray = x.toCharArray();

        // verify normal Long fails
        try
        {
            Long.parseLong(new String(charArray));
            fail("expected NFE");
        }
        catch (NumberFormatException e)
        {
        }

        final long l = LongValueCharArrayCodec.fromCharArray(charArray, 0, charArray.length);
        System.err.println("Got: " + l);
    }

    @Test
    public synchronized void test_performance_charsToLong()
    {
        long[] times = new long[2];

        String number = "1";
        // 19 is the max digit count for long
        for (int i = 0; i < 19; i++)
        {
            doPerfTestCharsToLong(number, times);
            number += "0";
        }

        checkLongVsLongValueResults(times);

        times = new long[2];
        // now negative numbers
        number = "-1";
        for (int i = 0; i < 19; i++)
        {
            doPerfTestCharsToLong(number, times);
            number += "0";
        }

        checkLongVsLongValueResults(times);
    }

    private static void checkLongVsLongValueResults(long[] times)
    {
        final double tolerance = 1.8d;
        final long timeWithTolerance = (long) (times[0] * tolerance);
        final String message = "Got total times tLong=" + times[0] + " (with " + tolerance + " tolerance="
                + timeWithTolerance + ") tLongValue=" + times[1];
        assertTrue(message, timeWithTolerance > times[1]);
        System.err.println(message);
    }

    private static void doPerfTestCharsToLong(String sVal, long[] times)
    {
        final char[] chars = sVal.toCharArray();

        System.err.println("==================== " + sVal + "-toLong loops:" + LOOPS + "===============");

        // warmup
        for (int i = 0; i < LOOPS; i++)
        {
            Long.parseLong(sVal);
            LongValueCharArrayCodec.fromCharArray(chars, 0, chars.length);
        }

        prepareForPerfTestRun();

        long tLongValue = System.nanoTime();
        for (int i = 0; i < LOOPS; i++)
        {
            LongValueCharArrayCodec.fromCharArray(chars, 0, chars.length);
        }
        tLongValue = System.nanoTime() - tLongValue;

        prepareForPerfTestRun();

        long tLong = System.nanoTime();
        for (int i = 0; i < LOOPS; i++)
        {
            Long.parseLong(sVal);
        }
        tLong = System.nanoTime() - tLong;

        assertEquals(Long.parseLong(sVal), LongValueCharArrayCodec.fromCharArray(chars, 0, chars.length));

        System.err.println("     tLong=" + tLong);
        System.err.println("tLongValue=" + tLongValue);

        times[0] += tLong;
        times[1] += tLongValue;
    }

    @Test
    public synchronized void test_performance_writeToString()
    {
        long[] times = new long[2];

        long l = 1;
        for (int i = 0; i < 19; i++)
        {
            doPerfTestWriteToString(l, times);
            l *= 10;

            prepareForPerfTestRun();
            LockSupport.parkNanos(10_000_000);
        }

        checkLongVsLongValueResults(times);

        times = new long[2];
        // now negative numbers
        l = -1;
        for (int i = 0; i < 19; i++)
        {
            doPerfTestWriteToString(l, times);
            l *= 10;

            prepareForPerfTestRun();
            LockSupport.parkNanos(10_000_000);
        }

        checkLongVsLongValueResults(times);
    }

    private static void doPerfTestWriteToString(long lVal, long[] times)
    {
        System.err.println(
                "==================== " + lVal + "-append-to-string loops:" + LOOPS + "===============");

        final StringAppender appender = new StringAppender();
        final LongValue longValue = LongValue.valueOf(lVal);

        // warmup
        for (int i = 0; i < LOOPS; i++)
        {
            appender.setLength(0);
            longValue.appendTo(appender);
            appender.setLength(0);
            appender.append(IValue.LONG_CODE)
                    .append(Long.toString(lVal));
        }

        prepareForPerfTestRun();

        long tLongValue = System.nanoTime();
        for (int i = 0; i < LOOPS; i++)
        {
            appender.setLength(0);
            longValue.appendTo(appender);
        }
        tLongValue = System.nanoTime() - tLongValue;

        prepareForPerfTestRun();

        long tLong = System.nanoTime();
        for (int i = 0; i < LOOPS; i++)
        {
            appender.setLength(0);
            appender.append(IValue.LONG_CODE)
                    .append(Long.toString(lVal));
        }
        tLong = System.nanoTime() - tLong;

        appender.setLength(0);
        final String longValueToString = longValue.appendTo(appender)
                .toString();

        appender.setLength(0);
        final String longToString = appender.append(IValue.LONG_CODE)
                .append(Long.toString(lVal))
                .toString();
        assertEquals(longToString, longValueToString);

        System.err.println("     tLong=" + tLong);
        System.err.println("tLongValue=" + tLongValue);

        times[0] += tLong;
        times[1] += tLongValue;
    }
}
