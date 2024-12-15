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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
    static final int LOOPS = 10;
    static final int REPEAT_RUNS = 100;
    static final double tolerance = 1.d;
    static final int max_retry = 4;

    static boolean checkNormalVsOptimisedResults(List<long[]> all_times, String context, int tries)
    {
        final long[] times = computeStats(all_times);
        final long timeWithTolerance = (long) (times[0] * tolerance);
        final String message = context + " got time t_normal=" + times[0] + " (with " + tolerance + " tolerance="
                + timeWithTolerance + ") t_optimised=" + times[1];
        final boolean passed = timeWithTolerance >= times[1];
        if (passed || tries == max_retry)
        {
            assertTrue(message, passed);
            System.err.println(message);
        }
        if (!passed)
        {
            System.err.println("RETRY: " + message);
            System.gc();
        }

        return passed;
    }

    static long[] computeStats(List<long[]> allTimes)
    {
        final List<Long> list1 = new ArrayList<>(allTimes.size());
        final List<Long> list2 = new ArrayList<>(allTimes.size());

        for (long[] allTime : allTimes)
        {
            list1.add(allTime[0]);
            list2.add(allTime[1]);
        }

        // remove min and max
        //        list1.remove(list1.stream()
        //                .min(Long::compare)
        //                .get());
        //        list2.remove(list2.stream()
        //                .min(Long::compare)
        //                .get());
        //        list1.remove(list1.stream()
        //                .max(Long::compare)
        //                .get());
        //        list2.remove(list2.stream()
        //                .max(Long::compare)
        //                .get());
        //
        //        return new long[] { (long) list1.stream()
        //                .mapToLong(Long::longValue)
        //                .average()
        //                .getAsDouble(), (long) list2.stream()
        //                .mapToLong(Long::longValue)
        //                .average()
        //                .getAsDouble() };

        return new long[] {
                //
                list1.stream()
                        .mapToLong(Long::longValue)
                        .min().getAsLong(),
                //
                list2.stream()
                        .mapToLong(Long::longValue)
                        .min().getAsLong() };
    }

    static void saveQuickestTimes(List<long[]> times, long t_0, long t_1)
    {
        times.add(new long[] { t_0, t_1 });
    }

    static void prepareForPerfTestStep()
    {
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

        final long l = LongValueCodec.fromCharArray(charArray, 0, charArray.length);
        System.err.println("Got: " + l);
    }

    @Test
    public synchronized void test_performance_charsToLong()
    {
        List<long[]> times;
        int tries = 0;
        do
        {
            tries++;
            times = new ArrayList<>();
            final Random random = new Random();
            for (int i = 0; i < 5; i++)
            {
                doPerfTestCharsToLong("" + random.nextLong(), times);
            }

        }
        while (!checkNormalVsOptimisedResults(times, "test_performance_charsToLong", tries));
    }

    private static void doPerfTestCharsToLong(String sVal, List<long[]> times)
    {
        final char[] chars = sVal.toCharArray();

        //        System.err.println("==================== " + sVal + "-toLong loops:" + LOOPS + "===============");

        // warmup
        for (int i = 0; i < LOOPS; i++)
        {
            Long.parseLong(sVal);
            LongValueCodec.fromCharArray(chars, 0, chars.length);
        }

        for (int j = 0; j < REPEAT_RUNS; j++)
        {
            prepareForPerfTestStep();

            long tLongValue = System.nanoTime();
            for (int i = 0; i < LOOPS; i++)
            {
                LongValueCodec.fromCharArray(chars, 0, chars.length);
            }
            tLongValue = System.nanoTime() - tLongValue;

            prepareForPerfTestStep();

            long tLong = System.nanoTime();
            for (int i = 0; i < LOOPS; i++)
            {
                Long.parseLong(sVal);
            }
            tLong = System.nanoTime() - tLong;

            assertEquals(Long.parseLong(sVal), LongValueCodec.fromCharArray(chars, 0, chars.length));

            //        System.err.println("tLong=" + tLong + " tLongValue=" + tLongValue);

            saveQuickestTimes(times, tLong, tLongValue);
        }
    }

    @Test
    public synchronized void test_performance_longToAppender()
    {
        List<long[]> times;
        int tries = 0;
        do
        {
            tries++;
            times = new ArrayList<>();
            final Random random = new Random();
            for (int i = 0; i < 5; i++)
            {
                doPerfTestWriteToAppender(random.nextLong(), times);
            }
        }
        while (!checkNormalVsOptimisedResults(times, "test_performance_longToAppender", tries));
    }

    private static void doPerfTestWriteToAppender(long lVal, List<long[]> times)
    {
        //        System.err.println(==================== " + lVal + "-append-to-string loops:" + LOOPS + "===============");

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

        for (int j = 0; j < REPEAT_RUNS; j++)
        {
            prepareForPerfTestStep();

            long tLongValue = System.nanoTime();
            for (int i = 0; i < LOOPS; i++)
            {
                appender.setLength(0);
                longValue.appendTo(appender);
            }
            tLongValue = System.nanoTime() - tLongValue;

            prepareForPerfTestStep();

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

            //        System.err.println("tLong=" + tLong + " tLongValue=" + tLongValue);

            saveQuickestTimes(times, tLong, tLongValue);
        }
    }
}
