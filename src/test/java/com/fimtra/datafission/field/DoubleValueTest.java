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
import static com.fimtra.datafission.field.LongValueTest.REPEAT_RUNS;
import static com.fimtra.datafission.field.LongValueTest.checkNormalVsOptimisedResults;
import static com.fimtra.datafission.field.LongValueTest.prepareForPerfTestStep;
import static com.fimtra.datafission.field.LongValueTest.saveQuickestTimes;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.util.StringAppender;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link DoubleValue}
 *
 * @author Ramon Servadei
 */
public class DoubleValueTest
{
    static final int LOOPS = LongValueTest.LOOPS;

    @Before
    public void setUp() throws Exception
    {
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
    public void testEquals()
    {
        assertEquals(new DoubleValue(1.2), new DoubleValue(1.2));
        assertNotEquals(new DoubleValue(1.2), new DoubleValue(1.21));
    }

    @Test
    public void testGetType()
    {
        assertEquals(TypeEnum.DOUBLE, new DoubleValue(1.2).getType());
    }

    @Test
    public void testInitialisedWithNaN()
    {
        assertEquals(NaN, new DoubleValue().doubleValue(), DoubleValueCodecTest.delta);
    }

    @Test
    public void testGet()
    {
        assertEquals(1.0, DoubleValue.get(DoubleValue.valueOf(1), -1), DoubleValueCodecTest.delta);
        assertTrue(Double.isNaN(DoubleValue.get(null, NaN)));
        assertTrue(Double.isNaN(DoubleValue.get(LongValue.valueOf(1), NaN)));
        assertTrue(Double.isNaN(DoubleValue.get(TextValue.valueOf("1"), NaN)));
    }

    @Test
    public void testToString()
    {
        for (int i = -LONG_VALUE_POOL_SIZE; i <= LONG_VALUE_POOL_SIZE; i++)
        {
            assertEquals(Double.toString(i), new DoubleValue(i).textValue());
            assertSame(DoubleValue.valueOf(i)
                    .textValue(), DoubleValue.valueOf(i)
                    .textValue());
        }

        assertEquals("Infinity", DoubleValue.valueOf(POSITIVE_INFINITY)
                .textValue());
        assertEquals("-Infinity", DoubleValue.valueOf(NEGATIVE_INFINITY)
                .textValue());
        assertEquals("NaN", DoubleValue.valueOf(NaN)
                .textValue());
        assertEquals("99.0", DoubleValue.valueOf(99.0)
                .textValue());
        assertEquals("99.0", DoubleValue.valueOf(99.00000000)
                .textValue());
        assertEquals("99.0001", DoubleValue.valueOf(99.0001)
                .textValue());
    }

    @Test
    public void testStringToDouble_largeDecimal()
    {
        assertEquals(Double.parseDouble("0.12345678901234567890"),
                DoubleValueCodec.fromCharArray("0.12345678901234567890".toCharArray(), 0, 22),
                DoubleValueCodecTest.delta);
    }

    @Test
    public void test_copilot_bitwiseChar09()
    {
        for (int i = 0; i < 128; i++)
        {
            char c = (char) i;
            if (c >= '0' && c <= '9')
            {
                assertEquals("Char '" + c + "'", 0, getIsDigitIb(c));
            }
            else
            {
                assertEquals("Char '" + c + "' code=" + i, 1, getIsDigitIb(c));
            }
        }

    }

    private static int getIsDigitIb(char c)
    {
        final int x = (c - '0');
        return ((x | (~(x - 10))) >>> 31);
    }

    @Test
    public synchronized void test_performance_charsToDouble()
    {
        List<long[]> times;
        int tries = 0;
        do
        {
            tries++;
            times = new ArrayList<>();
            final Random random = new Random();
            for (int i = 0; i < 19; i++)
            {
                final double random3dp = (double) random.nextInt(1000) / 1000;
                final String sVal = "" + ((double) random.nextInt(100_000_000) + random3dp);
                doPerfTestCharsToDouble(sVal, times);
            }
        }
        while (!checkNormalVsOptimisedResults(times, "test_performance_charsToDouble", tries));
    }

    @Test
    public void test_performance_doubleToAppender()
    {
        List<long[]> times;
        int tries = 0;
        do
        {
            tries++;
            times = new ArrayList<>();
            doPerfTestWriteToAppender(-2.3056918340057303E18, times);
            //        doPerfTestWriteToString(99.00, times);
            //        doPerfTestWriteToString(99.1, times);
            //        doPerfTestWriteToString(99.01, times);
            //        doPerfTestWriteToString(99.001, times);
            //        doPerfTestWriteToString(99.0001, times);
            //        doPerfTestWriteToString(Double.MAX_VALUE, times);
            //        doPerfTestWriteToString(Double.MIN_VALUE, times);
            //        doPerfTestWriteToString(POSITIVE_INFINITY, times);
            //        doPerfTestWriteToString(NEGATIVE_INFINITY, times);
            //        doPerfTestWriteToString(NaN, times);

            final Random random = new Random();
            for (int i = 0; i < 5; i++)
            {
                final double value = random.nextLong() + random.nextDouble();
                doPerfTestWriteToAppender(value, times);
                doPerfTestWriteToAppender(-value, times);
            }
        }
        while (!checkNormalVsOptimisedResults(times, "test_performance_doubleToAppender", tries));
    }

    private static void doPerfTestWriteToAppender(double value, List<long[]> times)
    {
        //        System.err.println("==================== " + value + "-to-string loops:" + LOOPS + "===============");

        final StringAppender appender = new StringAppender();
        final DoubleValue doubleValue = DoubleValue.valueOf(value);

        for (int i = 0; i < LOOPS; i++)
        {
            appender.setLength(0);
            doubleValue.appendTo(appender);
            appender.setLength(0);
            appender.append(IValue.DOUBLE_CODE)
                    .append(Double.toString(value));
        }

        for (int j = 0; j < REPEAT_RUNS; j++)
        {
            prepareForPerfTestStep();

            long tDoubleValue = System.nanoTime();
            for (int i = 0; i < LOOPS; i++)
            {
                appender.setLength(0);
                doubleValue.appendTo(appender);
            }
            tDoubleValue = System.nanoTime() - tDoubleValue;

            prepareForPerfTestStep();

            long tDouble = System.nanoTime();
            for (int i = 0; i < LOOPS; i++)
            {
                appender.setLength(0);
                appender.append(IValue.DOUBLE_CODE)
                        .append(Double.toString(value));
            }
            tDouble = System.nanoTime() - tDouble;

            final String doubleString = Double.toString(value);
            final String doubleValueString = doubleValue.textValue();
            assertEquals(Double.parseDouble(doubleString), Double.parseDouble(doubleValueString),
                    DoubleValueCodecTest.delta);

            //        System.err.println(" tDouble=" + tDouble + " tDoubleValue=" + tDoubleValue);

            saveQuickestTimes(times, tDouble, tDoubleValue);
        }
    }

    private static void doPerfTestCharsToDouble(String sVal, List<long[]> times)
    {
        final char[] chars = sVal.toCharArray();

        //        System.err.println("==================== " + sVal + "-toDouble loops:" + LOOPS + "===============");

        // warmup
        for (int i = 0; i < LOOPS; i++)
        {
            Double.parseDouble(sVal);
            DoubleValueCodec.fromCharArray(chars, 0, chars.length);
        }

        for (int j = 0; j < REPEAT_RUNS; j++)
        {
            prepareForPerfTestStep();

            long tDoubleValue = System.nanoTime();
            for (int i = 0; i < LOOPS; i++)
            {
                DoubleValueCodec.fromCharArray(chars, 0, chars.length);
            }
            tDoubleValue = System.nanoTime() - tDoubleValue;

            prepareForPerfTestStep();

            long tDouble = System.nanoTime();
            for (int i = 0; i < LOOPS; i++)
            {
                Double.parseDouble(sVal);
            }
            tDouble = System.nanoTime() - tDouble;

            assertEquals(Double.parseDouble(sVal), DoubleValueCodec.fromCharArray(chars, 0, chars.length),
                    DoubleValueCodecTest.delta);

            //        System.err.println(" tDouble=" + tDouble + " tDoubleValue=" + tDoubleValue);

            saveQuickestTimes(times, tDouble, tDoubleValue);
        }
    }
}
