package com.fimtra.datafission.field;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

/**
 * @author Ramon Servadei
 */
public class LongValueCharArrayCodecTest
{
    static final int LOOPS = LongValueTest.LOOPS;

    @Test
    public void test_to_from_charArray()
    {
        final int LOOPS = 1_000_000;
        // size=20 for -ve 19 digits
        final char[] chars = new char[20];

        doToFromCharArrayTest(Long.MAX_VALUE, chars);
        doToFromCharArrayTest(-Long.MAX_VALUE, chars);

        for (long lVal = 0; lVal < LOOPS; lVal++)
        {
            doToFromCharArrayTest(lVal, chars);
        }

        for (long lVal = 0; lVal < LOOPS; lVal++)
        {
            doToFromCharArrayTest(-lVal, chars);
        }
    }

    private static void doToFromCharArrayTest(long lVal, char[] chars)
    {
        final int len = lVal < 0 ? LongValueCharArrayCodec.stringSize(-lVal) + 1 :
                LongValueCharArrayCodec.stringSize(lVal);
        LongValueCharArrayCodec.writeToCharArray(lVal, chars, 0, len);
        assertEquals(lVal, LongValueCharArrayCodec.fromCharArray(chars, 0, len));
    }

    @Test
    public void test_stringSize()
    {
        final int LOOPS = 19;
        for (int i = 0; i < LOOPS; i++)
        {
            long v = 5 * (long) Math.pow(10, i);
            assertEquals("v=" + v, i + 1, LongValueCharArrayCodec.stringSize(v));
        }

        for (int i = 0; i < LOOPS; i++)
        {
            long v = (long) Math.pow(10, i);
            assertEquals("v=" + v, i + 1, LongValueCharArrayCodec.stringSize(v));
        }
    }

    @Test
    public synchronized void test_performance_loop_vs_array()
    {
        long[] times = new long[2];
        doPerfTest_loop_vs_array(5, times);
        doPerfTest_loop_vs_array(5000L, times);
        doPerfTest_loop_vs_array(50000000000000L, times);
        doPerfTest_loop_vs_array(1328623089214211837L, times);
        doPerfTest_loop_vs_array(Long.MAX_VALUE, times);

        final Random random = new Random();
        for (int i = 0; i < 5; i++)
        {
            long lVal = random.nextLong();
            if (lVal < 0)
            {
                lVal = -lVal;
            }
            doPerfTest_loop_vs_array(lVal, times);
        }

        checkLoopVsArrayResults(times);
    }

    private static void checkLoopVsArrayResults(long[] times)
    {
        final double tolerance = 1.8d;
        final long timeWithTolerance = (long) (times[0] * tolerance);
        final String message = "Got total times tLoop=" + times[0] + " (with " + tolerance + " tolerance="
                + timeWithTolerance + ") tArr=" + times[1];
        assertTrue(message, timeWithTolerance > times[1]);
        System.err.println(message);
    }

    private static void doPerfTest_loop_vs_array(long lVal, long[] times)
    {
        System.err.println("============== " + lVal + "-stringSize loops:" + LOOPS + "=============");

        // warmup
        for (int i = 0; i < LOOPS; i++)
        {
            stringSize_loop(lVal);
            LongValueCharArrayCodec.stringSize(lVal);
        }

        prepareForPerfTestRun();

        long tLoop = System.nanoTime();
        for (int i = 0; i < LOOPS; i++)
        {
            stringSize_loop(lVal);
        }
        tLoop = System.nanoTime() - tLoop;

        prepareForPerfTestRun();

        long tArr = System.nanoTime();
        for (int i = 0; i < LOOPS; i++)
        {
            LongValueCharArrayCodec.stringSize(lVal);
        }
        tArr = System.nanoTime() - tArr;

        assertEquals(stringSize_loop(lVal), LongValueCharArrayCodec.stringSize(lVal));

        System.err.println("tLoop=" + tLoop);
        System.err.println(" tArr=" + tArr);

        times[0] += tLoop;
        times[1] += tArr;

    }

    private static void prepareForPerfTestRun()
    {
        LongValueTest.prepareForPerfTestRun();
    }

    // taken from source code for Long.stringSize
    private static int stringSize_loop(long x)
    {
        long p = 10;
        for (int i = 1; i < 19; i++)
        {
            if (x < p)
            {
                return i;
            }
            p = 10 * p;
        }
        return 19;
    }

}