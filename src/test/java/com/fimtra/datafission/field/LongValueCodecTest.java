package com.fimtra.datafission.field;

import static com.fimtra.datafission.field.LongValueTest.REPEAT_RUNS;
import static com.fimtra.datafission.field.LongValueTest.checkNormalVsOptimisedResults;
import static com.fimtra.datafission.field.LongValueTest.prepareForPerfTestStep;
import static com.fimtra.datafission.field.LongValueTest.saveQuickestTimes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

/**
 * @author Ramon Servadei
 */
public class LongValueCodecTest
{
    static final int LOOPS = LongValueTest.LOOPS;

    @Test
    public void test_workbench()
    {
        char[] charArray = ("-3455").toCharArray();
        final long actual = LongValueCodec.fromCharArray(charArray, 0, charArray.length);
        final long expected = Long.parseLong(new String(charArray));
        assertEquals(expected, actual);
    }

    @Test
    public void test_invalidInputs()
    {
        final List<String> invalid = new ArrayList<>();

        invalid.add(null);
        invalid.add("");
        invalid.add(" ");
        invalid.add("+");
        invalid.add("-");
        invalid.add("0..123");
        invalid.add("1249396249097535200000000..123");
        invalid.add("3.1415926535898E2147d483640");
        invalid.add("3.1415926535898E ");
        invalid.add("3.141g");
        invalid.add("-92233720368547758099");
        invalid.add("92233720368547758099");

        char[] charArray = null;
        for (String s : invalid)
        {
            charArray = s == null ? null : s.toCharArray();
            try
            {
                LongValueCodec.fromCharArray(charArray, 0, charArray == null ? 0 : charArray.length);
                fail("Expected NumberFormatException for [" + s + "]");
            }
            catch (NumberFormatException e)
            {
                // ok
            }
        }
    }

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
        final int len =
                lVal < 0 ? LongValueCodec.stringSize(-lVal) + 1 : LongValueCodec.stringSize(lVal);
        LongValueCodec.writeToCharArray(lVal, chars, 0, len);
        assertEquals(lVal, LongValueCodec.fromCharArray(chars, 0, len));
    }

    @Test
    public void test_stringSize()
    {
        final int LOOPS = 19;
        for (int i = 0; i < LOOPS; i++)
        {
            long v = 5 * (long) Math.pow(10, i);
            assertEquals("v=" + v, i + 1, LongValueCodec.stringSize(v));
        }

        for (int i = 0; i < LOOPS; i++)
        {
            long v = (long) Math.pow(10, i);
            assertEquals("v=" + v, i + 1, LongValueCodec.stringSize(v));
        }
    }

    @Test
    public synchronized void test_performance_loop_vs_array()
    {
        List<long[]> times;
        int tries = 0;
        do
        {
            tries++;
            times = new ArrayList<>();
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
        }
        while (!checkNormalVsOptimisedResults(times, "test_performance_loop_vs_array", tries));
    }

    private static void doPerfTest_loop_vs_array(long lVal, List<long[]> times)
    {
        //        System.err.println("============== " + lVal + "-stringSize loops:" + LOOPS + "=============");

        // warmup
        for (int i = 0; i < LOOPS; i++)
        {
            stringSize_loop(lVal);
            LongValueCodec.stringSize(lVal);
        }

        for (int j = 0; j < REPEAT_RUNS; j++)
        {
            prepareForPerfTestStep();

            long tLoop = System.nanoTime();
            for (int i = 0; i < LOOPS; i++)
            {
                stringSize_loop(lVal);
            }
            tLoop = System.nanoTime() - tLoop;

            prepareForPerfTestStep();

            long tArr = System.nanoTime();
            for (int i = 0; i < LOOPS; i++)
            {
                LongValueCodec.stringSize(lVal);
            }
            tArr = System.nanoTime() - tArr;

            assertEquals(stringSize_loop(lVal), LongValueCodec.stringSize(lVal));

            //        System.err.println("tLoop=" + tLoop + " tArr=" + tArr);

            saveQuickestTimes(times, tLoop, tArr);
        }

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