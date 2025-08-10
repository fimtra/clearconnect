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
        final int len = lVal < 0 ? LongValueCodec.stringSize(-lVal) + 1 : LongValueCodec.stringSize(lVal);
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

    // ====================
    @Test
    public void compare_stringSize_implementations()
    {
        long[] testValues = { 0L, 1L,

                9L, 10L,

                99L, 100L,

                999L, 1000L,

                9999L, 10_000L,

                99_999L, 100_000L,

                999_999L, 1_000_000L,

                9_999_999L, 10_000_000L,

                99_999_999L, 100_000_000L,

                999_999_999L, 1_000_000_000L,

                9_999_999_999L, 10_000_000_000L,

                99_999_999_999L, 100_000_000_000L,

                999_999_999_999L, 1_000_000_000_000L,

                9_999_999_999_999L, 10_000_000_000_000L,

                99_999_999_999_999L, 100_000_000_000_000L,

                999_999_999_999_999L, 1_000_000_000_000_000L,

                9_999_999_999_999_999L, 10_000_000_000_000_000L,

                99_999_999_999_999_999L, 100_000_000_000_000_000L,

                999_999_999_999_999_999L, 1_000_000_000_000_000_000L };

        int iterations = 10_000_000;
        int warmupIterations = iterations;

        // Warmup
        for (int i = 0; i < warmupIterations; i++)
        {
            for (long value : testValues)
            {
                stringSize(value);
                stringSize_classic(value);
                stringSize_copilot(value);
            }
        }

        // Timing
        long startOriginal = System.nanoTime();
        for (int i = 0; i < iterations; i++)
        {
            for (long value : testValues)
            {
                stringSize(value);
            }
        }
        long timeOriginal = System.nanoTime() - startOriginal;

        long startClassic = System.nanoTime();
        for (int i = 0; i < iterations; i++)
        {
            for (long value : testValues)
            {
                stringSize_classic(value);
            }
        }
        long timeClassic = System.nanoTime() - startClassic;

        long startCopilot = System.nanoTime();
        for (int i = 0; i < iterations; i++)
        {
            for (long value : testValues)
            {
                stringSize_copilot(value);
            }
        }
        long timeCopilot = System.nanoTime() - startCopilot;

        // Verify results
        for (long value : testValues)
        {
            int sizeOriginal = stringSize(value);
            int sizeClassic = stringSize_classic(value);
            int sizeCopilot = stringSize_copilot(value);

            assertEquals("Classic implementation mismatch for value " + value, sizeOriginal, sizeClassic);
            assertEquals("Copilot implementation mismatch for value " + value, sizeOriginal, sizeCopilot);
        }

        // Report average time per operation
        long opsCount = (long) iterations * testValues.length;
        System.out.printf(
                "Average time per operation (ns):%n" + "Original: %.2f%nClassic: %.2f%nCopilot: %.2f%n",
                (double) timeOriginal / opsCount, (double) timeClassic / opsCount,
                (double) timeCopilot / opsCount);
    }

    static int stringSize_classic(long x)
    {
        long p = 10;
        for (int i = 1; i < 19; i++)
        {
            if (x < p)
            {
                return i;
            }
            p = (p << 3) + (p << 1);
        }
        return 19;
    }

    static int stringSize_copilot(long x)
    {
        if (x < 100000)
        {
            if (x < 100)
            {
                return x < 10 ? 1 : 2;
            }
            if (x < 10000)
            {
                return x < 1000 ? 3 : 4;
            }
            return 5;
        }

        if (x < 10000000000L)
        {
            if (x < 10000000)
            {
                if (x < 1000000)
                {
                    return 6;
                }
                return 7;
            }
            if (x < 1000000000)
            {
                if (x < 100000000)
                {
                    return 8;
                }
                return 9;
            }
            return 10;
        }

        if (x < 1000000000000000L)
        {
            if (x < 1000000000000L)
            {
                if (x < 100000000000L)
                {
                    return 11;
                }
                return 12;
            }
            if (x < 100000000000000L)
            {
                if (x < 10000000000000L)
                {
                    return 13;
                }
                return 14;
            }
            return 15;
        }

        if (x < 100000000000000000L)
        {
            if (x < 10000000000000000L)
            {
                return 16;
            }
            return 17;
        }
        if (x < 1000000000000000000L)
        {
            return 18;
        }
        return 19;
    }

    static int stringSize(long x)
    {
        return LongValueCodec.stringSize(x);
    }

}