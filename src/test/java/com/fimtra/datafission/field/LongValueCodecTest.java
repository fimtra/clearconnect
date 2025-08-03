package com.fimtra.datafission.field;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

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

        doToFromCharArrayTest(Long.MAX_VALUE);
        doToFromCharArrayTest(-Long.MAX_VALUE);

        for (long lVal = 0; lVal < LOOPS; lVal++)
        {
            doToFromCharArrayTest(lVal);
        }

        for (long lVal = 0; lVal < LOOPS; lVal++)
        {
            doToFromCharArrayTest(-lVal);
        }
    }

    private static void doToFromCharArrayTest(long lVal)
    {
        final char[] chars = new char[lVal < 0 ? 20 : 19];
        int start = LongValueCodec.writeToCharArray(lVal, chars);

        // NOTE: writeToCharArray is not 100% symmetrical with fromCharArray
        if (lVal < 0)
        {
            chars[--start] = '-';
        }

        assertEquals(lVal, LongValueCodec.fromCharArray(chars, start, chars.length - start));
    }

    @Test
    public void testArrayVsCompute()
    {
        final int MAX = 10_000_000;
        long t;
        char c;
        long q;
        int r;
        int num = 1234567;
        char[] arr_buf = new char[2];
        char[] com_buf = new char[2];

        long com_time, arr_time;
        long total_com_time = 0, total_arr_time = 0;
        for (int j = 0; j < 10; j++)
        {
            // old array routine from java.long.Long
            t = System.nanoTime();
            for (int i = 0; i < MAX; i++)
            {
                q = num / 100;
                r = (int) (num - (q * 100));
                arr_buf[1] = DigitOnes[r];
                arr_buf[0] = DigitTens[r];
            }
            arr_time = System.nanoTime() - t;
            //            System.err.println("arr_time=" + arr_time);
            total_arr_time += arr_time;

            t = System.nanoTime();
            for (int i = 0; i < MAX; i++)
            {
                q = num / 100;
                r = (int) (num - (q * 100));
                com_buf[1] = (char) (48 + (r % 10));
                com_buf[0] = (char) (48 + (r / 10));
            }
            com_time = System.nanoTime() - t;
            //            System.err.println("com_time=" + com_time);
            total_com_time += com_time;

            assertArrayEquals(arr_buf, com_buf);
        }
        System.err.println("total_arr_time=" + total_arr_time);
        System.err.println("total_com_time=" + total_com_time);
        assertTrue(total_com_time < total_arr_time);
    }

    static final char[] DigitTens = {
            //
            '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
            //
            '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
            //
            '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
            //
            '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
            //
            '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
            //
            '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
            //
            '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
            //
            '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
            //
            '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
            //
            '9', '9', '9', '9', '9', '9', '9', '9', '9', '9', };

    static final char[] DigitOnes = {
            //
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            //
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            //
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            //
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            //
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            //
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            //
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            //
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            //
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            //
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', };
}