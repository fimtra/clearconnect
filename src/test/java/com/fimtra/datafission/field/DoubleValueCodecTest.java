package com.fimtra.datafission.field;

import static com.fimtra.datafission.field.DoubleValueCodec.MAX_SMALL_TEN;
import static com.fimtra.datafission.field.DoubleValueCodec.writeToCharArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.fimtra.util.StringAppender;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for the {@link DoubleValueCodec}
 *
 * @author Ramon Servadei
 */
public class DoubleValueCodecTest
{
    static final double delta = 0.000000000000000000000000001d;

    static void _testPositiveAndNegative(String d)
    {
        _test_double_to_string_to_double(d);
        if (Character.isDigit(d.charAt(0)))
        {
            _test_double_to_string_to_double("-" + d);
            _test_double_to_string_to_double("+" + d);
        }
    }

    private static final ThreadLocal<StringAppender> STRING_APPENDER_THREAD_LOCAL =
            ThreadLocal.withInitial(StringAppender::new);

    private static String toString(double d)
    {
        final StringAppender stringAppender = STRING_APPENDER_THREAD_LOCAL.get();
        stringAppender.setLength(0);
        writeToCharArray(d, stringAppender);
        return stringAppender.toString();
    }

    private static void _test_double_to_string_to_double(String stringValue)
    {
        char[] charArray = stringValue.toCharArray();

        final double codec_dVal = DoubleValueCodec.fromCharArray(charArray, 0, charArray.length);
        // check we can rebuild from our own codec value
        final char[] codec_charArray = toString(codec_dVal).toCharArray();
        assertEquals(codec_dVal, DoubleValueCodec.fromCharArray(codec_charArray, 0, codec_charArray.length),
                delta);

        // check java double matches codec double
        assertEquals(Double.parseDouble(stringValue), codec_dVal, delta);

        // check reverse Double compatibility
        charArray = Double.toString(codec_dVal)
                .toCharArray();
        assertEquals(codec_dVal, DoubleValueCodec.fromCharArray(charArray, 0, charArray.length), delta);
    }

    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void test_invalidInputs()
    {
        final List<String> invalid = new ArrayList<>();

        invalid.add("Na");
        invalid.add("NAN");
        invalid.add("Nan");
        invalid.add("Infinit");
        invalid.add("INfinity");
        invalid.add("InFinity");
        invalid.add("InfInity");
        invalid.add("InfiNity");
        invalid.add("InfinIty");
        invalid.add("InfiniTy");
        invalid.add("InfinitY");
        invalid.add("12.345.6789");
        invalid.add("00.12.6789");
        invalid.add("00.123456789123456789.89");
        invalid.add("0f0.123456789123456789.89");
        invalid.add("");
        invalid.add(" ");
        invalid.add("+");
        invalid.add("-");
        invalid.add("0..123");
        invalid.add("1234567890..1");
        invalid.add("1249396249097535200000000..123");
        invalid.add("3.1415926535898E2147d483640");
        invalid.add("3.1415926535898E ");
        invalid.add("3.141g");

        char[] charArray = null;
        for (String s : invalid)
        {
            charArray = s.toCharArray();
            try
            {
                DoubleValueCodec.fromCharArray(charArray, 0, charArray.length);
                fail("Expected NumberFormatException for [" + s + "]");
            }
            catch (NumberFormatException e)
            {
                // ok
            }
        }
    }

    @Test
    public void test_workbench()
    {
        String dVal = "12345678901234.567890123e-200";

        final char[] charArray = dVal.toCharArray();
        final double actual = DoubleValueCodec.fromCharArray(charArray, 0, charArray.length);
        final double expected = Double.parseDouble(new String(charArray));
        System.err.println("test_workbench expected=" + expected);
        assertEquals(expected, actual, delta);
        final StringAppender stringAppender = new StringAppender();
        writeToCharArray(expected, stringAppender);
        System.err.println("test_workbench stringAppender.toString()=" + stringAppender);
    }

    @Test
    public void test_specialCase_roundup_1()
    {
        final DoubleValueCodec.DoubleToString candidate = new DoubleValueCodec.DoubleToString();
        int i = 0;
        candidate.nDigits = 6;
        candidate.digits[i++] = '1';
        candidate.digits[i++] = '9';
        candidate.digits[i++] = '9';
        candidate.digits[i++] = '9';
        candidate.digits[i++] = '9';
        candidate.digits[i++] = '9';

        final int prevDecExp = candidate.decExponent;
        candidate.roundup();

        assertEquals('2', (candidate.digits[candidate.firstDigitIndex]));
        assertEquals(prevDecExp, candidate.decExponent);
    }

    @Test
    public void test_specialCase_roundup_2()
    {
        final DoubleValueCodec.DoubleToString candidate = new DoubleValueCodec.DoubleToString();
        int i = 0;
        candidate.nDigits = 6;
        candidate.digits[i++] = '9';
        candidate.digits[i++] = '9';
        candidate.digits[i++] = '9';
        candidate.digits[i++] = '9';
        candidate.digits[i++] = '9';
        candidate.digits[i++] = '9';

        final int prevDecExp = candidate.decExponent;
        candidate.roundup();

        assertEquals('1', (candidate.digits[candidate.firstDigitIndex]));
        assertEquals(prevDecExp + 1, candidate.decExponent);
    }

    @Test
    public void test_specialCase_populateAppender()
    {
        final DoubleValueCodec.DoubleToString candidate = new DoubleValueCodec.DoubleToString();
        int i = 0;
        candidate.digits[i++] = '1';
        candidate.digits[i++] = '1';
        for (; i < candidate.digits.length; i++)
        {
            candidate.digits[i] = '0';
        }

        _test_populateAppender(candidate, 6, 0, "000000.0");
        _test_populateAppender(candidate, 6, 1, "100000.0");
        _test_populateAppender(candidate, 6, 2, "110000.0");
        _test_populateAppender(candidate, 6, 8, "110000.00");

        _test_populateAppender(candidate, 0, 0, "0.");
        _test_populateAppender(candidate, 0, 1, "0.1");
        _test_populateAppender(candidate, 0, 2, "0.11");
        _test_populateAppender(candidate, 0, 3, "0.110");

        _test_populateAppender(candidate, -1, 0, "0.0");
        _test_populateAppender(candidate, -1, 1, "0.01");
        _test_populateAppender(candidate, -1, 2, "0.011");
        _test_populateAppender(candidate, -1, 3, "0.0110");

        _test_populateAppender(candidate, -3, 0, "1.0E-4");
        _test_populateAppender(candidate, -3, 1, "1.0E-4");
        _test_populateAppender(candidate, -3, 2, "1.1E-4");
        _test_populateAppender(candidate, -3, 3, "1.10E-4");
    }

    private void _test_populateAppender(DoubleValueCodec.DoubleToString candidate, int decExp, int nDigits,
            String expected)
    {
        candidate.decExponent = decExp;
        candidate.nDigits = nDigits;
        final StringAppender stringAppender = new StringAppender();
        candidate.accept(stringAppender);
        assertEquals(expected, stringAppender.toString());
    }

    @Ignore
    @Test
    public void test_100_million_takes_5mins()
    {
        double current = 0;
        Random random = new Random();
        while (current < 100_000_000d)
        {
            // check to-from for this "integral" and a random number
            _testPositiveAndNegative(Double.toString(current + random.nextDouble()));

            if (++current % 1_000_000 == 0)
            {
                System.err.println("Completed " + current);
            }
        }
    }

    @Test
    public void test_MAIN()
    {
        _testPositiveAndNegative(Double.toString(Double.NaN));
        _testPositiveAndNegative(Double.toString(Double.POSITIVE_INFINITY));
        _testPositiveAndNegative(Double.toString(Double.NEGATIVE_INFINITY));
        _testPositiveAndNegative(Double.toString(Double.MAX_VALUE));
        _testPositiveAndNegative(Double.toString(Double.MIN_VALUE));

        _testPositiveAndNegative("0");
        _testPositiveAndNegative("0.0");
        _testPositiveAndNegative("00000000.0");
        _testPositiveAndNegative("00000000000000000.0");
        _testPositiveAndNegative("00000000.01");
        _testPositiveAndNegative("00000000000000000000000000000000.01");

        _testPositiveAndNegative("2.30569183400573030E18");
        _testPositiveAndNegative("2.3056918340057303E18");
        _testPositiveAndNegative("1.0");
        _testPositiveAndNegative("2047.0");
        _testPositiveAndNegative("100.123456789d");
        _testPositiveAndNegative("100.123d");

        // test endings
        _testPositiveAndNegative("12345678d");
        _testPositiveAndNegative("12345678D");
        _testPositiveAndNegative("12345678f");
        _testPositiveAndNegative("12345678F");
        _testPositiveAndNegative("1234567890d");
        _testPositiveAndNegative("12345678901d");
        _testPositiveAndNegative("012493962490975352d");
        _testPositiveAndNegative("12345678.1d");
        _testPositiveAndNegative("123456789.1D");
        _testPositiveAndNegative("1234567890.1d");
        _testPositiveAndNegative("12345678901.1D");
        _testPositiveAndNegative("1234567890123456789.1d");

        // some exponents
        _testPositiveAndNegative("1.23456789e8");
        _testPositiveAndNegative("1.23456789e136");
        _testPositiveAndNegative("3.1415926535898E18");
        _testPositiveAndNegative("3.1415926535898E+18");
        _testPositiveAndNegative("3.1415926535898E-18");
        _testPositiveAndNegative("3.1415926535898E20000");
        // check exponent limits
        _testPositiveAndNegative("3.1415926535898e214748364");
        _testPositiveAndNegative("3.1415926535898e2147483640");
        _testPositiveAndNegative("31415926535898E" + (MAX_SMALL_TEN + 1));
        _testPositiveAndNegative("3.14159e-200");
        _testPositiveAndNegative("3.14159e-400");
        _testPositiveAndNegative("0.0000001e-400");

        _testPositiveAndNegative("0.08212310038385495d");
        _testPositiveAndNegative("0.12345678d");
        _testPositiveAndNegative("0.12493962490975352d");
        _testPositiveAndNegative("01.12493962490975352d");
        _testPositiveAndNegative("1.12493962490975352d");
        _testPositiveAndNegative("0.12345678d");
        _testPositiveAndNegative("0.12493962490975352d");

        _testPositiveAndNegative("100000000100000000100000000100000000.01");

        String number = "1";
        for (int i = 1; i < 20; i++)
        {
            _testPositiveAndNegative(number);
            number += "0";
        }

        final Random random = new Random();
        for (int i = 0; i < 1000; i++)
        {
            final double d = random.nextDouble();
            _testPositiveAndNegative(Double.toString(d));
            final double bigValue = random.nextLong() + d;
            _testPositiveAndNegative(Double.toString(bigValue));
        }

        // test some BIG and super-small numbers

        // NOTE: this tests a BIG number with a decExp that is < MAX exp (308)
        _testPositiveAndNegative(
                "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                        + "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                        + "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                        // decimal here
                        + ".1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                        + "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                        + "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                        + "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                        + "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                        + "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                        + "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                        + "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                        // some extra to take us over the 1100 digit limit
                        + "1234567890");

        _testPositiveAndNegative("3.14E300");
        _testPositiveAndNegative("3.14E+300");
        _testPositiveAndNegative("3.14E-300");
        _testPositiveAndNegative("3.14E307");
        _testPositiveAndNegative("3.14E-324");
    }

    // Lifted from the OpenJDK FloatingDecimal tests at:
    // https://github.com/openjdk/jdk/blob/master/test/jdk/jdk/internal/math/FloatingDecimal/TestFloatingDecimal.java
    @Test
    public void test_vsBigDecimal()
    {
        final int NUM_RANDOM_TESTS = 100_000;
        final Random rnd = new Random();
        StringAppender stringAppender = new StringAppender();
        String dec;
        char[] chars;
        BigDecimal bd;
        String full;

        for (int i = 0; i < NUM_RANDOM_TESTS; i++)
        {
            double[] d = { rnd.nextLong(), rnd.nextInt() * rnd.nextGaussian(), rnd.nextGaussian(),
                    rnd.nextDouble() * Double.MAX_VALUE, };
            for (double v : d)
            {
                stringAppender.setLength(0);
                DoubleValueCodec.writeToCharArray(v, stringAppender);
                dec = stringAppender.toString();
                chars = dec.toCharArray();
                assertEquals(new BigDecimal(dec).doubleValue(),
                        DoubleValueCodec.fromCharArray(chars, 0, chars.length), delta);

                bd = new BigDecimal(v);
                full = bd.toString();
                chars = full.toCharArray();
                stringAppender.setLength(0);
                assertEquals(bd.doubleValue(), DoubleValueCodec.fromCharArray(chars, 0, chars.length), delta);
            }
        }
    }
}