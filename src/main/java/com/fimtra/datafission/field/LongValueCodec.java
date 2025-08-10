package com.fimtra.datafission.field;

/**
 * Methods to convert native long to char[] and vice-versa. Based on the {@link java.lang.Long}
 * implementations but heavily tweaked to operate with direct char[] (and other optimisations)
 *
 * @author Ramon Servadei
 */
abstract class LongValueCodec
{
    static void writeToCharArray(long i, char[] buf, int start, int lsDigitPos)
    {
        if (i < 0)
        {
            buf[start] = '-';
            i = -i;
        }

        long q;
        int r;

        // R.S. NOTE:
        // This logic works on the least significant digit to most significant
        // hence we start at the END of the array and work "backwards" (--lsDigitPos)

        // Get 2 digits/iteration using longs until quotient fits into a short
        while (i > 65535)
        {
            q = i / 100;
            r = (int) (i - (q * 100));
            i = q;
            // note: compute is faster than array access - CPU cycles faster than memory access for array
            buf[--lsDigitPos] = (char) (48 + (r % 10));
            buf[--lsDigitPos] = (char) (48 + (r / 10));
        }

        // Get 2 digits/iteration using ints
        int q2;
        int i2 = (int) i;

        // Fall thru to fast mode for smaller numbers
        // assert(i2 <= 65536, i2);
        do
        {
            q2 = (i2 * 52429) >>> 19;
            r = i2 - (q2 * 10);
            // note: compute is faster than array access - CPU cycles faster than memory access for array
            buf[--lsDigitPos] = (char) (48 + r);
            i2 = q2;
        }
        while (i2 != 0);
    }

    private static final long NEG_MAX_MULTMIN = -Long.MAX_VALUE / 10;
    private static final long POS_MIN_MULTMIN = Long.MIN_VALUE / 10;
    private static final int DIGIT_COUNT_FOR_LIMIT_CHECK = 17;

    /**
     * This is taken from {@link Long#parseLong(String, int)}  but with some further optimisations
     */
    static long fromCharArray(char[] chars, int start, int length) throws NumberFormatException
    {
        if (chars == null)
        {
            throw new NumberFormatException("null");
        }

        long result = 0;
        boolean negative = false;
        int i = start;
        final int len = start + length;
        int digit;

        if (len > 0)
        {
            switch(chars[i])
            {
                case '-':
                    negative = true;
                case '+':
                    i++;
                    if (len == 1)
                    {
                        // Cannot have lone "+" or "-"
                        throw new NumberFormatException(new String(chars, start, len));
                    }
            }

            // big digit count check to optimise for the majority of the time where we don't need to check
            // hitting +/-MAX_VALUE
            int v;
            if (len > DIGIT_COUNT_FOR_LIMIT_CHECK)
            {
                final long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
                final long multmin = negative ? POS_MIN_MULTMIN : NEG_MAX_MULTMIN;
                while (i <= DIGIT_COUNT_FOR_LIMIT_CHECK)
                {
                    digit = chars[i++];
                    // (old: if (digit < 48 || digit > 57))
                    // This is typically faster as it uses only two operations and one branch instead of two comparisons and two branches
                    v = digit - 48;
                    if ((v & 0xFFFF) > 9)
                    {
                        throw new NumberFormatException(new String(chars, start, len));
                    }
                    result *= 10;
                    // Accumulating negatively avoids surprises near MAX_VALUE
                    result -= v;
                }
                while (i != len)
                {
                    digit = chars[i++];
                    v = digit - 48;
                    if ((v & 0xFFFF) > 9 || result < multmin)
                    {
                        throw new NumberFormatException(new String(chars, start, len));
                    }
                    result *= 10;
                    digit = v;
                    if (result < limit + digit)
                    {
                        throw new NumberFormatException(new String(chars, start, len));
                    }
                    // Accumulating negatively avoids surprises near MAX_VALUE
                    result -= digit;
                }
                return negative ? result : -result;
            }
            else
            {
                // with digits < digitCountForLimitCheck, no way we can exceed the +/-MAX_VALUE for a long
                // so we have a straight path to the finish
                while (i != len)
                {
                    digit = chars[i++];
                    // (old: if (digit < 48 || digit > 57))
                    // This is typically faster as it uses only two operations and one branch instead of two comparisons and two branches
                    v = digit - 48;
                    if ((v & 0xFFFF) > 9)
                    {
                        throw new NumberFormatException(new String(chars, start, len));
                    }
                    result *= 10;
                    // NOTE: here we accumulate positively
                    result += v;
                }
                return negative ? -result : result;
            }
        }
        else
        {
            throw new NumberFormatException(new String(chars, start, len));
        }
    }

    /**
     * Uses a binary search algorithm to compute the number of digits
     */
    static int stringSize(long x)
    {
        if (x < 100000000L) //8
        {
            if (x < 10000L) //4
            {
                if (x < 100L) //2
                {
                    //1
                    return x < 10L ? 1 : 2;
                }
                else
                {
                    //3
                    return x < 1000L ? 3 : 4;
                }
            }
            else
            {
                if (x < 1000000L) //6
                {
                    //5
                    return x < 100000L ? 5 : 6;
                }
                else
                {
                    //7
                    return x < 10000000L ? 7 : 8;
                }
            }
        }
        else
        {
            if (x < 1000000000000L) //12
            {
                if (x < 10000000000L) //10
                {
                    //9
                    return x < 1000000000L ? 9 : 10;
                }
                else
                {
                    //11
                    return x < 100000000000L ? 11 : 12;
                }
            }
            else
            {
                if (x < 100000000000000L) //14
                {
                    //13
                    return x < 10000000000000L ? 13 : 14;
                }
                else
                {
                    if (x < 10000000000000000L) //16
                    {
                        //15
                        return x < 1000000000000000L ? 15 : 16;
                    }
                    else
                    {
                        if (x < 100000000000000000L) //17
                        {
                            return 17;
                        }
                        else
                        {
                            //18
                            return x < 1000000000000000000L ? 18 : 19;
                        }
                    }
                }
            }
        }
    }

    private LongValueCodec()
    {
        // not for construction
    }
}
