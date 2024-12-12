package com.fimtra.datafission.field;

/**
 * Methods to convert native long to char[] and vice-versa. Based on the {@link java.lang.Long}
 * implementations but heavily tweaked to operate with direct char[] (and other optimisations)
 *
 * @author Ramon Servadei
 */
abstract class LongToCharArrayCodec
{
    private static final char[] DigitTens = {
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

    private static final char[] DigitOnes = {
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

    private static final char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

    static void writeToCharArray(long i, char[] buf, int start, int lsDigitPos)
    {
        int r;

        if (i < 0)
        {
            buf[start] = '-';
            i = -i;
        }

        // NOTE: this logic works on the least significant digit to most significant
        //       hence we start at the END of the array and work "backwards" (--lsDigitPos)

        // Get 2 digits/iteration using longs until quotient fits into an int
        if (i > Integer.MAX_VALUE)
        {
            long q;
            do
            {
                q = i / 100;
                // really: r = i - (q * 100);
                r = (int) (i - ((q << 6) + (q << 5) + (q << 2)));
                i = q;
                buf[--lsDigitPos] = DigitOnes[r];
                buf[--lsDigitPos] = DigitTens[r];
            }
            while (i > Integer.MAX_VALUE);
        }

        // Get 2 digits/iteration using ints
        int q2;
        int i2 = (int) i;
        while (i2 >= 65536)
        {
            q2 = i2 / 100;
            // really: r = i2 - (q * 100);
            r = i2 - ((q2 << 6) + (q2 << 5) + (q2 << 2));
            i2 = q2;
            buf[--lsDigitPos] = DigitOnes[r];
            buf[--lsDigitPos] = DigitTens[r];
        }

        // Fall thru to fast mode for smaller numbers
        // assert(i2 <= 65536, i2);
        do
        {
            q2 = (i2 * 52429) >>> (16 + 3);
            r = i2 - ((q2 << 3) + (q2 << 1));  // r = i2-(q2*10) ...
            buf[--lsDigitPos] = digits[r];
            i2 = q2;
        }
        while (i2 != 0);
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
                    if (x < 10L) //1
                    {
                        return 1;
                    }
                    return 2;
                }
                else
                {
                    if (x < 1000L) //3
                    {
                        return 3;
                    }
                    return 4;
                }
            }
            else
            {
                if (x < 1000000L) //6
                {
                    if (x < 100000L) //5
                    {
                        return 5;
                    }
                    else
                    {
                        return 6;
                    }
                }
                else
                {
                    if (x < 10000000L) //7
                    {
                        return 7;
                    }
                    else
                    {
                        return 8;
                    }
                }
            }
        }
        else
        {
            if (x < 1000000000000L) //12
            {
                if (x < 10000000000L) //10
                {
                    if (x < 1000000000L) //9
                    {
                        return 9;
                    }
                    else
                    {
                        return 10;
                    }
                }
                else
                {
                    if (x < 100000000000L) //11
                    {
                        return 11;
                    }
                    else
                    {
                        return 12;
                    }
                }
            }
            else
            {
                if (x < 100000000000000L) //14
                {
                    if (x < 10000000000000L) //13
                    {
                        return 13;
                    }
                    else
                    {
                        return 14;
                    }
                }
                else
                {
                    if (x < 10000000000000000L) //16
                    {
                        if (x < 1000000000000000L) //15
                        {
                            return 15;
                        }
                        else
                        {
                            return 16;
                        }
                    }
                    else
                    {
                        if (x < 100000000000000000L) //17
                        {
                            return 17;
                        }
                        else
                        {
                            if (x < 1000000000000000000L) //18
                            {
                                return 18;
                            }
                            return 19;
                        }
                    }
                }
            }
        }
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
                default:
            }

            // big digit count check to optimise for the majority of the time where we don't need to check
            // hitting +/-MAX_VALUE
            if (len > DIGIT_COUNT_FOR_LIMIT_CHECK)
            {
                final long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
                final long multmin = negative ? POS_MIN_MULTMIN : NEG_MAX_MULTMIN;
                while (i < len)
                {
                    digit = chars[i++] - '0';
                    if (digit < 0 || digit > 9 || (i > DIGIT_COUNT_FOR_LIMIT_CHECK && result < multmin))
                    {
                        throw new NumberFormatException(new String(chars, start, len));
                    }
                    result *= 10;
                    if (i > DIGIT_COUNT_FOR_LIMIT_CHECK && (result < limit + digit))
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
                while (i < len)
                {
                    digit = chars[i++] - '0';
                    if (digit < 0 || digit > 9)
                    {
                        throw new NumberFormatException(new String(chars, start, len));
                    }
                    result *= 10;
                    // NOTE: here we accumulate positively
                    result += digit;
                }
                return negative ? -result : result;
            }
        }
        else
        {
            throw new NumberFormatException(new String(chars, start, len));
        }
    }

    private LongToCharArrayCodec()
    {
        // not for construction
    }
}
