package com.fimtra.datafission.field;

/**
 * Methods to convert native long to char[] and vice-versa. Based on the {@link java.lang.Long}
 * implementations but heavily tweaked to operate with direct char[] (and other optimisations)
 *
 * @author Ramon Servadei
 */
abstract class LongValueCodec
{
    /**
     * @param i
     * @param buf
     * @return the index where the data starts, it ends at the end of the char[]
     */
    static int writeToCharArray(long i, char[] buf)
    {
        if (i < 0)
        {
            i = -i;
        }

        int lsDigitPos = buf.length;
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

        return lsDigitPos;
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

    private LongValueCodec()
    {
        // not for construction
    }
}
