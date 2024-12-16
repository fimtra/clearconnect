package com.fimtra.datafission.field;

import static com.fimtra.datafission.field.LongValueCodec.DigitOnes;
import static com.fimtra.datafission.field.LongValueCodec.DigitTens;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.fimtra.util.StringAppender;
import sun.misc.DoubleConsts;
import sun.misc.FDBigInteger;

/**
 * Based on the {@link sun.misc.FloatingDecimal} java source code to allow use of {@link StringAppender}. A
 * lot of tidy up was done on the original code and non-relevant methods/classes have been removed (we only
 * want to focus on double-to-string and vice-versa).
 * <br>
 * This does not support HEX format.
 */
class DoubleValueCodec
{
    /*
    R.S. NOTE:
    I wont pretend that I fully understand all the IEEE arithmetic happening here, my focus has been
    to optimise for reducing char[] create/copy and other minor performance optimisations. The bulk of the
    actual conversion logic is unchanged w.r.t. FloatingDecimal algorithm.
     */

    private static final long[] LONG_5_POW =
              { 1L,
                5L,
                5L * 5,
                5L * 5 * 5,
                5L * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5, };

    //
    // Constants of the implementation;
    // most are IEEE-754 related.
    // (There are more really boring constants at the end.)
    //
    private static final int EXP_SHIFT = DoubleConsts.SIGNIFICAND_WIDTH - 1;
    private static final long FRACT_HOB = (1L << EXP_SHIFT); // assumed High-Order bit
    private static final long EXP_ONE = ((long) DoubleConsts.EXP_BIAS) << EXP_SHIFT; // exponent of 1.0
    private static final int MAX_SMALL_BIN_EXP = 62;
    private static final int MIN_SMALL_BIN_EXP = -(63 / 3);
    private static final int MAX_DECIMAL_DIGITS = 15;
    private static final int MAX_DECIMAL_EXPONENT = 308;
    private static final int MIN_DECIMAL_EXPONENT = -324;
    private static final int BIG_DECIMAL_EXPONENT = 324; // i.e. abs(MIN_DECIMAL_EXPONENT)
    static final int MAX_NDIGITS = 1100;
    private static final int INT_DECIMAL_DIGITS = 9;
    private static final int REALLY_BIG = Integer.MAX_VALUE / 10;
    /**
     * All the positive powers of 10 that can be represented exactly in double/float.
     */
    private static final double[] SMALL_10_POW =
            { 1.0e0, 1.0e1, 1.0e2, 1.0e3, 1.0e4, 1.0e5, 1.0e6, 1.0e7, 1.0e8, 1.0e9, 1.0e10, 1.0e11, 1.0e12,
                    1.0e13, 1.0e14, 1.0e15, 1.0e16, 1.0e17, 1.0e18, 1.0e19, 1.0e20, 1.0e21, 1.0e22 };
    static final int MAX_SMALL_TEN = SMALL_10_POW.length - 1;

    private static final double[] BIG_10_POW = { 1e16, 1e32, 1e64, 1e128, 1e256 };
    private static final double[] TINY_10_POW = { 1e-16, 1e-32, 1e-64, 1e-128, 1e-256 };

    private static final char[] INFINITY_REP = "Infinity".toCharArray();
    private static final char[] NEG_INFINITY = ("-" + new String(INFINITY_REP)).toCharArray();
    private static final int INFINITY_LENGTH = INFINITY_REP.length;
    private static final char[] NAN_REP = "NaN".toCharArray();
    private static final int NAN_LENGTH = NAN_REP.length;

    private static final Consumer<StringAppender> D2S_POSITIVE_INFINITY = sa -> sa.append(INFINITY_REP);
    private static final Consumer<StringAppender> D2S_NEGATIVE_INFINITY = sa -> sa.append(NEG_INFINITY);
    private static final Consumer<StringAppender> D2S_NOT_A_NUMBER = sa -> sa.append(NAN_REP);
    private static final Consumer<StringAppender> D2S_POSITIVE_ZERO = new DoubleToString(false, new char[] { '0' });
    private static final Consumer<StringAppender> D2S_NEGATIVE_ZERO = new DoubleToString(true, new char[] { '0' });

    private static final Supplier<Double> S2D_POSITIVE_INFINITY = () -> (Double.POSITIVE_INFINITY);
    private static final Supplier<Double> S2D_NEGATIVE_INFINITY = () -> (Double.NEGATIVE_INFINITY);
    private static final Supplier<Double> S2D_NOT_A_NUMBER = () -> (Double.NaN);

    /**
     * The core logic for converting double-to-string. Thread-local instances keep GC churn and
     * synchronization concerns to a minimum.
     */
    static class DoubleToString implements Consumer<StringAppender>
    {
        boolean isNegative;
        int decExponent;
        int firstDigitIndex;
        int nDigits;
        final char[] digits;

        /**
         * Default constructor; used for non-zero values,
         * <code>DoubleToString</code> may be thread-local and reused
         */
        DoubleToString()
        {
            this.digits = new char[20];
        }

        /**
         * Creates a specialized value (positive and negative zeros).
         */
        DoubleToString(boolean isNegative, char[] digits)
        {
            this.isNegative = isNegative;
            this.decExponent = 0;
            this.digits = digits;
            this.firstDigitIndex = 0;
            this.nDigits = digits.length;
        }

        @Override
        public void accept(StringAppender stringAppender)
        {
            populateAppender(stringAppender);
        }

        /**
         * This is the easy subcase -- all the significant bits, after scaling, are held in lvalue. negSign
         * and decExponent tell us what processing and scaling has already been done. Exceptional cases have
         * already been stripped out. In particular: lvalue is a finite number (not Inf, nor NaN) lvalue > 0L
         * (not zero, nor negative).
         * <p>
         * The only reason that we develop the digits here, rather than calling on Long.toString() is that we
         * can do it a little faster, and besides want to treat trailing 0s specially. If Long.toString
         * changes, we should re-evaluate this strategy!
         */
        private void developLongDigits(long lValue, int insignificantDigits)
        {
            int decExponent = 0;
            if (insignificantDigits != 0)
            {
                // Discard non-significant low-order bits, while rounding,
                // up to insignificant value.
                final long pow10 =
                        LONG_5_POW[insignificantDigits] << insignificantDigits; // 10^i == 5^i * 2^i;
                final long residue = lValue % pow10;
                lValue /= pow10;
                decExponent += insignificantDigits;
                if (residue >= (pow10 >> 1))
                {
                    // round up based on the low-order bits we're discarding
                    lValue++;
                }
            }

            int lsDigitPos = digits.length - 1;
            long q;
            int r;

            //
            // R.S. NOTE:
            // I'm using the same algorithm for long-to-string (see LongToCharrArrayCodec)
            // This long-to-string logic comes from the java.lang.Long class and seems to
            // be much faster than the equivalent logic in Double/FloatingDecimal...
            // different authors on the original java code maybe?

            // NOTE: this logic works on the least significant digit to most significant
            //       hence we start at the END of the array and work "backwards" (--lsDigitPos)

            // Get 2 digits/iteration using longs until quotient fits into an int
            while (lValue > Integer.MAX_VALUE) {
                q = lValue / 100;
                r = (int) (lValue - (q * 100));
                lValue = q;
                digits[--lsDigitPos] = DigitOnes[r];
                digits[--lsDigitPos] = DigitTens[r];
                decExponent+=2;
            }

            // Get 2 digits/iteration using ints
            int q2;
            int i2 = (int)lValue;
            while (i2 >= 65536) {
                q2 = i2 / 100;
                r = i2 - (q2 * 100);
                i2 = q2;
                digits[--lsDigitPos] = DigitOnes[r];
                digits[--lsDigitPos] = DigitTens[r];
                decExponent+=2;
            }

            // Fall thru to fast mode for smaller numbers
            do
            {
                q2 = (i2 * 52429) >>> 19;
                r = i2 - (q2 * 10);
                digits[--lsDigitPos] = LongValueCodec.digits[r];
                decExponent++;
                i2 = q2;
            }
            while (i2 != 0);
            this.decExponent = decExponent;
            this.firstDigitIndex = lsDigitPos;
            this.nDigits = this.digits.length - lsDigitPos - 1;
        }

        private void hardCase(StringAppender stringAppender, long fractBits, int binExp, int nSignificantBits,
                int nTinyBits, int tailZeros, int nFractBits)
        {
            //
            // R.S. NOTE:
            // This is taken from the FloatingDecimal.BinaryToASCIIBuffer.dtoa() method, circa line 497
            //

            //
            // This is the hard case. We are going to compute large positive
            // integers B and S and integer decExp, s.t.
            //      d = ( B / S )// 10^decExp
            //      1 <= B / S < 10
            // Obvious choices are:
            //      decExp = floor( log10(d) )
            //      B      = d// 2^nTinyBits// 10^max( 0, -decExp )
            //      S      = 10^max( 0, decExp)// 2^nTinyBits
            // (noting that nTinyBits has already been forced to non-negative)
            // I am also going to compute a large positive integer
            //      M      = (1/2^nSignificantBits)// 2^nTinyBits// 10^max( 0, -decExp )
            // i.e. M is (1/2) of the ULP of d, scaled like B.
            // When we iterate through dividing B/S and picking off the
            // quotient bits, we will know when to stop when the remainder
            // is <= M.
            //
            // We keep track of powers of 2 and powers of 5.
            //
            int decExp = estimateDecExp(fractBits, binExp);
            int B2, B5; // powers of 2 and powers of 5, respectively, in B
            int S2, S5; // powers of 2 and powers of 5, respectively, in S
            int M2, M5; // powers of 2 and powers of 5, respectively, in M

            B5 = Math.max(0, -decExp);
            B2 = B5 + nTinyBits + binExp;

            S5 = Math.max(0, decExp);
            S2 = S5 + nTinyBits;

            M5 = B5;
            M2 = B2 - nSignificantBits;

            //
            // the long integer fractBits contains the (nFractBits) interesting
            // bits from the mantissa of d ( hidden 1 added if necessary) followed
            // by (EXP_SHIFT+1-nFractBits) zeros. In the interest of compactness,
            // I will shift out those zeros before turning fractBits into a
            // FDBigInteger. The resulting whole number will be
            //      d * 2^(nFractBits-1-binExp).
            //
            fractBits >>>= tailZeros;
            B2 -= nFractBits - 1;
            final int common2factor = Math.min(B2, S2);
            B2 -= common2factor;
            S2 -= common2factor;
            M2 -= common2factor;

            //
            // HACK!! For exact powers of two, the next smallest number
            // is only half as far away as we think (because the meaning of
            // ULP changes at power-of-two bounds) for this reason, we
            // hack M2. Hope this works.
            //
            if (nFractBits == 1)
            {
                M2 -= 1;
            }

            if (M2 < 0)
            {
                // oops.
                // since we cannot scale M down far enough,
                // we must scale the other values up.
                B2 -= M2;
                S2 -= M2;
                M2 = 0;
            }
            //
            // Construct, Scale, iterate.
            // Some day, we'll write a stopping test that takes
            // account of the asymmetry of the spacing of floating-point
            // numbers below perfect powers of 2
            // 26 Sept 96 is not that day.
            // So we use a symmetric test.
            //
            int ndigit = 0;
            boolean low, high;
            long lowDigitDifference;
            int q;

            //
            // Detect the special cases where all the numbers we are about
            // to compute will fit in int or long integers.
            // In these cases, we will avoid doing FDBigInteger arithmetic.
            // We use the same algorithms, except that we "normalize"
            // our FDBigIntegers before iterating. This is to make division easier,
            // as it makes our fist guess (quotient of high-order words)
            // more accurate!
            //
            // Some day, we'll write a stopping test that takes
            // account of the asymmetry of the spacing of floating-point
            // numbers below perfect powers of 2
            // 26 Sept 96 is not that day.
            // So we use a symmetric test.
            //
            // binary digits needed to represent B, approx.
            final int Bbits = nFractBits + B2 + ((B5 < N_5_BITS.length) ? N_5_BITS[B5] : (B5 * 3));

            // binary digits needed to represent 10*S, approx.
            final int tenSbits =
                    S2 + 1 + (((S5 + 1) < N_5_BITS.length) ? N_5_BITS[(S5 + 1)] : ((S5 + 1) * 3));
            if (Bbits < 64 && tenSbits < 64)
            {
                // R.S. its 64 bits (aka a double) - original also handled 32bits (float) but this is removed

                // still good! they're all longs!
                long b = (fractBits * LONG_5_POW[B5]) << B2;
                final long s = LONG_5_POW[S5] << S2;
                long m = LONG_5_POW[M5] << M2;
                final long tens = s * 10L;
                //
                // Unroll the first iteration. If our decExp estimate
                // was too high, our first quotient will be zero. In this
                // case, we discard it and decrement decExp.
                //
                q = (int) (b / s);
                b = 10L * (b % s);
                m *= 10L;
                low = (b < m);
                high = (b + m > tens);
                // todo ignore asserts?
                //assert q < 10 : q; // excessively large digit
                if ((q == 0) && !high)
                {
                    // oops. Usually ignore leading zero.
                    decExp--;
                }
                else
                {
                    digits[ndigit++] = LongValueCodec.digits[q];
                }
                //
                // HACK! Java spec sez that we always have at least
                // one digit after the . in either F- or E-form output.
                // Thus we will need more than one digit if we're using
                // E-form
                //
                if (decExp < -3 || decExp >= 8)
                {
                    high = low = false;
                }
                while (!low && !high)
                {
                    q = (int) (b / s);
                    b = 10 * (b % s);
                    m *= 10;
                    // todo ignore asserts?
                    //assert q < 10 : q;  // excessively large digit
                    if (m > 0L)
                    {
                        low = (b < m);
                        high = (b + m > tens);
                    }
                    else
                    {
                        // hack -- m might overflow!
                        // in this case, it is certainly > b,
                        // which won't
                        // and b+m > tens, too, since that has overflowed
                        // either!
                        low = true;
                        high = true;
                    }
                    digits[ndigit++] = LongValueCodec.digits[q];
                }
                lowDigitDifference = (b << 1) - tens;
            }
            else
            {
                //
                // We really must do FDBigInteger arithmetic.
                // Fist, construct our FDBigInteger initial values.
                //
                FDBigInteger Sval = FDBigInteger.valueOfPow52(S5, S2);
                final int shiftBias = Sval.getNormalizationBias();
                Sval = Sval.leftShift(shiftBias); // normalize so that division works better

                FDBigInteger Bval = FDBigInteger.valueOfMulPow52(fractBits, B5, B2 + shiftBias);
                FDBigInteger Mval = FDBigInteger.valueOfPow52(M5 + 1, M2 + shiftBias + 1);

                FDBigInteger tenSval =
                        FDBigInteger.valueOfPow52(S5 + 1, S2 + shiftBias + 1); //Sval.mult( 10 );
                //
                // Unroll the first iteration. If our decExp estimate
                // was too high, our first quotient will be zero. In this
                // case, we discard it and decrement decExp.
                //
                q = Bval.quoRemIteration(Sval);
                low = (Bval.cmp(Mval) < 0);
                high = tenSval.addAndCmp(Bval, Mval) <= 0;

                // todo ignore asserts?
                //assert q < 10 : q; // excessively large digit
                if ((q == 0) && !high)
                {
                    // oops. Usually ignore leading zero.
                    decExp--;
                }
                else
                {
                    digits[ndigit++] = LongValueCodec.digits[q];
                }
                //
                // HACK! Java spec sez that we always have at least
                // one digit after the . in either F- or E-form output.
                // Thus we will need more than one digit if we're using
                // E-form
                //
                if (decExp < -3 || decExp >= 8)
                {
                    high = low = false;
                }
                while (!low && !high)
                {
                    q = Bval.quoRemIteration(Sval);
                    // todo ignore asserts?
                    //assert q < 10 : q;  // excessively large digit
                    Mval = Mval.multBy10(); //Mval = Mval.mult( 10 );
                    low = (Bval.cmp(Mval) < 0);
                    high = tenSval.addAndCmp(Bval, Mval) <= 0;
                    digits[ndigit++] = LongValueCodec.digits[q];
                }
                if (high && low)
                {
                    Bval = Bval.leftShift(1);
                    lowDigitDifference = Bval.cmp(tenSval);
                }
                else
                {
                    lowDigitDifference = 0L; // this here only for flow analysis!
                }
            }
            this.decExponent = decExp + 1;
            this.firstDigitIndex = 0;
            this.nDigits = ndigit;
            //
            // Last digit gets rounded based on stopping condition.
            //
            if (high)
            {
                if (low)
                {
                    if (lowDigitDifference == 0L)
                    {
                        // it's a tie!
                        // choose based on which digits we like.
                        if ((digits[firstDigitIndex + nDigits - 1] & 1) != 0)
                        {
                            roundup();
                        }
                    }
                    else if (lowDigitDifference > 0)
                    {
                        roundup();
                    }
                }
                else
                {
                    roundup();
                }
            }
            populateAppender(stringAppender);
        }

        // add one to the least significant digit.
        // in the unlikely event there is a carry out, deal with it.
        // assert that this will only happen where there
        // is only one digit, e.g. (float)1e-44 seems to do it.
        //
        void roundup()
        {
            int i = (firstDigitIndex + nDigits - 1);
            int q = digits[i];
            if (q == '9')
            {
                while (q == '9' && i > firstDigitIndex)
                {
                    digits[i] = '0';
                    q = digits[--i];
                }
                if (q == '9')
                {
                    // carryout! High-order 1, rest 0s, larger exp.
                    decExponent += 1;
                    digits[firstDigitIndex] = '1';
                    return;
                }
                // else fall through.
            }
            digits[i] = (char) (q + 1);
        }

        /**
         * Estimate decimal exponent. (If it is small-ish, we could double-check.)
         * <p>
         * First, scale the mantissa bits such that 1 <= d2 < 2. We are then going to estimate log10(d2) ~=~
         * (d2-1.5)/1.5 + log(1.5) and so we can estimate log10(d) ~=~ log10(d2) + binExp * log10(2) take the
         * floor and call it decExp.
         */
        private static int estimateDecExp(long fractBits, int binExp)
        {
            final double d2 = Double.longBitsToDouble(EXP_ONE | (fractBits & DoubleConsts.SIGNIF_BIT_MASK));
            final double d = (d2 - 1.5D) * 0.289529654D + 0.176091259 + (double) binExp * 0.301029995663981;
            final long dBits = Double.doubleToRawLongBits(d);  //can't be NaN here so use raw
            final int exponent =
                    (int) ((dBits & DoubleConsts.EXP_BIT_MASK) >> EXP_SHIFT) - DoubleConsts.EXP_BIAS;
            final boolean isNegative = (dBits & DoubleConsts.SIGN_BIT_MASK) != 0; // discover sign
            if (exponent >= 0 && exponent < 52)
            {
                // hot path
                final long mask = DoubleConsts.SIGNIF_BIT_MASK >> exponent;
                final int r = (int) (((dBits & DoubleConsts.SIGNIF_BIT_MASK) | FRACT_HOB) >> (EXP_SHIFT
                        - exponent));
                return isNegative ? (((mask & dBits) == 0L) ? -r : -r - 1) : r;
            }
            else if (exponent < 0)
            {
                return (((dBits & ~DoubleConsts.SIGN_BIT_MASK) == 0) ? 0 : ((isNegative) ? -1 : 0));
            }
            else
            {
                //if (exponent >= 52)
                return (int) d;
            }
        }

        /**
         * If insignificant==(1L << ixd) i = insignificantDigitsNumber[idx] is the same as: int i; for ( i =
         * 0; insignificant >= 10L; i++ ) insignificant /= 10L;
         */
        private static final int[] insignificantDigitsNumber =
                { 0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9,
                        9, 9, 9, 10, 10, 10, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 14, 14, 14, 15, 15, 15,
                        15, 16, 16, 16, 17, 17, 17, 18, 18, 18, 19 };

        // approximately ceil( log2( long5pow[i] ) )
        private static final int[] N_5_BITS =
                { 0, 3, 5, 7, 10, 12, 14, 17, 19, 21, 24, 26, 28, 31, 33, 35, 38, 40, 42, 45, 47, 49, 52, 54,
                        56, 59, 61, };

        private void populateAppender(StringAppender stringAppender)
        {
            final int start = stringAppender.getLength();
            final char[] buffer = stringAppender.reserveAndGet(26);
            int i = start;
            if (isNegative)
            {
                buffer[i++] = '-';
            }
            if (decExponent > 0 && decExponent < 8)
            {
                // print digits.digits.
                int charLength = Math.min(nDigits, decExponent);
                System.arraycopy(digits, firstDigitIndex, buffer, i, charLength);
                i += charLength;
                if (charLength < decExponent)
                {
                    charLength = decExponent - charLength;
                    Arrays.fill(buffer, i, i + charLength, '0');
                    i += charLength;
                    buffer[i++] = '.';
                    buffer[i++] = '0';
                }
                else
                {
                    buffer[i++] = '.';
                    if (charLength < nDigits)
                    {
                        int t = nDigits - charLength;
                        System.arraycopy(digits, firstDigitIndex + charLength, buffer, i, t);
                        i += t;
                    }
                    else
                    {
                        buffer[i++] = '0';
                    }
                }
            }
            else if (decExponent <= 0 && decExponent > -3)
            {
                buffer[i++] = '0';
                buffer[i++] = '.';
                if (decExponent != 0)
                {
                    Arrays.fill(buffer, i, i - decExponent, '0');
                    i -= decExponent;
                }
                System.arraycopy(digits, firstDigitIndex, buffer, i, nDigits);
                i += nDigits;
            }
            else
            {
                buffer[i++] = digits[firstDigitIndex];
                buffer[i++] = '.';
                if (nDigits > 1)
                {
                    System.arraycopy(digits, firstDigitIndex + 1, buffer, i, nDigits - 1);
                    i += nDigits - 1;
                }
                else
                {
                    buffer[i++] = '0';
                }
                buffer[i++] = 'E';
                int e;
                if (decExponent <= 0)
                {
                    buffer[i++] = '-';
                    e = -decExponent + 1;
                }
                else
                {
                    e = decExponent - 1;
                }
                // decExponent has 1, 2, or 3, digits
                if (e <= 9)
                {
                    buffer[i++] = LongValueCodec.digits[e];
                }
                else if (e <= 99)
                {
                    buffer[i++] = LongValueCodec.digits[e / 10];
                    buffer[i++] = LongValueCodec.digits[e % 10];
                }
                else
                {
                    buffer[i++] = LongValueCodec.digits[e / 100];
                    e %= 100;
                    buffer[i++] = LongValueCodec.digits[e / 10];
                    buffer[i++] = LongValueCodec.digits[e % 10];
                }
            }
            stringAppender.setLength(i);
        }
    }

    private static final ThreadLocal<DoubleToString> DOUBLE_TO_STRING_THREAD_LOCAL =
            ThreadLocal.withInitial(DoubleToString::new);

    /**
     * Appends a double precision floating point value to an <code>StringAppender</code>.
     *
     * @param d   The double precision value.
     * @param stringAppender The <code>StringAppender</code> with the value appended.
     */
    static void writeToCharArray(double d, StringAppender stringAppender)
    {
        final long dBits = Double.doubleToRawLongBits(d);
        final boolean isNegative = (dBits & DoubleConsts.SIGN_BIT_MASK) != 0; // discover sign
        long fractBits = dBits & DoubleConsts.SIGNIF_BIT_MASK;
        int binExp = (int) ((dBits & DoubleConsts.EXP_BIT_MASK) >> EXP_SHIFT);
        // Discover obvious special cases of NaN and Infinity.
        if (binExp == (int) (DoubleConsts.EXP_BIT_MASK >> EXP_SHIFT))
        {
            if (fractBits == 0L)
            {
                (isNegative ? D2S_NEGATIVE_INFINITY : D2S_POSITIVE_INFINITY).accept(stringAppender);
            }
            else
            {
                D2S_NOT_A_NUMBER.accept(stringAppender);
            }
            return;
        }
        // Finish unpacking
        // Normalize denormalized numbers.
        // Insert assumed high-order bit for normalized numbers.
        // Subtract exponent bias.
        int nSignificantBits;
        if (binExp == 0)
        {
            if (fractBits == 0L)
            {
                // not a denorm, just a 0!
                (isNegative ? D2S_NEGATIVE_ZERO : D2S_POSITIVE_ZERO).accept(stringAppender);
                return;
            }
            final int leadingZeros = Long.numberOfLeadingZeros(fractBits);
            final int shift = leadingZeros - (63 - EXP_SHIFT);
            fractBits <<= shift;
            binExp = 1 - shift;
            nSignificantBits = 64 - leadingZeros; // recall binExp is  - shift count.
        }
        else
        {
            fractBits |= FRACT_HOB;
            nSignificantBits = EXP_SHIFT + 1;
        }
        binExp -= DoubleConsts.EXP_BIAS;

        // call the routine that actually does all the hard work.
        final DoubleToString doubleToString = DOUBLE_TO_STRING_THREAD_LOCAL.get();
        doubleToString.isNegative = isNegative;

        // todo ignore asserts?
        //assert fractBits > 0; // fractBits here can't be zero or negative
        //assert (fractBits & FRACT_HOB) != 0; // Hi-order bit should be set

        // Examine number. Determine if it is an easy case,
        // which we can do pretty trivially using float/long conversion,
        // or whether we must do real work.
        final int tailZeros = Long.numberOfTrailingZeros(fractBits);

        // number of significant bits of fractBits;
        final int nFractBits = EXP_SHIFT + 1 - tailZeros;

        // reset flags to default values as dtoa() does not always set these
        // flags and a prior call to dtoa() might have set them to incorrect
        // values with respect to the current state.

        // number of significant bits to the right of the point.
        final int nTinyBits = Math.max(0, nFractBits - binExp - 1);
        if (binExp <= MAX_SMALL_BIN_EXP && binExp >= MIN_SMALL_BIN_EXP
                // Look more closely at the number to decide if,
                // with scaling by 10^nTinyBits, the result will fit in
                // a long.
                && (nTinyBits < LONG_5_POW.length) && ((nFractBits + DoubleToString.N_5_BITS[nTinyBits]) < 64)
                //
                // We can do this:
                // take the fraction bits, which are normalized.
                // (a) nTinyBits == 0: Shift left or right appropriately
                //     to align the binary point at the extreme right, i.e.
                //     where a long int point is expected to be. The integer
                //     result is easily converted to a string.
                // (b) nTinyBits > 0: Shift right by EXP_SHIFT-nFractBits,
                //     which effectively converts to long and scales by
                //     2^nTinyBits. Then multiply by 5^nTinyBits to
                //     complete the scaling. We know this won't overflow
                //     because we just counted the number of bits necessary
                //     in the result. The integer you get from this can
                //     then be converted to a string pretty easily.
                //
                && nTinyBits == 0)
        {
            final int index = binExp - nSignificantBits - 1;
            doubleToString.developLongDigits(binExp >= EXP_SHIFT ? (fractBits << (binExp - EXP_SHIFT)) :
                            (fractBits >>> (EXP_SHIFT - binExp)),
                    binExp > nSignificantBits && index > 1 ? DoubleToString.insignificantDigitsNumber[index] :
                            0);
            doubleToString.populateAppender(stringAppender);
            return;
        }
        doubleToString.hardCase(stringAppender, fractBits, binExp, nSignificantBits, nTinyBits, tailZeros,
                nFractBits);
    }

    /**
     * Converts a <code>char[]</code> to a double precision floating point value.
     *
     * @param in    The <code>char[]</code> to convert, starts at index 0.
     * @param start the start of the double number in the char[]
     * @param len   the length of the chars in the array
     * @return The double precision value.
     * @throws NumberFormatException If the <code>char[]</code> does not represent a properly formatted double
     *                          x     precision value.
     */
    static double fromCharArray(char[] in, int start, int len) throws NumberFormatException
    {
        boolean isNegative = false;
        boolean signSeen = false;
        int decExp;
        char c;

        parseNumber:
        try
        {
            if (len == 0)
            {
                throw new NumberFormatException("empty String");
            }
            int i = start;
            c = in[i];
            if (c == '-')
            {
                isNegative = true;
                signSeen = true;
                c = in[++i];
            }
            else if (c == '+')
            {
                signSeen = true;
                c = in[++i];
            }
            if (c == 'N')
            {
                // Check for NaN
                if ((len) == NAN_LENGTH && in[i + 1] == 'a' && in[i + 2] == 'N')
                {
                    return S2D_NOT_A_NUMBER.get();
                }
                // something went wrong, throw exception
                break parseNumber;
            }
            else if (c == 'I')
            {
                // Check for Infinity
                if ((len - i) == INFINITY_LENGTH && in[i + 1] == 'n' && in[i + 2] == 'f' && in[i + 3] == 'i'
                        && in[i + 4] == 'n' && in[i + 5] == 'i' && in[i + 6] == 't' && in[i + 7] == 'y')
                {
                    return isNegative ? S2D_NEGATIVE_INFINITY.get() : S2D_POSITIVE_INFINITY.get();
                }
                // something went wrong, throw exception
                break parseNumber;
            }
            // look for and process decimal floating-point string

            int nDigits = 0;
            boolean decSeen = false;
            int decPt = 0;
            int nLeadZero = 0;

            final int end = start + len;
            // loop to find leading zeros and decimal
            while (true)
            {
                if (c == '0')
                {
                    nLeadZero++;
                }
                else if (c == '.')
                {
                    if (decSeen)
                    {
                        // already saw one ., this is the 2nd.
                        throw new NumberFormatException("multiple points");
                    }
                    decPt = i;
                    if (signSeen)
                    {
                        decPt -= 1;
                    }
                    decSeen = true;
                }
                else
                {
                    break;
                }
                // loop control
                if (++i < end)
                {
                    // next inspection
                    c = in[i];
                }
                else
                {
                    break;
                }
            }

            final int _start = i;

            // integer reading
            int iValue = 0;
            do
            {
                if (c >= '1' && c <= '9')
                {
                    iValue = iValue * 10 + LongValueCodec.digits_from_char[c];
                    nDigits++;
                }
                else if (c == '0')
                {
                    iValue = iValue * 10;
                    nDigits++;
                }
                else if (c == '.')
                {
                    if (decSeen)
                    {
                        // already saw one ., this is the 2nd.
                        throw new NumberFormatException("multiple points");
                    }
                    decPt = i;
                    if (signSeen)
                    {
                        decPt -= 1;
                    }
                    decSeen = true;
                }
                else
                {
                    break;
                }
                // loop control
                if (++i < end)
                {
                    // next inspection
                    c = in[i];
                }
                else
                {
                    break;
                }
            }
            while (nDigits < INT_DECIMAL_DIGITS);

            // we are in long territory now, nDigits >= INT_DECIMAL_DIGITS
            long lValue = iValue;
            if (i < end)
            {
                do
                {
                    if (c >= '1' && c <= '9')
                    {
                        lValue = lValue * 10L + (long) LongValueCodec.digits_from_char[c];
                        nDigits++;
                    }
                    else if (c == '0')
                    {
                        lValue = lValue * 10L;
                        nDigits++;
                    }
                    else if (c == '.')
                    {
                        if (decSeen)
                        {
                            // already saw one ., this is the 2nd.
                            throw new NumberFormatException("multiple points");
                        }
                        decPt = i;
                        if (signSeen)
                        {
                            decPt -= 1;
                        }
                        decSeen = true;
                    }
                    else
                    {
                        break;
                    }
                    // loop control
                    if (++i < end)
                    {
                        // next inspection
                        c = in[i];
                    }
                    else
                    {
                        break;
                    }
                }
                while (nDigits < MAX_DECIMAL_DIGITS + 1);

                // process overspill
                while (i < end)
                {
                    c = in[i];
                    if (c >= '0' && c <= '9')
                    {
                        nDigits++;
                    }
                    else if (c == '.')
                    {
                        if (decSeen)
                        {
                            // already saw one ., this is the 2nd.
                            throw new NumberFormatException("multiple points");
                        }
                        decPt = i;
                        if (signSeen)
                        {
                            decPt -= 1;
                        }
                        decSeen = true;
                    }
                    else
                    {
                        break;
                    }
                    i++;
                }
            }

            // adjust the decPt relative to the start and len
            decPt -= start;

            //
            // At this point, we've scanned all the digits and decimal
            // point we're going to see. Trim off leading and trailing
            // zeros, which will just confuse us later, and adjust
            // our initial decimal exponent accordingly.
            // To review:
            // we have seen i total characters.
            // nLeadZero of them were zeros before any other digits.
            // nTrailZero of them were zeros after any other digits.
            // if ( decSeen ), then a . was seen after decPt characters
            // ( including leading zeros which have been discarded )
            // nDigits characters were neither lead nor trailing
            // zeros, nor point
            //
            //
            // special hack: if we saw no non-zero digits, then the
            // answer is zero!
            // Unfortunately, we feel honor-bound to keep parsing!
            //
            if (nDigits == 0 && nLeadZero == 0)
            {
                // we saw NO DIGITS AT ALL,
                // not even a crummy 0!
                // this is not allowed.
                break parseNumber; // go throw exception
            }
            //
            // Our initial exponent is decPt, adjusted by the number of
            // discarded zeros. Or, if there was no decPt,
            // then its just nDigits adjusted by discarded trailing zeros.
            //
            if (decSeen)
            {
                decExp = decPt - nLeadZero;
            }
            else
            {
                decExp = nDigits;
            }

            //
            // Look for 'e' or 'E' and an optionally signed integer.
            //
            if ((i < end - 1) && (((c = in[i]) == 'e') || (c == 'E')))
            {
                int expSign = 1;
                int expVal = 0;
                boolean expOverflow = false;
                switch(in[++i])
                {
                    case '-':
                        expSign = -1;
                        //FALLTHROUGH
                    case '+':
                        i++;
                }
                int expAt = i;
                while (i < end)
                {
                    if (expVal >= REALLY_BIG)
                    {
                        // the next character will cause integer
                        // overflow.
                        expOverflow = true;
                    }
                    c = in[i++];
                    if (c >= '0' && c <= '9')
                    {
                        expVal = expVal * 10 + LongValueCodec.digits_from_char[c];
                    }
                    else
                    {
                        i--;           // back up.
                        break; // stop parsing exponent.
                    }
                }
                final int expLimit = BIG_DECIMAL_EXPONENT + nDigits;
                if (expOverflow || (expVal > expLimit))
                {
                    //
                    // The intent here is to end up with
                    // infinity or zero, as appropriate.
                    // The reason for yielding such a small decExponent,
                    // rather than something intuitive such as
                    // expSign*Integer.MAX_VALUE, is that this value
                    // is subject to further manipulation in
                    // doubleValue(), and I don't want
                    // it to be able to cause overflow there!
                    // (The only way we can get into trouble here is for
                    // really outrageous nDigits+nTrailZero, such as 2 billion. )
                    //
                    decExp = expSign * expLimit;
                }
                else
                {
                    // this should not overflow, since we tested
                    // for expVal > (MAX+N), where N >= abs(decExp)
                    decExp = decExp + expSign * expVal;
                }

                // if we saw something not a digit ( or end of string )
                // after the [Ee][+-], without seeing any digits at all
                // this is certainly an error. If we saw some digits,
                // but then some trailing garbage, that might be ok.
                // so we just fall through in that case.
                // HUMBUG
                if (i == expAt)
                {
                    break parseNumber; // certainly bad
                }
            }
            //
            // We parsed everything we could.
            // If there are leftovers, then this is not good input!
            //
            if (i < end && ((i != end - 1) || (in[i] != 'f' && in[i] != 'F' && in[i] != 'd' && in[i] != 'D')))
            {
                break parseNumber; // go throw exception
            }

            // R.S.
            // ======================================
            // now do the work to convert to double....everything above was preparation :)
            // ======================================
            return computeDouble(in, _start, nDigits, decExp, lValue, isNegative, decSeen && nLeadZero == 0);
        }
        catch (Exception ignored)
        {
        }
        throw new NumberFormatException("For input string: \"" + new String(in, start, len) + "\"");
    }

    private static double computeDouble(char[] in, int _start,
            int nDigits, int decExp, long lValue, boolean isNegative, boolean hasDecimalPoint)
    {
        final int kDigits = Math.min(nDigits, MAX_DECIMAL_DIGITS + 1);
        double dValue = (double) lValue;
        int exp = decExp - kDigits;

        //
        // R.S. NOTE:
        // The remainder of this method is copied from FloatingDecimal.doubleValue method circa line 1074.
        // This logic is very IEEE indepth so not logic changes have been made.
        // Only some code cleanup has been done.
        //

        //
        // lValue now contains a long integer with the value of
        // the first kDigits digits of the number.
        // dValue contains the (double) of the same.
        //

        if (nDigits <= MAX_DECIMAL_DIGITS)
        {
            //
            // possibly an easy case.
            // We know that the digits can be represented
            // exactly. And if the exponent isn't too outrageous,
            // the whole thing can be done with one operation,
            // thus one rounding error.
            // Note that all our constructors trim all leading and
            // trailing zeros, so simple values (including zero)
            // will always end up here
            //
            if (exp == 0 || dValue == 0.0)
            {
                return isNegative ? -dValue : dValue; // small floating integer
            }
            else if (exp >= 0)
            {
                if (exp <= MAX_SMALL_TEN)
                {
                    //
                    // Can get the answer with one operation,
                    // thus one roundoff.
                    //
                    final double rValue = dValue * SMALL_10_POW[exp];
                    return isNegative ? -rValue : rValue;
                }
                else
                {
                    final int slop = MAX_DECIMAL_DIGITS - kDigits;
                    if (exp <= MAX_SMALL_TEN + slop)
                    {
                        //
                        // We can multiply dValue by 10^(slop)
                        // and it is still "small" and exact.
                        // Then we can multiply by 10^(exp-slop)
                        // with one rounding.
                        //
                        dValue *= SMALL_10_POW[slop];
                        final double rValue = dValue * SMALL_10_POW[exp - slop];
                        return isNegative ? -rValue : rValue;
                    }
                    //
                    // Else we have a hard case with a positive exp.
                    //
                }
            }
            else if (exp >= -MAX_SMALL_TEN)
            {
                //
                // Can get the answer in one division.
                //
                final double rValue = dValue / SMALL_10_POW[-exp];
                return isNegative ? -rValue : rValue;
            }
            //
            // Else we have a hard case with a negative exp.
            //
        }

        //
        // Harder cases:
        // The sum of digits plus exponent is greater than
        // what we think we can do with one error.
        //
        // Start by approximating the right answer by,
        // naively, scaling by powers of 10.
        //
        if (exp > 0)
        {
            if (decExp > MAX_DECIMAL_EXPONENT + 1)
            {
                //
                // Lets face it. This is going to be
                // Infinity. Cut to the chase.
                //
                return isNegative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            }
            if ((exp & 15) != 0)
            {
                dValue *= SMALL_10_POW[exp & 15];
            }
            if ((exp >>= 4) != 0)
            {
                int j;
                for (j = 0; exp > 1; j++, exp >>= 1)
                {
                    if ((exp & 1) != 0)
                    {
                        dValue *= BIG_10_POW[j];
                    }
                }
                //
                // The reason for the weird exp > 1 condition
                // in the above loop was so that the last multiply
                // would get unrolled. We handle it here.
                // It could overflow.
                //
                double t = dValue * BIG_10_POW[j];
                if (Double.isInfinite(t))
                {
                    // todo not executed in unit tests
                    
                    //
                    // It did overflow.
                    // Look more closely at the result.
                    // If the exponent is just one too large,
                    // then use the maximum finite as our estimate
                    // value. Else call the result infinity
                    // and punt it.
                    // ( I presume this could happen because
                    // rounding forces the result here to be
                    // an ULP or two larger than
                    // Double.MAX_VALUE ).
                    //
                    t = dValue / 2.0;
                    t *= BIG_10_POW[j];
                    if (Double.isInfinite(t))
                    {
                        return isNegative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                    }
                    t = Double.MAX_VALUE;
                }
                dValue = t;
            }
        }
        else if (exp < 0)
        {
            exp = -exp;
            if (decExp < MIN_DECIMAL_EXPONENT - 1)
            {
                //
                // Lets face it. This is going to be
                // zero. Cut to the chase.
                //
                return isNegative ? -0.0 : 0.0;
            }
            if ((exp & 15) != 0)
            {
                dValue /= SMALL_10_POW[exp & 15];
            }
            if ((exp >>= 4) != 0)
            {
                int j;
                for (j = 0; exp > 1; j++, exp >>= 1)
                {
                    if ((exp & 1) != 0)
                    {
                        dValue *= TINY_10_POW[j];
                    }
                }
                //
                // The reason for the weird exp > 1 condition
                // in the above loop was so that the last multiply
                // would get unrolled. We handle it here.
                // It could underflow.
                //
                double t = dValue * TINY_10_POW[j];
                if (t == 0.0)
                {
                    //
                    // It did underflow.
                    // Look more closely at the result.
                    // If the exponent is just one too small,
                    // then use the minimum finite as our estimate
                    // value. Else call the result 0.0
                    // and punt it.
                    // ( I presume this could happen because
                    // rounding forces the result here to be
                    // an ULP or two less than
                    // Double.MIN_VALUE ).
                    //
                    t = dValue * 2.0;
                    t *= TINY_10_POW[j];
                    if (t == 0.0)
                    {
                        return isNegative ? -0.0 : 0.0;
                    }
                    // todo not executed in unit tests
                    t = Double.MIN_VALUE;
                }
                dValue = t;
            }
        }
        //
        // dValue is now approximately the result.
        // The hard part is adjusting it, by comparison
        // with FDBigInteger arithmetic.
        // Formulate the EXACT big-number result as
        // bigD0 * 10^exp
        //
        if (nDigits > MAX_NDIGITS)
        {
            nDigits = MAX_NDIGITS + 1;
            in[MAX_NDIGITS + _start] = '1';
        }
        // R.S. need to compensate for any "." in the char[]  with an offset to kDigits and nDigits
        final int offset = _start + (hasDecimalPoint ? 1 : 0);
        FDBigInteger bigD0 = new FDBigInteger(lValue, in, kDigits + offset, nDigits + offset);
        exp = decExp - nDigits;

        long ieeeBits = Double.doubleToRawLongBits(dValue); // IEEE-754 bits of double candidate
        final int B5 = Math.max(0, -exp); // powers of 5 in bigB, value is not modified inside correctionLoop
        final int D5 = Math.max(0, exp); // powers of 5 in bigD, value is not modified inside correctionLoop
        bigD0 = bigD0.multByPow52(D5, 0);
        bigD0.makeImmutable();   // prevent bigD0 modification inside correctionLoop
        FDBigInteger bigD = null;
        int prevD2 = 0;

        correctionLoop:
        while (true)
        {
            // here ieeeBits can't be NaN, Infinity or zero
            int binexp = (int) (ieeeBits >>> EXP_SHIFT);
            long bigBbits = ieeeBits & DoubleConsts.SIGNIF_BIT_MASK;
            if (binexp > 0)
            {
                bigBbits |= FRACT_HOB;
            }
            else
            {
                // Normalize denormalized numbers.
                // todo ignore asserts?
                //assert bigBbits != 0L : bigBbits; // doubleToBigInt(0.0)
                int leadingZeros = Long.numberOfLeadingZeros(bigBbits);
                int shift = leadingZeros - (63 - EXP_SHIFT);
                bigBbits <<= shift;
                binexp = 1 - shift;
            }
            binexp -= DoubleConsts.EXP_BIAS;
            int lowOrderZeros = Long.numberOfTrailingZeros(bigBbits);
            bigBbits >>>= lowOrderZeros;
            final int bigIntExp = binexp - EXP_SHIFT + lowOrderZeros;
            final int bigIntNBits = EXP_SHIFT + 1 - lowOrderZeros;

            //
            // Scale bigD, bigB appropriately for
            // big-integer operations.
            // Naively, we multiply by powers of ten
            // and powers of two. What we actually do
            // is keep track of the powers of 5 and
            // powers of 2 we would use, then factor out
            // common divisors before doing the work.
            //
            int B2 = B5; // powers of 2 in bigB
            int D2 = D5; // powers of 2 in bigD
            int Ulp2;   // powers of 2 in halfUlp.
            if (bigIntExp >= 0)
            {
                B2 += bigIntExp;
            }
            else
            {
                D2 -= bigIntExp;
            }
            Ulp2 = B2;
            // shift bigB and bigD left by a number s. t.
            // halfUlp is still an integer.
            final int hulpbias;
            if (binexp <= -DoubleConsts.EXP_BIAS)
            {
                // This is going to be a denormalized number
                // (if not actually zero).
                // half an ULP is at 2^-(DoubleConsts.EXP_BIAS+EXP_SHIFT+1)
                hulpbias = binexp + lowOrderZeros + DoubleConsts.EXP_BIAS;
            }
            else
            {
                hulpbias = 1 + lowOrderZeros;
            }
            B2 += hulpbias;
            D2 += hulpbias;
            // if there are common factors of 2, we might just as well
            // factor them out, as they add nothing useful.
            final int common2 = Math.min(B2, Math.min(D2, Ulp2));
            B2 -= common2;
            D2 -= common2;
            Ulp2 -= common2;
            // do multiplications by powers of 5 and 2
            final FDBigInteger bigB = FDBigInteger.valueOfMulPow52(bigBbits, B5, B2);
            if (bigD == null || prevD2 != D2)
            {
                bigD = bigD0.leftShift(D2);
                prevD2 = D2;
            }
            //
            // to recap:
            // bigB is the scaled-big-int version of our floating-point
            // candidate.
            // bigD is the scaled-big-int version of the exact value
            // as we understand it.
            // halfUlp is 1/2 an ulp of bigB, except for special cases
            // of exact powers of 2
            //
            // the plan is to compare bigB with bigD, and if the difference
            // is less than halfUlp, then we're satisfied. Otherwise,
            // use the ratio of difference to halfUlp to calculate a fudge
            // factor to add to the floating value, then go 'round again.
            //
            FDBigInteger diff;
            int cmpResult;
            boolean overvalue;
            if ((cmpResult = bigB.cmp(bigD)) > 0)
            {
                overvalue = true; // our candidate is too big.
                diff = bigB.leftInplaceSub(bigD); // bigB is not user further - reuse
                if ((bigIntNBits == 1) && (bigIntExp > -DoubleConsts.EXP_BIAS + 1))
                {
                    // todo not executed in unit tests
                    
                    // candidate is a normalized exact power of 2 and
                    // is too big (larger than Double.MIN_NORMAL). We will be subtracting.
                    // For our purposes, ulp is the ulp of the
                    // next smaller range.
                    Ulp2 -= 1;
                    if (Ulp2 < 0)
                    {
                        // rats. Cannot de-scale ulp this far.
                        // must scale diff in other direction.
                        Ulp2 = 0;
                        diff = diff.leftShift(1);
                    }
                }
            }
            else if (cmpResult < 0)
            {
                overvalue = false; // our candidate is too small.
                diff = bigD.rightInplaceSub(bigB); // bigB is not user further - reuse
            }
            else
            {
                // the candidate is exactly right!
                // this happens with surprising frequency
                break correctionLoop;
            }
            cmpResult = diff.cmpPow52(B5, Ulp2);
            if ((cmpResult) < 0)
            {
                // difference is small.
                // this is close enough
                break correctionLoop;
            }
            else if (cmpResult == 0)
            {
                // todo not executed in unit tests
                
                // difference is exactly half an ULP
                // round to some other value maybe, then finish
                if ((ieeeBits & 1) != 0)
                {
                    // half ties to even
                    ieeeBits += overvalue ? -1 : 1; // nextDown or nextUp
                }
                break correctionLoop;
            }
            else
            {
                // difference is non-trivial.
                // could scale addend by ratio of difference to
                // halfUlp here, if we bothered to compute that difference.
                // Most of the time ( I hope ) it is about 1 anyway.
                ieeeBits += overvalue ? -1 : 1; // nextDown or nextUp
                if (ieeeBits == 0 || ieeeBits == DoubleConsts.EXP_BIT_MASK)
                {
                    // 0.0 or Double.POSITIVE_INFINITY
                    break correctionLoop; // oops. Fell off end of range.
                }
                // try again.
            }
        } // while(true)

        if (isNegative)
        {
            ieeeBits |= DoubleConsts.SIGN_BIT_MASK;
        }
        return Double.longBitsToDouble(ieeeBits);
    }
}
