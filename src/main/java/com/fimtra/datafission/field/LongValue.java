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

import com.fimtra.datafission.IValue;
import com.fimtra.util.is;

/**
 * The IValue for a long.
 * 
 * @author Ramon Servadei
 */
public final class LongValue extends AbstractValue
{
    static final int POOL_SIZE = 2048;
    static final LongValue[] pool = new LongValue[POOL_SIZE];
    static final int poolTop = POOL_SIZE / 2;
    static final int poolBottom = -((POOL_SIZE / 2) - 1);
    static
    {
        int j = 0;
        for (int i = poolBottom; i <= poolTop; i++)
        {
            pool[j++] = new LongValue(i);
        }
    }

    /**
     * Get a canonical {@link LongValue} for the value from a pool. If the pool does not contain an
     * instance for this value this returns a new instance.
     * 
     * @param value
     *            the value
     * @return a canonical {@link LongValue} representing the value if available otherwise a new
     *         instance
     */
    public static LongValue valueOf(long value)
    {
        if (value >= poolBottom && value <= poolTop)
        {
            return pool[(int) value - poolBottom];
        }
        return new LongValue(value);
    }

    /**
     * Get a long from the passed in IValue, returning the defaultValue if the IValue is
     * <code>null</code> or not a LongValue
     * 
     * @param target
     *            the IValue to extract a long from
     * @param defaultValue
     *            the default value
     * @return the long value of the IValue or the defaultValue if the IValue is <code>null</code>
     *         or not a LongValue
     */
    public static long get(IValue target, long defaultValue)
    {
        return target == null || !(target instanceof LongValue) ? defaultValue : target.longValue();
    }

    private long value;

    /** Initialises to represent 0. */
    LongValue()
    {
        this(0);
    }

    private LongValue(long value)
    {
        super();
        this.value = value;
    }

    @Override
    public TypeEnum getType()
    {
        return TypeEnum.LONG;
    }

    @Override
    public long longValue()
    {
        return this.value;
    }

    @Override
    public double doubleValue()
    {
        return this.value;
    }

    @Override
    public String textValue()
    {
        return Long.toString(this.value);
    }

    @Override
    public void fromString(String value)
    {
        this.value = Long.valueOf(value).longValue();
    }

    @Override
    void fromChars(char[] chars, int start, int len)
    {
        this.value = Long.valueOf(new String(chars, start, len)).longValue();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (this.value ^ (this.value >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (is.same(this, obj))
        {
            return true;
        }
        if (is.differentClass(this, obj))
        {
            return false;
        }
        LongValue other = (LongValue) obj;
        return is.eq(this.value, other.value);
    }

    public static IValue valueOf(char[] chars, int start, int len)
    {
        return LongValue.valueOf(parseLong(chars, start, len, 10));
    }

    private static long parseLong(char[] chars, int start, int length, int radix) throws NumberFormatException
    {
        if (chars == null)
        {
            throw new NumberFormatException("null");
        }

        if (radix < Character.MIN_RADIX)
        {
            throw new NumberFormatException("radix " + radix + " less than Character.MIN_RADIX");
        }
        if (radix > Character.MAX_RADIX)
        {
            throw new NumberFormatException("radix " + radix + " greater than Character.MAX_RADIX");
        }

        long result = 0;
        boolean negative = false;
        int i = start, len = start + length;
        long limit = -Long.MAX_VALUE;
        long multmin;
        int digit;

        if (len > 0)
        {
            char firstChar = chars[i];
            if (firstChar < '0')
            {
                // Possible leading "+" or "-"
                if (firstChar == '-')
                {
                    negative = true;
                    limit = Long.MIN_VALUE;
                }
                else
                {
                    if (firstChar != '+')
                    {
                        throw new NumberFormatException(new String(chars, start, len));
                    }
                }
                if (len == 1)
                { // Cannot have lone "+" or "-"
                    throw new NumberFormatException(new String(chars, start, len));
                }
                i++;
            }
            multmin = limit / radix;
            while (i < len)
            {
                // Accumulating negatively avoids surprises near MAX_VALUE
                digit = Character.digit(chars[i++], radix);
                if (digit < 0)
                {
                    throw new NumberFormatException(new String(chars, start, len));
                }
                if (result < multmin)
                {
                    throw new NumberFormatException(new String(chars, start, len));
                }
                result *= radix;
                if (result < limit + digit)
                {
                    throw new NumberFormatException(new String(chars, start, len));
                }
                result -= digit;
            }
        }
        else
        {
            throw new NumberFormatException(new String(chars, start, len));
        }
        return negative ? result : -result;
    }
}
