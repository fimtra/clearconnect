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

import static com.fimtra.datafission.field.CachedLongValue.NEG_POOL;
import static com.fimtra.datafission.field.CachedLongValue.POS_POOL;

import com.fimtra.datafission.IValue;
import com.fimtra.util.StringAppender;

/**
 * The IValue for a long.
 * 
 * @author Ramon Servadei
 */
public class LongValue extends AbstractValue
{
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
        // get permanent cached -2048 to 2048
        if (value >= 0)
        {
            if (value < POS_POOL.length)
            {
                return POS_POOL[(int) value];
            }
        }
        else if (-value < NEG_POOL.length)
        {
            return NEG_POOL[(int) -value];
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
        return (target instanceof LongValue) ? target.longValue() : defaultValue;
    }

    private final long value;

    /** Initialises to represent 0. */
    LongValue()
    {
        this(0);
    }

    LongValue(long value)
    {
        super();
        this.value = value;
    }

    @Override
    public final TypeEnum getType()
    {
        return TypeEnum.LONG;
    }

    @Override
    public final long longValue()
    {
        return this.value;
    }

    @Override
    public final double doubleValue()
    {
        return this.value;
    }

    @Override
    public String textValue()
    {
        final int digitCount = value < 0 ? LongToCharArrayCodec.stringSize(-value) + 1 :
                LongToCharArrayCodec.stringSize(value);
        final char[] chars = new char[digitCount];
        LongToCharArrayCodec.writeToCharArray(value, chars, 0, chars.length);
        return new String(chars);
    }

    @Override
    public final StringAppender toStringAppender()
    {
        final int digitCount = (value < 0 ? LongToCharArrayCodec.stringSize(-value) + 1 :
                LongToCharArrayCodec.stringSize(value))
                // plus 1 for the LONG_CODE
                + 1;
        final StringAppender appender = new StringAppender(digitCount);
        final char[] buf = appender.reserveAndGet(digitCount);
        buf[0] = IValue.LONG_CODE;
        LongToCharArrayCodec.writeToCharArray(value, buf, 1, digitCount);
        return appender;
    }

    @Override
    public final int hashCode()
    {
        return (int) (this.value ^ (this.value >>> 32));
    }

    @Override
    public final boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (!(obj instanceof LongValue))
        {
            return false;
        }

        return value == ((LongValue) obj).value;
    }

    public static IValue valueOf(char[] chars, int start, int len)
    {
        return LongValue.valueOf(LongToCharArrayCodec.fromCharArray(chars, start, len));
    }

    @Override
    public StringAppender appendTo(StringAppender stringAppender)
    {
        final int digitCount = (value < 0 ? LongToCharArrayCodec.stringSize(-value) + 1 :
                LongToCharArrayCodec.stringSize(value))
                // plus 1 for the LONG_CODE
                + 1;
        int start = stringAppender.getLength();
        final char[] buf = stringAppender.reserveAndGet(digitCount);
        final int len = start + digitCount;
        buf[start++] = IValue.LONG_CODE;
        LongToCharArrayCodec.writeToCharArray(value, buf, start, len);
        return stringAppender;
    }
}
