package com.fimtra.datafission.field;

import static com.fimtra.datafission.DataFissionProperties.Values.LONG_VALUE_POOL_SIZE;

import com.fimtra.datafission.IValue;
import com.fimtra.util.StringAppender;

/**
 * Specialisation that caches its {@link #textValue()} and {@link #toString} forms and the char[] used for
 * writing to a {@link StringAppender}
 *
 * @author Ramon Servadei
 */
final class CachedLongValue extends LongValue
{
    static final CachedLongValue[] POS_POOL = new CachedLongValue[LONG_VALUE_POOL_SIZE + 1];
    static final CachedLongValue[] NEG_POOL = new CachedLongValue[LONG_VALUE_POOL_SIZE + 1];

    static
    {
        for (int i = 0; i < POS_POOL.length; i++)
        {
            POS_POOL[i] = new CachedLongValue(i);
        }

        for (int i = 0; i < NEG_POOL.length; i++)
        {
            int v = -i;
            NEG_POOL[i] = new CachedLongValue(v);
        }
    }

    private final String textValue;
    private final String toString;
    private final char[] toStringCharArray;

    CachedLongValue(long value)
    {
        super(value);
        this.textValue = Long.toString(value)
                .intern();
        this.toString = IValue.LONG_CODE + textValue;
        this.toStringCharArray = toString.toCharArray();
    }

    @Override
    public String textValue()
    {
        return textValue;
    }

    @Override
    public StringAppender appendTo(StringAppender stringAppender)
    {
        return stringAppender.append(toStringCharArray);
    }

    @Override
    public String toString()
    {
        return toString;
    }
}
