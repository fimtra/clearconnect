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
final class CachedDoubleValue extends DoubleValue
{
    // positive and negative integral value pools
    static final CachedDoubleValue[] POS_INTEGRAL_POOL = new CachedDoubleValue[LONG_VALUE_POOL_SIZE + 1];
    static final CachedDoubleValue[] NEG_INTEGRAL_POOL = new CachedDoubleValue[LONG_VALUE_POOL_SIZE + 1];

    static
    {
        for (int i = 0; i < POS_INTEGRAL_POOL.length; i++)
        {
            POS_INTEGRAL_POOL[i] = new CachedDoubleValue(i);
        }

        for (int i = 0; i < NEG_INTEGRAL_POOL.length; i++)
        {
            int v = -i;
            NEG_INTEGRAL_POOL[i] = new CachedDoubleValue(v);
        }
    }

    private final String textValue;
    private final String toString;
    private final char[] toStringCharArray;

    CachedDoubleValue(double value)
    {
        super(value);
        this.textValue = Double.toString(value)
                .intern();
        this.toString = IValue.DOUBLE_CODE + textValue;
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
