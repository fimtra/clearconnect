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
import com.fimtra.util.StringAppender;

/**
 * The IValue for a double.
 * 
 * @author Ramon Servadei
 */
public class DoubleValue extends AbstractValue
{
    private final double value;

    /**
     * Static short-hand constructor for a {@link DoubleValue}
     */
    public static DoubleValue valueOf(double value)
    {
        return new DoubleValue(value);
    }

    /**
     * Get a double from the passed in IValue, returning the defaultValue if the IValue is
     * <code>null</code> or not a DoubleValue
     * 
     * @param target
     *            the IValue to extract a double from
     * @param defaultValue
     *            the default value
     * @return the double value of the IValue or the defaultValue if the IValue is <code>null</code>
     *         or not a DoubleValue
     */
    public static double get(IValue target, double defaultValue)
    {
        return (target instanceof DoubleValue) ? target.doubleValue() : defaultValue;
    }

    /** Initialises to represent NaN. */
    DoubleValue()
    {
        this(Double.NaN);
    }

    public DoubleValue(double value)
    {
        super();
        this.value = value;
    }

    DoubleValue(char[] chars, int start, int len)
    {
        this.value = Double.parseDouble(new String(chars, start, len));
    }

    @Override
    public final TypeEnum getType()
    {
        return TypeEnum.DOUBLE;
    }

    @Override
    public final long longValue()
    {
        return (long) this.value;
    }

    @Override
    public final double doubleValue()
    {
        return this.value;
    }

    @Override
    public String textValue()
    {
        return Double.toString(value);
    }
    
    @Override
    public final StringAppender toStringAppender()
    {
        return appendTo(new StringAppender(28));
    }

    @Override
    public final int hashCode()
    {
        final long bits = Double.doubleToLongBits(this.value);
        return (int) (bits ^ (bits >>> 32));
    }

    @Override
    public final boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof DoubleValue))
        {
            return false;
        }
        return Double.doubleToLongBits(this.value) == Double.doubleToLongBits(((DoubleValue) obj).value);
    }

    @Override
    public StringAppender appendTo(StringAppender stringAppender)
    {
        return stringAppender.append(DOUBLE_CODE)
                .append(this.value);
    }
}
