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
 * The IValue for a double.
 * 
 * @author Ramon Servadei
 */
public final class DoubleValue extends AbstractValue
{
    private double value;

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
        return target == null || !(target instanceof DoubleValue) ? defaultValue : target.doubleValue();
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

    @Override
    public TypeEnum getType()
    {
        return TypeEnum.DOUBLE;
    }

    @Override
    public long longValue()
    {
        return (long) this.value;
    }

    @Override
    public double doubleValue()
    {
        return this.value;
    }

    @Override
    public String textValue()
    {
        return Double.toString(this.value);
    }

    @Override
    public void fromString(String value)
    {
        this.value = Double.valueOf(value).doubleValue();
    }

    @Override
    void fromChars(char[] chars, int start, int len)
    {
        this.value = Double.valueOf(new String(chars, start, len)).doubleValue();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(this.value);
        result = prime * result + (int) (temp ^ (temp >>> 32));
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
        DoubleValue other = (DoubleValue) obj;
        return is.eq(this.value, other.value);
    }
}
