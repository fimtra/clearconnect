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
 * The IValue for a string.
 * <p>
 * The {@link #textValue()} will <b>never</b> return <code>null</code>
 * 
 * @author Ramon Servadei
 */
public final class TextValue extends AbstractValue
{
    static final String NULL = "null";

    final static TextValue BLANK = new TextValue("");

    private String value;

    /**
     * Static short-hand constructor for a {@link TextValue}
     */
    public static TextValue valueOf(String value)
    {
        if ("".equals(value))
        {
            return BLANK;
        }
        return new TextValue(value);
    }

    /**
     * Get a String from the passed in IValue, returning the defaultValue if the IValue is
     * <code>null</code>.
     * <p>
     * {@link DoubleValue}, {@link LongValue} and {@link BlobValue} can be converted to String.
     * 
     * @param target
     *            the IValue to extract a String from
     * @param defaultValue
     *            the value to return if the target argument is <code>null</code>
     * @return the String value of the IValue or the defaultValue if the IValue is <code>null</code>
     */
    public static String get(IValue target, String defaultValue)
    {
        return target == null ? defaultValue : target.textValue();
    }
    
    /** Initialises the string value to "null". */
    TextValue()
    {
        this(NULL);
    }

    /**
     * Construct the text value to represent the given string
     * 
     * @param value
     *            the value to construct this with
     * @throws IllegalArgumentException
     *             if the value is null
     */
    public TextValue(String value)
    {
        super();
        if (value == null)
        {
            throw new IllegalArgumentException("null values are not allowed");
        }
        setValue(value);
    }

    @Override
    public TypeEnum getType()
    {
        return TypeEnum.TEXT;
    }

    @Override
    public long longValue()
    {
        return (this.value == NULL || this.value == "") ? 0 : Long.valueOf(this.value).longValue();
    }

    @Override
    public double doubleValue()
    {
        try
        {
            return (this.value == NULL || this.value == "") ? Double.NaN : Double.valueOf(this.value).doubleValue();
        }
        catch (NumberFormatException e)
        {
            return Double.NaN;
        }
    }

    @Override
    public String textValue()
    {
        return this.value;
    }

    @Override
    public void fromString(String value)
    {
        setValue(value);
    }

    @Override
    void fromChars(char[] chars, int start, int len)
    {
        if (chars == null)
        {
            setValue(null);
        }
        else
        {
            setValue(new String(chars, start, len));
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.value == null) ? 0 : this.value.hashCode());
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
        TextValue other = (TextValue) obj;
        return is.eq(this.value, other.value);
    }

    private void setValue(String value)
    {
        this.value = (value == null ? NULL : (NULL.equals(value) ? NULL : value));
    }

}
