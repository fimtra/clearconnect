/*
 * Copyright (c) 2014 Ramon Servadei 
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

import java.io.Serializable;
import java.util.Arrays;

import com.fimtra.datafission.IValue;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SerializationUtils;
import com.fimtra.util.StringAppender;
import com.fimtra.util.is;
/**
 * The IValue for a binary large object
 * 
 * @author Ramon Servadei
 */
public final class BlobValue extends AbstractValue
{
    /**
     * Return an object held in the {@link BlobValue} byte[]
     * <p>
     * <b>This uses object serialization to convert the byte[] into the object</b>
     * 
     * @param value
     *            the {@link BlobValue}
     * @return the object held in the blob's internal byte[], <code>null</code> if a problem occurs
     */
    @SuppressWarnings("unchecked")
    public static <T> T fromBlob(BlobValue value)
    {
        try
        {
            return SerializationUtils.fromByteArray(value.getBytes());
        }
        catch (Exception e)
        {
            Log.log(BlobValue.class, "Could not construct object from " + ObjectUtils.safeToString(value), e);
            return null;
        }
    }

    /**
     * Convenience method to construct from an {@link IValue} reference
     * 
     * @see #fromBlob(BlobValue)
     */
    public static <T> T fromBlob(IValue value)
    {
        try
        {
            return fromBlob((BlobValue) value);
        }
        catch (Exception e)
        {
            Log.log(BlobValue.class, "Could not construct object from " + ObjectUtils.safeToString(value), e);
            return null;
        }
    }

    /**
     * Construct a {@link BlobValue} to represent the object passed in.
     * <p>
     * <b>This uses object serialization to obtain the byte[] representing the object</b>
     * 
     * @param object
     *            the object to wrap in the {@link BlobValue}
     * @return a {@link BlobValue} wrapping the object, <code>null</code> if a problem occurs
     */
    public static BlobValue toBlob(Serializable object)
    {
        try
        {
            return new BlobValue(SerializationUtils.toByteArray(object));
        }
        catch (Exception e)
        {
            Log.log(BlobValue.class, "Could not construct BlobValue from " + ObjectUtils.safeToString(object), e);
            return null;
        }
    }

    private final static char[][] POS_HEX_CODES = new char[128][2];
    /** arranged so NEG_HEX_CODES[0]=-128, [1]=-127, [2]=-126 ... [127]=-1 */
    private final static char[][] NEG_HEX_CODES = new char[128][2];
    static
    {
        POS_HEX_CODES[0] = "00".toCharArray();
        String hexString;
        for (int b = 1; b < POS_HEX_CODES.length; b++)
        {
            hexString = Integer.toHexString(b);
            switch(hexString.length())
            {
                case 1:
                    POS_HEX_CODES[b] = ("0" + hexString).toCharArray();
                    break;
                case 2:
                    POS_HEX_CODES[b] = hexString.toCharArray();
                    break;
            }
        }
        int index;
        for (int i = NEG_HEX_CODES.length; i > 0; i--)
        {
            hexString = Integer.toHexString(-i);
            index = NEG_HEX_CODES.length - i;
            // negative hex codes always start ffffff80, we want 80
            NEG_HEX_CODES[index] = hexString.substring(6).toCharArray();
        }
    }

    /** maps char to hex value for the 4 least-significant bits (LSB) of a byte */
    static final byte[] LSB_HEX_VALS = new byte[103];
    /** decode for the 4 most-significant bits (msb) of a byte */
    static final byte[] MSB_HEX_VALS = new byte[103];
    static
    {
        Arrays.fill(LSB_HEX_VALS, (byte) -1);

        LSB_HEX_VALS['0'] = 0x0;
        LSB_HEX_VALS['1'] = 0x1;
        LSB_HEX_VALS['2'] = 0x2;
        LSB_HEX_VALS['3'] = 0x3;
        LSB_HEX_VALS['4'] = 0x4;
        LSB_HEX_VALS['5'] = 0x5;
        LSB_HEX_VALS['6'] = 0x6;
        LSB_HEX_VALS['7'] = 0x7;
        LSB_HEX_VALS['8'] = 0x8;
        LSB_HEX_VALS['9'] = 0x9;
        LSB_HEX_VALS['a'] = 0xa;
        LSB_HEX_VALS['b'] = 0xb;
        LSB_HEX_VALS['c'] = 0xc;
        LSB_HEX_VALS['d'] = 0xd;
        LSB_HEX_VALS['e'] = 0xe;
        LSB_HEX_VALS['f'] = 0xf;
        LSB_HEX_VALS['A'] = 0xa;
        LSB_HEX_VALS['B'] = 0xb;
        LSB_HEX_VALS['C'] = 0xc;
        LSB_HEX_VALS['D'] = 0xd;
        LSB_HEX_VALS['E'] = 0xe;
        LSB_HEX_VALS['F'] = 0xf;

        for (int c = 0; c < MSB_HEX_VALS.length; c++)
        {
            MSB_HEX_VALS[c] = (byte) (LSB_HEX_VALS[c] << 4);
        }
    }

    /**
     * Static short-hand constructor for a {@link BlobValue}
     */
    public static BlobValue valueOf(byte[] value)
    {
        return new BlobValue(value);
    }

    /**
     * Get a byte[] from the passed in IValue, returning the defaultValue if the IValue is
     * <code>null</code> or not a BlobValue
     * 
     * @param target
     *            the IValue to extract a byte[] from
     * @param defaultValue
     *            the default value
     * @return the byte[] value of the IValue or the defaultValue if the IValue is <code>null</code>
     *         or not a BlobValue
     */
    public static byte[] get(IValue target, byte[] defaultValue)
    {
        return (target instanceof BlobValue) ? target.byteValue() : defaultValue;
    }

    byte[] value;

    /**
     * @param value
     *            the byte[] of this blob
     */
    public BlobValue(byte[] value)
    {
        this();
        this.value = value;
    }

    /**
     * @param value
     *            the hex string for the byte[] of this blob
     */
    public BlobValue(String value)
    {
        this();
        final char[] charArray = value.toCharArray();
        fromChars(charArray, 0, charArray.length);
    }

    BlobValue()
    {
    }

    BlobValue(char[] chars, int start, int len)
    {
        this();
        fromChars(chars, start, len);
    }

    public byte[] getBytes()
    {
        return this.value;
    }

    @Override
    public TypeEnum getType()
    {
        return TypeEnum.BLOB;
    }

    @Override
    public long longValue()
    {
        return this.value.length;
    }

    @Override
    public double doubleValue()
    {
        return longValue();
    }

    @Override
    public String textValue()
    {
        final char[] cbuf = new char[this.value.length << 1];
        // note: a full array copy happens when constructing the string
        return new String(fillCharArray(cbuf, 0));
    }

    private char[] fillCharArray(char[] cbuf, int bufPtr)
    {
        char[] code;
        for (byte b : this.value)
        {
            code = (b & 0x80) == 0x80 ? NEG_HEX_CODES[b & 0x7f] : POS_HEX_CODES[b];
            cbuf[bufPtr++] = code[0];
            cbuf[bufPtr++] = code[1];
        }
        return cbuf;
    }

    @Override
    public StringAppender toStringAppender()
    {
        final int len = (this.value.length << 1);
        final StringAppender stringAppender = new StringAppender(len + 1);
        final char[] chars = stringAppender.reserveAndGet(len + 1);
        chars[0] = IValue.BLOB_CODE;
        fillCharArray(chars, 1);
        return stringAppender;
    }
    
    void fromChars(char[] chars, int start, int len)
    {
        if ((len & 0x1) != 0)
        {
            throw new IllegalStateException("BlobValue text length should be divisible by 2");
        }
        this.value = new byte[len >> 1];
        int j = 0;
        for (int i = start; i < len; )
        {
            this.value[j++] = (byte) (MSB_HEX_VALS[chars[i++]] | LSB_HEX_VALS[chars[i++]]);
        }
    }

    @Override
    public int hashCode()
    {
        if (this.value == null)
        {
            return 0;
        }

        int result = 1;
        for (byte element : this.value)
        {
            result = 31 * result + element;
        }

        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof BlobValue))
        {
            return false;
        }
        return is.eq(this.value, ((BlobValue) obj).value);
    }

    @Override
    public StringAppender appendTo(StringAppender stringAppender)
    {
        int start = stringAppender.getLength();
        final int len = this.value.length << 1;
        final char[] chars = stringAppender.reserveAndGet(len + 1);
        chars[start++] = IValue.BLOB_CODE;
        fillCharArray(chars, start);
        return stringAppender;
    }
}
