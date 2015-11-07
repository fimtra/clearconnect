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

import com.fimtra.datafission.IValue;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SerializationUtils;
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
            return (T) SerializationUtils.fromByteArray(value.getBytes());
        }
        catch (Exception e)
        {
            Log.log(BlobValue.class, "Could not construct object from ", ObjectUtils.safeToString(value));
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
            Log.log(BlobValue.class, "Could not construct object from ", ObjectUtils.safeToString(value));
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
            Log.log(BlobValue.class, "Could not construct BlobValue from ", ObjectUtils.safeToString(object));
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
                default :
                    POS_HEX_CODES[b] = (hexString.substring(6)).toCharArray();
            }
        }
        int index;
        for (int i = NEG_HEX_CODES.length; i > 0; i--)
        {
            hexString = Integer.toHexString(-i);
            index = NEG_HEX_CODES.length - i;
            switch(hexString.length())
            {
                case 1:
                    NEG_HEX_CODES[index] = ("0" + hexString).toCharArray();
                    break;
                case 2:
                    NEG_HEX_CODES[index] = hexString.toCharArray();
                    break;
                default :
                    NEG_HEX_CODES[index] = hexString.substring(6).toCharArray();
            }
        }
    }

    private static final byte decodeHex(char c)
    {
        switch(c)
        {
            case '0':
                return 0x0;
            case '1':
                return 0x1;
            case '2':
                return 0x2;
            case '3':
                return 0x3;
            case '4':
                return 0x4;
            case '5':
                return 0x5;
            case '6':
                return 0x6;
            case '7':
                return 0x7;
            case '8':
                return 0x8;
            case '9':
                return 0x9;
            case 'a':
                return 0xa;
            case 'b':
                return 0xb;
            case 'c':
                return 0xc;
            case 'd':
                return 0xd;
            case 'e':
                return 0xe;
            case 'f':
                return 0xf;
            case 'A':
                return 0xa;
            case 'B':
                return 0xb;
            case 'C':
                return 0xc;
            case 'D':
                return 0xd;
            case 'E':
                return 0xe;
            case 'F':
                return 0xf;                
        }
        throw new IllegalArgumentException("Unhandled char:" + c);
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
        return target == null || !(target instanceof BlobValue) ? defaultValue : target.byteValue();
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
        fromString(value);
    }

    BlobValue()
    {
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
        final char[] cbuf = new char[this.value.length * 2];
        char[] code;
        byte val;
        int bufPtr = 0;
        for (int i = 0; i < this.value.length; i++)
        {
            val = this.value[i];
            if (val < 0)
            {
                code = NEG_HEX_CODES[val & 0x7f];
                cbuf[bufPtr++] = code[0];
                cbuf[bufPtr++] = code[1];
            }
            else
            {
                code = (POS_HEX_CODES[val]);
                cbuf[bufPtr++] = code[0];
                cbuf[bufPtr++] = code[1];
            }
        }
        // note: a full array copy happens when constructing the string
        return new String(cbuf);
    }

    @Override
    void fromString(String value)
    {
        if (value.length() % 2 != 0)
        {
            throw new IllegalStateException("BlobValue text length should be divisible by 2");
        }
        this.value = new byte[value.length() / 2];
        final char[] hexStream = value.toCharArray();
        final int len = hexStream.length;
        int j = 0;
        for (int i = 0; i < len;)
        {
            this.value[j++] = (byte) ((byte) (decodeHex(hexStream[i++]) << 4) | decodeHex(hexStream[i++]));
        }
    }

    @Override
    void fromChars(char[] chars, int start, int len)
    {
        if (len % 2 != 0)
        {
            throw new IllegalStateException("BlobValue text length should be divisible by 2");
        }
        this.value = new byte[len / 2];
        final char[] hexStream = chars;
        int j = 0;
        for (int i = start; i < len;)
        {
            this.value[j++] = (byte) ((byte) (decodeHex(hexStream[i++]) << 4) | decodeHex(hexStream[i++]));
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
        BlobValue other = (BlobValue) obj;
        return is.eq(this.value, other.value);
    }
}
