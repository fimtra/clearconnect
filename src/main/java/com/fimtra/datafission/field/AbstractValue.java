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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.fimtra.datafission.IValue;
import com.fimtra.util.ByteBufferUtils;
import com.fimtra.util.Log;

/**
 * Base class for IValue objects.
 * 
 * @author Ramon Servadei
 */
public abstract class AbstractValue implements IValue
{
    static final Charset UTF8 = Charset.forName("UTF-8");

    /**
     * Construct the {@link IValue} indicated by the type from the byte[] data in the
     * {@link ByteBuffer}
     * 
     * @param type
     *            the type of {@link IValue} to construct
     * @param buffer
     *            the buffer holding the byte[] data for the value
     * @param len
     *            the end of the data in the buffer
     * @return the constructed {@link IValue}
     */
    public static IValue fromBytes(TypeEnum type, ByteBuffer buffer, int len)
    {
        switch(type)
        {
            case DOUBLE:
                return new DoubleValue(Double.longBitsToDouble(buffer.getLong()));
            case LONG:
                return LongValue.valueOf(buffer.getLong());
            case TEXT:
                return TextValue.valueOf(new String(ByteBufferUtils.getBytesFromBuffer(buffer, len), UTF8));
            case BLOB:
                return new BlobValue(ByteBufferUtils.getBytesFromBuffer(buffer, len));
            default :
                throw new UnsupportedOperationException("Unhandled type: " + type);
        }
    }

    /**
     * Get the byte[] for the value
     * 
     * @param value
     *            the value to convert to byte[]
     * @param reuse8ByteBuffer
     *            a reusable 8 byte buffer
     * @return the byte[] representing the data for the value
     */
    public static byte[] toBytes(IValue value, ByteBuffer reuse8ByteBuffer)
    {
        switch(value.getType())
        {
            case DOUBLE:
                reuse8ByteBuffer.putLong(Double.doubleToRawLongBits(value.doubleValue()));
                return reuse8ByteBuffer.array();
            case LONG:
                reuse8ByteBuffer.putLong(value.longValue());
                return reuse8ByteBuffer.array();
            case TEXT:
                return value.textValue().getBytes(UTF8);
            case BLOB:
                return ((BlobValue) value).value;
            default :
                throw new UnsupportedOperationException("Unhandled type: " + value.getType());
        }
    }

    /**
     * Construct the correct {@link IValue} object to represent the CharBuffer representation.
     * <p>
     * This is more efficient than {@link #constructFromStringValue(String)} as it skips the
     * internal char[] copying associated with {@link String} operations.
     * 
     * @param cBuf
     *            the CharBuffer version of the IValue implementation, position=0, limit=char[]
     *            length
     * @return the correct {@link IValue} type instance initialised to the value in the string
     *         argument
     */
    public static IValue constructFromCharValue(char[] chars, int start, int len)
    {
        if (chars == null || chars.length == 0 || len == 0)
        {
            return null;
        }
        switch(chars[0])
        {
            case IValue.LONG_CODE:
                return LongValue.valueOf(chars, start + 1, len - 1);
            case IValue.DOUBLE_CODE:
                return new DoubleValue(chars, start + 1, len - 1);
            case IValue.TEXT_CODE:
                return TextValue.valueOf(chars, start + 1, len - 1);
            case IValue.BLOB_CODE:
                return new BlobValue(chars, start + 1, len - 1);
            default :
                throw new UnsupportedOperationException("Unhandled type: " + new String(chars, start + 1, len - 1));
        }
    }

    @Override
    public final String toString()
    {
        final String type = getType().toString();
        final String textValue = textValue();
        return new StringBuilder(textValue.length() + type.length()).append(type).append(textValue).toString();
    }

    @Override
    public int compareTo(IValue o)
    {
        if (o == null)
        {
            return 1;
        }
        try
        {
            switch(o.getType())
            {
                case DOUBLE:
                    return (int) (this.doubleValue() - o.doubleValue());
                case LONG:
                case BLOB:
                    return (int) (this.longValue() - o.longValue());
                case TEXT:
                    return this.textValue().compareTo(o.textValue());
            }
        }
        catch (NumberFormatException e)
        {
            Log.log(this, "NumberFormatException: ", e.getMessage());
            return 0;
        }
        return 0;
    }

    @Override
    public byte[] byteValue()
    {
        return toBytes(this, ByteBuffer.allocate(8));
    }
}
