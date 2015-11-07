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
package com.fimtra.util;

import java.nio.CharBuffer;

/**
 * Utilities for working with a {@link CharBuffer}
 * 
 * @author Ramon Servadei
 */
public abstract class CharBufferUtils
{
    public static final int BLOCK_SIZE = 64;

    private CharBufferUtils()
    {
    }

    /**
     * Copy the source into the target at the current position in the target
     * 
     * @see #copyCharsIntoBuffer(char[], CharBuffer)
     */
    public static final CharBuffer copyBufferIntoBuffer(CharBuffer source, CharBuffer target)
    {
        return copyCharsIntoBuffer(CharBufferUtils.asChars(source), target);
    }

    /**
     * @return a new char[] that holds the data in the buffer from 0-limit in the buffer
     */
    public static final char[] asChars(CharBuffer buffer)
    {
        final char[] data = new char[buffer.limit()];
        System.arraycopy(buffer.array(), 0, data, 0, buffer.limit());
        return data;
    }

    /**
     * Copy the data into the buffer, resizing it if needed
     * 
     * @return the {@link CharBuffer} with the data added to it (resized if needed)
     */
    public static final CharBuffer copyCharsIntoBuffer(char[] data, CharBuffer buffer)
    {
        CharBuffer localBuf = buffer;
        try
        {
            if (data.length + localBuf.position() > localBuf.limit())
            {
                // resize the buffer
                final CharBuffer resizedBuffer = CharBuffer.allocate(localBuf.capacity() + data.length + BLOCK_SIZE);
                final int position = localBuf.position();
                System.arraycopy(localBuf.array(), 0, resizedBuffer.array(), 0, position);
                localBuf = resizedBuffer;
                System.arraycopy(data, 0, localBuf.array(), position, data.length);
                localBuf.position(position + data.length);
            }
            else
            {
                System.arraycopy(data, 0, localBuf.array(), localBuf.position(), data.length);
                localBuf.position(localBuf.position() + data.length);
            }
        }
        catch (RuntimeException e)
        {
            Log.log(CharBufferUtils.class, "data.length=" + data.length + ", buffer.array=" + localBuf.array().length
                + ", buffer=" + localBuf);
            throw e;
        }
        return localBuf;
    }

    /**
     * Get chars out of a buffer, moves the buffer position to where the chars finished
     * 
     * @param len
     *            the number of chars to retrieve
     */
    public static final char[] getCharsFromBuffer(CharBuffer buffer, int len)
    {
        final int start = buffer.position();
        final char[] chars = new char[len];
        System.arraycopy(buffer.array(), start, chars, 0, len);
        buffer.position(start + len);
        return chars;
    }

    /**
     * Copy the chars out of a buffer from 0 to the current position, resets the position to 0
     */
    public static final char[] getCharsFromBufferAndReset(CharBuffer buffer)
    {
        final int len = buffer.position();
        final char[] chars = new char[len];
        System.arraycopy(buffer.array(), 0, chars, 0, len);
        buffer.position(0);
        return chars;
    }

    /**
     * Add the character to the buffer, extending the buffer if needed
     * 
     * @return the same buffer if not extended, <b>a new buffer if extended</b>
     */
    public static final CharBuffer put(char c, CharBuffer buffer)
    {
        CharBuffer localBuf = buffer;
        if (capacityRemaining(buffer) < 1)
        {
            localBuf = extendBuffer(buffer, BLOCK_SIZE);
        }
        localBuf.put(c);
        return localBuf;
    }

    /**
     * @see #putChar(char, CharBuffer)
     */
    public static final CharBuffer put(char[] chars, CharBuffer buffer)
    {
        CharBuffer localBuf = buffer;
        if (capacityRemaining(buffer) < chars.length)
        {
            localBuf = extendBuffer(buffer, chars.length + BLOCK_SIZE);
        }
        localBuf.put(chars);
        return localBuf;
    }

    /**
     * @return a new buffer with the contents of the original buffer and extended by the size
     */
    private static CharBuffer extendBuffer(CharBuffer buffer, int size)
    {
        final CharBuffer localBuffer = CharBuffer.allocate(buffer.capacity() + size);
        System.arraycopy(buffer.array(), 0, localBuffer.array(), 0, buffer.position());
        localBuffer.position(buffer.position());
        return localBuffer;
    }

    /**
     * @return the remaining capacity in the buffer
     */
    private static int capacityRemaining(CharBuffer buffer)
    {
        return buffer.limit() - buffer.position();
    }
}