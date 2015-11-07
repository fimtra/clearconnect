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

import java.nio.ByteBuffer;

/**
 * Utilities for working with a {@link ByteBuffer}
 * 
 * @author Ramon Servadei
 */
public abstract class ByteBufferUtils
{
    public static final int BLOCK_SIZE = 1024;

    private ByteBufferUtils()
    {
    }

    /**
     * Copy the source into the target at the current position in the target
     * 
     * @see #copyBytesIntoBuffer(byte[], ByteBuffer)
     */
    public static final ByteBuffer copyBufferIntoBuffer(ByteBuffer source, ByteBuffer target)
    {
        return copyBytesIntoBuffer(ByteBufferUtils.asBytes(source), target);
    }

    /**
     * @return a new byte[] that holds the data in the buffer from 0-limit in the buffer
     */
    public static final byte[] asBytes(ByteBuffer buffer)
    {
        final byte[] data = new byte[buffer.limit()];
        System.arraycopy(buffer.array(), 0, data, 0, buffer.limit());
        return data;
    }

    /**
     * Copy the data into the buffer, resizing it if needed
     * 
     * @return the {@link ByteBuffer} with the data added to it (resized if needed)
     */
    public static final ByteBuffer copyBytesIntoBuffer(byte[] data, ByteBuffer buffer)
    {
        ByteBuffer localBuf = buffer;
        try
        {
            if (data.length + localBuf.position() > localBuf.limit())
            {
                // resize the buffer
                final ByteBuffer resizedBuffer = ByteBuffer.allocate(localBuf.capacity() + data.length + BLOCK_SIZE);
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
            Log.log(ByteBufferUtils.class, "data.length=" + data.length + ", buffer.array=" + localBuf.array().length
                + ", buffer=" + localBuf);
            throw e;
        }
        return localBuf;
    }

    /**
     * Get bytes out of a buffer, moves the buffer position to where the bytes finished
     * 
     * @param len
     *            the number of bytes to retrieve
     */
    public static final byte[] getBytesFromBuffer(ByteBuffer buffer, int len)
    {
        final int start = buffer.position();
        final byte[] bytes = new byte[len];
        System.arraycopy(buffer.array(), start, bytes, 0, len);
        buffer.position(start + len);
        return bytes;
    }

    /**
     * Add the character to the buffer, extending the buffer if needed
     * 
     * @return the same buffer if not extended, a new buffer if extended
     */
    public static final ByteBuffer putChar(char c, ByteBuffer buffer)
    {
        ByteBuffer localBuf = buffer;
        if (capacityRemaining(buffer) < 2)
        {
            localBuf = extendBuffer(buffer, BLOCK_SIZE);
        }
        localBuf.putChar(c);
        return localBuf;
    }

    /**
     * @see #putChar(char, ByteBuffer)
     */
    public static final ByteBuffer put(byte b, ByteBuffer buffer)
    {
        ByteBuffer localBuf = buffer;
        if (capacityRemaining(buffer) < 1)
        {
            localBuf = extendBuffer(buffer, BLOCK_SIZE);
        }
        localBuf.put(b);
        return localBuf;
    }

    /**
     * Add the int to the buffer, extending the buffer if needed
     * 
     * @return the same buffer if not extended, a new buffer if extended
     */
    public static final ByteBuffer putInt(int i, ByteBuffer buffer)
    {
        ByteBuffer localBuf = buffer;
        if (capacityRemaining(buffer) < 4)
        {
            localBuf = extendBuffer(buffer, BLOCK_SIZE);
        }
        localBuf.putInt(i);
        return localBuf;
    }

    /**
     * @see #putChar(char, ByteBuffer)
     */
    public static final ByteBuffer put(byte[] b, ByteBuffer buffer)
    {
        ByteBuffer localBuf = buffer;
        if (capacityRemaining(buffer) < b.length)
        {
            localBuf = extendBuffer(buffer, b.length + BLOCK_SIZE);
        }
        localBuf.put(b);
        return localBuf;
    }

    /**
     * @return a new buffer with the contents of the original buffer and extended by the size
     */
    private static ByteBuffer extendBuffer(ByteBuffer buffer, int size)
    {
        final ByteBuffer localBuffer = ByteBuffer.allocate(buffer.capacity() + size);
        System.arraycopy(buffer.array(), 0, localBuffer.array(), 0, buffer.position());
        localBuffer.position(buffer.position());
        return localBuffer;
    }

    /**
     * @return the remaining capacity in the buffer
     */
    private static int capacityRemaining(ByteBuffer buffer)
    {
        return buffer.limit() - buffer.position();
    }
}