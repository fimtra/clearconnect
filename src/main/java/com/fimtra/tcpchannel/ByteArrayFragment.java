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
package com.fimtra.tcpchannel;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.fimtra.util.ByteArrayPool;
import com.fimtra.util.IReusableObject;
import com.fimtra.util.IReusableObjectBuilder;
import com.fimtra.util.MultiThreadReusableObjectPool;
import com.fimtra.util.is;

/**
 * This class and its counterpart {@link TxByteArrayFragment} allow large byte[] data to be broken
 * into smaller parts and then rebuilt back into the original byte[]. A
 * {@link ByteArrayFragmentResolver} consumes the fragments and re-builds the original data byte[].
 * <p>
 * NOTE: fragments are equal by their ID only - this indicates the data they are fragments of. The
 * sequenceID is the sequence of the fragment for the data.
 * 
 * @see TxByteArrayFragment
 * @author Ramon Servadei
 */
class ByteArrayFragment implements IReusableObject
{
    static final MultiThreadReusableObjectPool<ByteArrayFragment> BYTE_ARRAY_FRAGMENT_POOL =
            new MultiThreadReusableObjectPool<>("RxFragmentPool",
                    new IReusableObjectBuilder<ByteArrayFragment>()
                    {
                        @Override
                        public ByteArrayFragment newInstance()
                        {
                            return new ByteArrayFragment(BYTE_ARRAY_FRAGMENT_POOL);
                        }
                    }, ByteArrayFragment::reset, TcpChannelProperties.Values.RX_FRAGMENT_POOL_MAX_SIZE);

    /**
     * Utility methods exclusive to a {@link ByteArrayFragment}
     * 
     * @author Ramon Servadei
     */
    static final class ByteArrayFragmentUtils
    {
        /**
         * Split a string "1|2|3|" into int[]{1,2,3}
         */
        static int[] split3NumbersByPipe(String numberString)
        {
            int[] numbers = new int[3];
            int index = 0;
            final char[] chars = numberString.toCharArray();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < chars.length; i++)
            {
                switch(chars[i])
                {
                    case '|':
                        numbers[index++] = Integer.parseInt(sb.toString());
                        if (index == numbers.length)
                        {
                            return numbers;
                        }
                        sb = new StringBuilder();
                        break;
                    default:
                        sb.append(chars[i]);
                }
            }
            numbers[index] = Integer.parseInt(sb.toString());
            return numbers;
        }

        /**
         * Ensures that the string is 4 chars long, left padded with 0, e.g. 123 is "0123"
         */
        static String pad4DigitWithLeadingZeros(int number)
        {
            if (number > 9999)
            {
                throw new IllegalArgumentException("Cannot be larger than 9999: " + number);
            }
            final StringBuilder result = new StringBuilder(4);
            if (number > 9)
            {
                if (number > 99)
                {
                    if (number > 999)
                    {
                        return result.append(number).toString();
                    }
                    else
                    {
                        return result.append("0").append(number).toString();
                    }
                }
                else
                {
                    return result.append("00").append(number).toString();
                }
            }
            else
            {
                return result.append("000").append(number).toString();
            }
        }

    }

    /**
     * If an incorrect sequence is received when rebuilding a data byte[] from received
     * {@link ByteArrayFragment} instances
     * 
     * @author Ramon Servadei
     */
    static class IncorrectSequenceException extends Exception
    {
        private static final long serialVersionUID = 1L;

        IncorrectSequenceException(String message)
        {
            super(message);
        }
    }

    /**
     * Resolve a {@link ByteArrayFragment} from the received {@link ByteBuffer} with the header
     * encoded in raw byte form. The byte[] will be one of those returned from
     * {@link TxByteArrayFragment#toTxBytesRawByteHeader()}
     * 
     * @param rxData
     *            the {@link ByteBuffer} for a ByteArrayFragment
     * @return a ByteArrayFragment
     */
    static ByteArrayFragment fromRxBytesRawByteHeader(ByteBuffer rxData)
    {
        final int id = rxData.getInt();
        final int sequenceId = rxData.getInt();
        final byte lastElement = rxData.get();

        return BYTE_ARRAY_FRAGMENT_POOL.get().initialise(id, sequenceId, lastElement, rxData.array(),
            rxData.position(), rxData.limit() - rxData.position());
    }

    /**
     * Resolve a {@link ByteArrayFragment} from the received {@link ByteBuffer} with the header
     * encoded in UTF8 characters. The byte[] will be one of those returned from
     * {@link TxByteArrayFragment#toTxBytesUTF8Header}
     * 
     * @param rxData
     *            the {@link ByteBuffer} for a ByteArrayFragment
     * @return a ByteArrayFragment
     */
    static ByteArrayFragment fromRxBytesUTF8Header(ByteBuffer rxData)
    {
        final byte[] lenArr = new byte[4];
        rxData.get(lenArr);
        // UTF8 has 1 byte per char
        final int headerLen = Integer.parseInt(new String(lenArr, StandardCharsets.UTF_8));

        // we don't want the first "|"
        rxData.get();
        final byte[] headerArr = new byte[headerLen - 1];
        rxData.get(headerArr);
        final String header = new String(headerArr, StandardCharsets.UTF_8);

        final int[] parts = ByteArrayFragmentUtils.split3NumbersByPipe(header);
        final int id = parts[0];
        final int sequenceId = parts[1];
        final byte lastElement = (byte) parts[2];

        return BYTE_ARRAY_FRAGMENT_POOL.get().initialise(id, sequenceId, lastElement, rxData.array(),
            rxData.position(), rxData.limit() - rxData.position());
    }

    int id;
    volatile byte v_lastElement;
    int offset;
    int length;
    int sequenceId;
    volatile byte[] v_data;
    @SuppressWarnings("rawtypes")
    final MultiThreadReusableObjectPool poolRef;

    @SuppressWarnings("rawtypes")
    ByteArrayFragment(MultiThreadReusableObjectPool poolRef)
    {
        super();
        this.poolRef = poolRef;
    }

    @Override
    public void reset()
    {
        initialise(-1, -1, (byte) -1, null, -1, -1);
    }

    final ByteArrayFragment initialise(int id, int sequenceId, byte lastElement, byte[] data, int offset, int len)
    {
        this.id = id;
        this.sequenceId = sequenceId;
        this.offset = offset;
        this.length = len;
        // write volatile last to write all vars in scope to main-memory (write visibility guarantee)
        this.v_data = data;
        this.v_lastElement = lastElement;
        return this;
    }

    /**
     * @return <code>true</code> if this fragment is the last one for the data
     */
    final boolean isLastElement()
    {
        return this.v_lastElement != 0;
    }

    /**
     * Merge this fragment with the other one - effectively append the other's data to this one
     * 
     * @param other
     *            the fragment to merge into this one
     * @return this fragment instance (NOT the other)
     */
    final ByteArrayFragment merge(ByteArrayFragment other) throws IncorrectSequenceException
    {
        // read volatile first to force all vars in scope to read from main memory
        final byte[] data = this.v_data;
        final int offset = this.offset;
        final int length = this.length;
        int sequenceId = this.sequenceId;

        final byte[] other_data = other.v_data;
        final int other_length = other.length;
        final int other_offset = other.offset;
        final int other_sequenceId = other.sequenceId;

        if (++sequenceId != other_sequenceId)
        {
            throw new IncorrectSequenceException("Expected " + (sequenceId) + " but got " + other_sequenceId);
        }

        byte[] d = ByteArrayPool.get(length + other_length);
        System.arraycopy(data, offset, d, 0, length);
        System.arraycopy(other_data, other_offset, d, length, other_length);
        this.sequenceId = sequenceId;
        this.offset = 0;
        this.length += other_length;
        // write volatile last to write all vars in scope to main-memory (write visibility guarantee)
        this.v_data = d;
        return this;
    }

    /**
     * @return the data resolved from all the fragments
     */
    final ByteBuffer getData()
    {
        // read volatile first to force all vars in scope to read from main memory
        final byte[] data = this.v_data;
        if (data == null)
        {
            return null;
        }
        
        final int offset = this.offset;
        final int length = this.length;
        
        if (offset != 0 || length != data.length)
        {
            final byte[] d = ByteArrayPool.get(length);
            System.arraycopy(data, offset, d, 0, length);
            // don't free the old data byte[] - could be the rxData permanent byte[]
            this.offset = 0;
            // write volatile last to write all vars in scope to main-memory (write visibility guarantee)
            this.v_data = d;
            return ByteBuffer.wrap(d, 0, length);
        }
        return ByteBuffer.wrap(data, offset, length);
    }

    @Override
    public final int hashCode()
    {
        return this.id;
    }

    @Override
    public final boolean equals(Object obj)
    {
        if (is.same(this, obj))
        {
            return true;
        }
        if (!(obj instanceof ByteArrayFragment))
        {
            return false;
        }
        ByteArrayFragment other = (ByteArrayFragment) obj;
        return is.eq(this.id, other.id);
    }

    @Override
    public final String toString()
    {
        // read volatile first to force all vars in scope to read from main memory
        final byte lastElement = this.v_lastElement;
        final int id = this.id;
        final int length = this.length;
        final int sequenceId = this.sequenceId;

        return getClass().getSimpleName() + " [id=" + id + ", sequenceId=" + sequenceId + ", lastElement="
            + lastElement + ", data=" + length + "]";
    }

    @SuppressWarnings("unchecked")
    void free()
    {
        this.poolRef.offer(this);
    }
}
