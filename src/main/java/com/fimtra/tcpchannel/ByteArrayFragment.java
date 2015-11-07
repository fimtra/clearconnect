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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.fimtra.util.is;

/**
 * This class allows large byte[] data to be broken into smaller parts, each part held in a
 * ByteArrayFragment object. A {@link ByteArrayFragmentResolver} consumes the fragments and
 * re-builds the original data byte[].
 * <p>
 * NOTE: fragments are equal by their ID only - this indicates the data they are fragments of. The
 * sequenceID is the sequence of the fragment for the data.
 * 
 * @author Ramon Servadei
 */
final class ByteArrayFragment
{
    private static final Charset UTF8 = Charset.forName("UTF-8");

    /**
     * Utility methods exclusive to a {@link ByteArrayFragment}
     * 
     * @author Ramon Servadei
     */
    static class ByteArrayFragmentUtils
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
                    default :
                        sb.append(chars[i]);
                }
            }
            numbers[index++] = Integer.parseInt(sb.toString());
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

    final static AtomicInteger ID_COUNTER = new AtomicInteger();

    /**
     * Break the byte[] into fragments.
     * 
     * @param data
     *            the data
     * @param maxFragmentInternalByteSize
     *            the maximum size of each {@link ByteArrayFragment} instance's internal byte[]
     * @return the list of ByteArrayFragment instances that collectively represent the data
     */
    static ByteArrayFragment[] getFragmentsForTxData(byte[] data, int maxFragmentInternalByteSize)
    {
        int id = ID_COUNTER.incrementAndGet();
        int fragmentCount =
            data.length < maxFragmentInternalByteSize ? 1
                : ((int) Math.ceil((data.length * (1 / (double) maxFragmentInternalByteSize))));
        ByteArrayFragment[] fragments = new ByteArrayFragment[fragmentCount];
        int pointer = 0;
        int remainder = data.length;
        byte[] fragmentData;
        for (int i = 0; i < fragmentCount; i++)
        {
            fragmentData = new byte[remainder > maxFragmentInternalByteSize ? maxFragmentInternalByteSize : remainder];
            System.arraycopy(data, pointer, fragmentData, 0, fragmentData.length);
            fragments[i] = new ByteArrayFragment(id, i, (byte) (i == (fragmentCount - 1) ? 1 : 0), fragmentData);
            pointer += fragmentData.length;
            remainder -= fragmentData.length;
        }
        return fragments;
    }

    /**
     * Resolve a {@link ByteArrayFragment} from the received byte[] with the header encoded in raw
     * byte form. The byte[] will be one of those returned from {@link #toTxBytesRawByteHeader()}
     * 
     * @param rxData
     *            the byte[] for a ByteArrayFragment
     * @return a ByteArrayFragment
     */
    static ByteArrayFragment fromRxBytesRawByteHeader(byte[] rxData)
    {
        final ByteBuffer buffer = ByteBuffer.wrap(rxData);
        final int id = buffer.getInt();
        final int sequenceId = buffer.getInt();
        final byte lastElement = buffer.get();
        final byte[] data = new byte[buffer.array().length - 9];
        System.arraycopy(rxData, 9, data, 0, data.length);
        return new ByteArrayFragment(id, sequenceId, lastElement, data);
    }

    /**
     * Resolve a {@link ByteArrayFragment} from the received byte[] with the header encoded in UTF8
     * characters. The byte[] will be one of those returned from {@link #toTxBytesUTF8Header()}
     * 
     * @param rxData
     *            the byte[] for a ByteArrayFragment
     * @return a ByteArrayFragment
     */
    static ByteArrayFragment fromRxBytesUTF8Header(byte[] rxData)
    {
        // UTF8 has 1 byte per char
        int length = Integer.parseInt(new String(rxData, 0, 4));
        final int lengthPlus4 = length + 4;
        // we don't want the first "|"
        String header = new String(rxData, 5, length);
        int[] parts = ByteArrayFragmentUtils.split3NumbersByPipe(header);
        final int id = parts[0];
        final int sequenceId = parts[1];
        final byte lastElement = (byte) parts[2];
        final byte[] data = new byte[rxData.length - (lengthPlus4)];
        System.arraycopy(rxData, lengthPlus4, data, 0, data.length);
        return new ByteArrayFragment(id, sequenceId, lastElement, data);
    }

    /**
     * Convenience method to split a byte[] into the transmission bytes representing the byte array
     * fragments for the whole message. The header encoding is UTF8.
     * 
     * @param toSend
     *            the data to send
     * @param maxFragmentInternalByteSize
     *            the maximum size of each {@link ByteArrayFragment} instance's internal byte[]
     * @return the list of byte[] objects to send
     */
    static List<byte[]> getByteFragmentsToSendUTF8Header(byte[] toSend, int maxFragmentInternalByteSize)
    {
        ByteArrayFragment[] fragments = ByteArrayFragment.getFragmentsForTxData(toSend, maxFragmentInternalByteSize);
        List<byte[]> fragmentsToSend = new ArrayList<byte[]>(fragments.length);
        for (int i = 0; i < fragments.length; i++)
        {
            ByteArrayFragment byteArrayFragment = fragments[i];
            fragmentsToSend.add(byteArrayFragment.toTxBytesUTF8Header());
        }
        return fragmentsToSend;
    }

    final int id;
    final byte lastElement;
    int sequenceId;
    byte[] data;

    private ByteArrayFragment(int id, int sequenceId, byte lastElement, byte[] data)
    {
        super();
        this.id = id;
        this.sequenceId = sequenceId;
        this.lastElement = lastElement;
        this.data = data;
    }

    /**
     * Get the wire-frame for the byte array fragment, encoding the header in the raw byte
     * representation of the integers. The frame specification in ABNF is:
     * 
     * <pre>
     * frame = header data
     * 
     * header = id sequence-id last-element-flag 
     * data = 1*OCTET
     * 
     * id = 4OCTET
     * sequence-id = 4OCTET
     * last-element-flag = OCTET; 1=true 0=false
     * 
     * </pre>
     * 
     * @see #fromRxBytesRawByteHeader(byte[])
     * @return the byte[] to send that represents this fragment
     */
    byte[] toTxBytesRawByteHeader()
    {
        byte[] txBytes = new byte[9 + this.data.length];
        System.arraycopy(this.data, 0, txBytes, 9, this.data.length);
        // write the header
        txBytes[0] = (byte) (this.id >> 24);
        txBytes[1] = (byte) (this.id >> 16);
        txBytes[2] = (byte) (this.id >> 8);
        txBytes[3] = (byte) (this.id);
        txBytes[4] = (byte) (this.sequenceId >> 24);
        txBytes[5] = (byte) (this.sequenceId >> 16);
        txBytes[6] = (byte) (this.sequenceId >> 8);
        txBytes[7] = (byte) (this.sequenceId);
        txBytes[8] = (this.lastElement);
        return txBytes;
    }

    /**
     * Get the wire-frame for the byte array fragment, encoding the header in UTF8 characters. The
     * frame specification in ABNF is:
     * 
     * <pre>
     * frame = header data
     * 
     * header = len "|" id "|" sequence-id "|" last-element-flag "|"  
     * data = 1*OCTET
     * 
     * len = 3DIGIT ; the length of the header, padded with 0
     * id = 1*DIGIT
     * sequence-id = 1*DIGIT
     * last-element-flag = ALPHA ; 1=true 0=false
     * 
     * </pre>
     * 
     * @see #fromRxBytesUTF8Header(byte[])
     * @return the byte[] to send that represents this fragment
     */
    byte[] toTxBytesUTF8Header()
    {
        final StringBuilder sb = new StringBuilder(32);
        sb.append('|').append(this.id).append('|').append(this.sequenceId).append('|').append(this.lastElement).append(
            '|');
        final byte[] header = sb.toString().getBytes(UTF8);
        final byte[] len = ByteArrayFragmentUtils.pad4DigitWithLeadingZeros(header.length).getBytes();
        final int headerSize = len.length + header.length;
        final byte[] txBytes = new byte[headerSize + this.data.length];
        System.arraycopy(len, 0, txBytes, 0, len.length);
        System.arraycopy(header, 0, txBytes, len.length, header.length);
        System.arraycopy(this.data, 0, txBytes, headerSize, this.data.length);
        return txBytes;
    }

    /**
     * @return <code>true</code> if this fragment is the last one for the data
     */
    boolean isLastElement()
    {
        return this.lastElement != 0;
    }

    /**
     * Merge this fragment with the other one - effectively append the other's data to this one
     * 
     * @param other
     *            the fragment to merge into this one
     * @return this fragment instance (NOT the other)
     */
    ByteArrayFragment merge(ByteArrayFragment other) throws IncorrectSequenceException
    {
        if (++this.sequenceId != other.sequenceId)
        {
            throw new IncorrectSequenceException("Expected " + (this.sequenceId) + " but got " + other.sequenceId);
        }
        byte[] d = new byte[this.data.length + other.data.length];
        System.arraycopy(this.data, 0, d, 0, this.data.length);
        System.arraycopy(other.data, 0, d, this.data.length, other.data.length);
        this.data = d;
        return this;
    }

    /**
     * @return the data resolved from all the fragments
     */
    byte[] getData()
    {
        return this.data;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.id ^ (this.id >>> 32));
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
        ByteArrayFragment other = (ByteArrayFragment) obj;
        return is.eq(this.id, other.id);
    }

    @Override
    public String toString()
    {
        return "ByteArrayFragment [id=" + this.id + ", sequenceId=" + this.sequenceId + ", lastElement="
            + this.lastElement + ", data=" + this.data.length + "]";
    }
}
