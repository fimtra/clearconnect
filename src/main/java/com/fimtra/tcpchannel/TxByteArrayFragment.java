/*
 * Copyright (c) 2017 Ramon Servadei
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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import com.fimtra.util.LowGcLinkedList;

/**
 * For efficiency reasons, there is a TX version of the ByteArrayFragment.
 * 
 * @author Ramon Servadei
 */
final class TxByteArrayFragment extends ByteArrayFragment
{
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
    static TxByteArrayFragment[] getFragmentsForTxData(byte[] data, int maxFragmentInternalByteSize,
        TxByteArrayFragment[][][] fragmentsArrayPool, LowGcLinkedList<TxByteArrayFragment> fragmentsPool)
    {
        final int id = ID_COUNTER.incrementAndGet();
        final int fragmentCount = data.length < maxFragmentInternalByteSize ? 1
            : ((int) Math.ceil((data.length * (1 / (double) maxFragmentInternalByteSize))));

        // resize fragment array pool
        if (fragmentCount >= fragmentsArrayPool[0].length)
        {
            fragmentsArrayPool[0] = Arrays.copyOf(fragmentsArrayPool[0], fragmentCount + 1);
        }

        // get the re-usable fragment array from the pool for the required size (fragment count)
        TxByteArrayFragment[] fragments = fragmentsArrayPool[0][fragmentCount];
        if (fragments == null)
        {
            fragments = new TxByteArrayFragment[fragmentCount];
            fragmentsArrayPool[0][fragmentCount] = fragments;
        }

        int pointer = 0;
        int remainder = data.length;
        int length;
        TxByteArrayFragment fragment;
        for (int i = 0; i < fragmentCount; i++)
        {
            length = remainder > maxFragmentInternalByteSize ? maxFragmentInternalByteSize : remainder;
            fragment = fragmentsPool.poll();
            if (fragment == null)
            {
                fragment =
                    new TxByteArrayFragment(id, i, (byte) (i == (fragmentCount - 1) ? 1 : 0), data, pointer, length);
            }
            else
            {
                fragment.initialise(id, i, (byte) (i == (fragmentCount - 1) ? 1 : 0), data, pointer, length);
            }
            fragments[i] = fragment;
            pointer += length;
            remainder -= length;
        }
        return fragments;
    }

    final ByteBuffer[] txDataWithHeader;
    byte[] header;

    TxByteArrayFragment(int id, int sequenceId, byte lastElement, byte[] data, int offset, int len)
    {
        super(id, sequenceId, lastElement, data, offset, len);
        this.header = new byte[9];
        this.txDataWithHeader = new ByteBuffer[2];
        this.txDataWithHeader[0] = ByteBuffer.wrap(this.header, 0, this.header.length);
    }

    void reset()
    {
        this.id = this.sequenceId = this.offset = this.length = this.lastElement = -1;
        this.data = null;
        // reset the data part only
        this.txDataWithHeader[1] = null;
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
     * @return the ByteBuffer[] to send that represents the header and data for this fragment
     */
    ByteBuffer[] toTxBytesRawByteHeader()
    {
        // write the header
        this.header[0] = (byte) (this.id >> 24);
        this.header[1] = (byte) (this.id >> 16);
        this.header[2] = (byte) (this.id >> 8);
        this.header[3] = (byte) (this.id);
        this.header[4] = (byte) (this.sequenceId >> 24);
        this.header[5] = (byte) (this.sequenceId >> 16);
        this.header[6] = (byte) (this.sequenceId >> 8);
        this.header[7] = (byte) (this.sequenceId);
        this.header[8] = (this.lastElement);

        this.txDataWithHeader[0].position(0);
        this.txDataWithHeader[0].limit(9);
        this.txDataWithHeader[1] = ByteBuffer.wrap(this.data, this.offset, this.length);

        return this.txDataWithHeader;
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
     * @return the ByteBuffer[] to send that represents the header and data for this fragment
     */
    ByteBuffer[] toTxBytesUTF8Header()
    {
        final StringBuilder sb = new StringBuilder(32);
        sb.append('|').append(this.id).append('|').append(this.sequenceId).append('|').append(this.lastElement).append(
            '|');
        final byte[] idSeqLstElement = sb.toString().getBytes(UTF8);
        final byte[] len = ByteArrayFragmentUtils.pad4DigitWithLeadingZeros(idSeqLstElement.length).getBytes(UTF8);
        final int headerLen = len.length + idSeqLstElement.length;
        if (this.header.length < headerLen)
        {
            this.header = new byte[headerLen];
            this.txDataWithHeader[0] = ByteBuffer.wrap(this.header, 0, headerLen);
        }
        System.arraycopy(len, 0, this.header, 0, len.length);
        System.arraycopy(idSeqLstElement, 0, this.header, len.length, idSeqLstElement.length);

        this.txDataWithHeader[0].position(0);
        this.txDataWithHeader[0].limit(headerLen);
        this.txDataWithHeader[1] = ByteBuffer.wrap(this.data, this.offset, this.length);

        return this.txDataWithHeader;
    }
}