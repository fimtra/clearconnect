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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import com.fimtra.util.IReusableObject;
import com.fimtra.util.IReusableObjectBuilder;
import com.fimtra.util.MultiThreadReusableObjectPool;

/**
 * For efficiency reasons, there is a TX version of the ByteArrayFragment.
 *
 * @author Ramon Servadei
 */
final class TxByteArrayFragment extends ByteArrayFragment
{
    final static AtomicInteger ID_COUNTER = new AtomicInteger();

    final static MultiThreadReusableObjectPool<TxByteArrayFragment> TX_FRAGMENTS_POOL =
        new MultiThreadReusableObjectPool<>("TxFragmentPool", new IReusableObjectBuilder<TxByteArrayFragment>()
        {
            @Override
            public TxByteArrayFragment newInstance()
            {
                return new TxByteArrayFragment(TX_FRAGMENTS_POOL, 0, 0, (byte) 0, null, 0, 0);
            }
        }, TxByteArrayFragment::reset, TcpChannelProperties.Values.TX_FRAGMENT_POOL_MAX_SIZE);

    /**
     * Break the byte[] into fragments.
     *
     * @param data
     *            the data
     * @param maxFragmentInternalByteSize
     *            the maximum size of each {@link ByteArrayFragment} instance's internal byte[]
     * @return the list of ByteArrayFragment instances that collectively represent the data
     */
    static TxByteArrayFragment[] getFragmentsForTxData(byte[] data, int maxFragmentInternalByteSize)
    {
        final int id = ID_COUNTER.incrementAndGet();
        final int fragmentCount = data.length < maxFragmentInternalByteSize ? 1
            : ((int) Math.ceil((data.length * (1 / (double) maxFragmentInternalByteSize))));

        final TxByteArrayFragment[] fragments = new TxByteArrayFragment[fragmentCount];

        int pointer = 0;
        int remainder = data.length;
        int length;
        for (int i = 0; i < fragmentCount; i++)
        {
            length = Math.min(remainder, maxFragmentInternalByteSize);
            fragments[i] = (TxByteArrayFragment) TX_FRAGMENTS_POOL.get().initialise(id, i, (byte) (i == (fragmentCount - 1) ? 1 : 0),
                data, pointer, length);
            pointer += length;
            remainder -= length;
        }
        return fragments;
    }

    private final ByteBuffer[] txDataWithHeader;
    private byte[] header;

    TxByteArrayFragment(MultiThreadReusableObjectPool poolRef, int id, int sequenceId, byte lastElement, byte[] data, int offset, int len)
    {
        super(poolRef);
        initialise(id, sequenceId, lastElement, data, offset, len);
        this.header = new byte[9];
        this.txDataWithHeader = new ByteBuffer[2];
        this.txDataWithHeader[0] = ByteBuffer.wrap(this.header, 0, this.header.length);
    }

    @Override
    public void reset()
    {
        // reset the data part only
        this.txDataWithHeader[1] = null;
        super.reset();
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
     * @see #fromRxBytesRawByteHeader(ByteBuffer)
     * @return the ByteBuffer[] to send that represents the header and data for this fragment
     */
    ByteBuffer[] toTxBytesRawByteHeader()
    {
        // read volatile first to force all vars in scope to read from main memory
        final byte[] data = this.v_data;
        final byte lastElement = this.v_lastElement;
        final byte[] header = this.header;
        final int id = this.id;
        final int offset = this.offset;
        final int length = this.length;
        final int sequenceId = this.sequenceId;
        final ByteBuffer[] txDataWithHeader = this.txDataWithHeader;

        // write the header
        header[0] = (byte) (id >> 24);
        header[1] = (byte) (id >> 16);
        header[2] = (byte) (id >> 8);
        header[3] = (byte) (id);
        header[4] = (byte) (sequenceId >> 24);
        header[5] = (byte) (sequenceId >> 16);
        header[6] = (byte) (sequenceId >> 8);
        header[7] = (byte) (sequenceId);
        header[8] = (lastElement);

        txDataWithHeader[0].position(0);
        txDataWithHeader[0].limit(9);
        txDataWithHeader[1] = ByteBuffer.wrap(data, offset, length);

        return txDataWithHeader;
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
     * @see #fromRxBytesUTF8Header(ByteBuffer)
     * @return the ByteBuffer[] to send that represents the header and data for this fragment
     */
    ByteBuffer[] toTxBytesUTF8Header()
    {
        // read volatile first to force all vars in scope to read from main memory
        final byte[] data = this.v_data;
        final byte lastElement = this.v_lastElement;
        byte[] header = this.header;
        final int id = this.id;
        final int offset = this.offset;
        final int length = this.length;
        final int sequenceId = this.sequenceId;
        final ByteBuffer[] txDataWithHeader = this.txDataWithHeader;

        final StringBuilder sb = new StringBuilder(32);
        sb.append('|').append(id).append('|').append(sequenceId).append('|').append(lastElement).append(
            '|');
        final byte[] idSeqLstElement = sb.toString().getBytes(StandardCharsets.UTF_8);
        final byte[] len = ByteArrayFragmentUtils.pad4DigitWithLeadingZeros(idSeqLstElement.length).getBytes(
                StandardCharsets.UTF_8);
        final int headerLen = len.length + idSeqLstElement.length;
        if (header.length < headerLen)
        {
            header = this.header = new byte[headerLen];
            // perform this to force a write to main memory for header
            this.v_lastElement = lastElement;
            txDataWithHeader[0] = ByteBuffer.wrap(header, 0, headerLen);
        }
        System.arraycopy(len, 0, header, 0, len.length);
        System.arraycopy(idSeqLstElement, 0, header, len.length, idSeqLstElement.length);

        txDataWithHeader[0].position(0);
        txDataWithHeader[0].limit(headerLen);
        txDataWithHeader[1] = ByteBuffer.wrap(data, offset, length);

        return txDataWithHeader;
    }

    ByteBuffer[] getTxDataWithHeader()
    {
        return this.txDataWithHeader;
    }
}
