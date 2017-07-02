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
import java.util.HashMap;
import java.util.Map;

import com.fimtra.tcpchannel.ByteArrayFragment.IncorrectSequenceException;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.Log;

/**
 * Manages resolving {@link ByteArrayFragment} instances into a resolved data byte[]
 * <p>
 * This is NOT thread-safe.
 * 
 * @author Ramon Servadei
 */
abstract class ByteArrayFragmentResolver
{
    final Map<ByteArrayFragment, ByteArrayFragment> fragments;

    /**
     * Construct a new instance to support the frame encoding format
     */
    static ByteArrayFragmentResolver newInstance(FrameEncodingFormatEnum frameEncodingFormat)
    {
        switch(frameEncodingFormat)
        {
            case LENGTH_BASED:
                return new RawByteHeaderByteArrayFragmentResolver();
            case TERMINATOR_BASED:
                return new UTF8HeaderByteArrayFragmentResolver();
            default :
                throw new IllegalArgumentException(
                    "No byte array fragment resolver available for frame encoding " + frameEncodingFormat);
        }
    }

    private ByteArrayFragmentResolver()
    {
        this.fragments = new HashMap<ByteArrayFragment, ByteArrayFragment>();
    }

    @SuppressWarnings("synthetic-access")
    static final class RawByteHeaderByteArrayFragmentResolver extends ByteArrayFragmentResolver
    {
        @Override
        byte[] resolve(ByteBuffer byteFragmentTxData)
        {
            return resolveInternal(ByteArrayFragment.fromRxBytesRawByteHeader(byteFragmentTxData));
        }

        @Override
        ByteBuffer[] getByteFragmentsToSend(byte[] toSend, int maxFragmentInternalByteSize)
        {
            final ByteArrayFragment[] fragments =
                ByteArrayFragment.getFragmentsForTxData(toSend, maxFragmentInternalByteSize);
            final ByteBuffer[] fragmentsToSend = new ByteBuffer[fragments.length * 2];
            ByteBuffer[] parts;
            int k = 0;
            for (int i = 0; i < fragments.length; i++)
            {
                parts = fragments[i].toTxBytesRawByteHeader();
                fragmentsToSend[k++] = parts[0];
                fragmentsToSend[k++] = parts[1];
            }
            return fragmentsToSend;
        }
    }

    @SuppressWarnings("synthetic-access")
    static final class UTF8HeaderByteArrayFragmentResolver extends ByteArrayFragmentResolver
    {
        @Override
        byte[] resolve(ByteBuffer byteFragmentTxData)
        {
            return resolveInternal(ByteArrayFragment.fromRxBytesUTF8Header(byteFragmentTxData));
        }

        @Override
        ByteBuffer[] getByteFragmentsToSend(byte[] toSend, int maxFragmentInternalByteSize)
        {
            final ByteArrayFragment[] fragments =
                ByteArrayFragment.getFragmentsForTxData(toSend, maxFragmentInternalByteSize);
            final ByteBuffer[] fragmentsToSend = new ByteBuffer[fragments.length * 2];
            ByteBuffer[] parts;
            int k = 0;
            for (int i = 0; i < fragments.length; i++)
            {
                parts = fragments[i].toTxBytesUTF8Header();
                fragmentsToSend[k++] = parts[0];
                fragmentsToSend[k++] = parts[1];
            }
            return fragmentsToSend;
        }
    }

    /**
     * Resolve the {@link ByteBuffer} of the byteFragmentTxData into a data byte[]. If the original
     * {@link ByteBuffer} was split into multiple fragments then only when all the fragments have
     * been received (IN ORDER) will this method return the resolved data byte[].
     * 
     * @param byteFragmentTxData
     *            a {@link ByteArrayFragment} in {@link ByteBuffer} form
     * @return the fully resolved data byte[] or <code>null</code> if fragments are still missing
     */
    abstract byte[] resolve(ByteBuffer byteFragmentTxData);

    /**
     * Convenience method to split a byte[] into the transmission {@link ByteBuffer} objects
     * representing the byte array fragments for the whole message.
     * 
     * @param toSend
     *            the data to send
     * @param maxFragmentInternalByteSize
     *            the maximum size of each {@link ByteArrayFragment} instance's internal byte[]
     * @return the array of {@link ByteBuffer} objects to send
     */
    abstract ByteBuffer[] getByteFragmentsToSend(byte[] toSend, int maxFragmentInternalByteSize);

    byte[] resolveInternal(final ByteArrayFragment fragment)
    {
        byte[] resolvedData = null;
        try
        {
            if (fragment.isLastElement())
            {
                final ByteArrayFragment current = this.fragments.remove(fragment);
                if (current == null)
                {
                    // the fragment holds the full data (it was never fragmented)
                    resolvedData = fragment.getData();
                }
                else
                {
                    resolvedData = current.merge(fragment).getData();
                }
            }
            else
            {
                final ByteArrayFragment current = this.fragments.get(fragment);
                if (current == null)
                {
                    // call this to make a copy of the data - important as the internal byte buffer
                    // can point to the TCP rx buffer, which will compact on future calls
                    fragment.getData();
                    this.fragments.put(fragment, fragment);
                }
                else
                {
                    current.merge(fragment);
                }
            }
            return resolvedData;
        }
        catch (IncorrectSequenceException e)
        {
            this.fragments.remove(fragment);
            Log.log(this, "Could not handle " + fragment, e);
            return null;
        }
    }
}
