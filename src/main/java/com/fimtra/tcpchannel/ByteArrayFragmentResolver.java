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
            default:
                throw new IllegalArgumentException(
                        "No byte array fragment resolver available for frame encoding "
                                + frameEncodingFormat);
        }
    }

    private ByteArrayFragmentResolver()
    {
        this.fragments = new HashMap<>();
    }

    @SuppressWarnings("synthetic-access")
    static final class RawByteHeaderByteArrayFragmentResolver extends ByteArrayFragmentResolver
    {
        @Override
        ByteBuffer resolve(ByteBuffer byteFragmentTxData)
        {
            return resolveInternal(ByteArrayFragment.fromRxBytesRawByteHeader(byteFragmentTxData));
        }

        @Override
        ByteBuffer[] prepareBuffersToSend(TxByteArrayFragment byteArrayFragment)
        {
            return byteArrayFragment.toTxBytesRawByteHeader();
        }
    }

    @SuppressWarnings("synthetic-access")
    static final class UTF8HeaderByteArrayFragmentResolver extends ByteArrayFragmentResolver
    {
        @Override
        ByteBuffer resolve(ByteBuffer byteFragmentTxData)
        {
            return resolveInternal(ByteArrayFragment.fromRxBytesUTF8Header(byteFragmentTxData));
        }

        @Override
        ByteBuffer[] prepareBuffersToSend(TxByteArrayFragment byteArrayFragment)
        {
            return byteArrayFragment.toTxBytesUTF8Header();
        }
    }

    /**
     * Resolve the {@link ByteBuffer} of the byteFragmentTxData into a data byte[]. If the original {@link
     * ByteBuffer} was split into multiple fragments then only when all the fragments have been received (IN
     * ORDER) will this method return the resolved data byte[].
     *
     * @param byteFragmentTxData a {@link ByteArrayFragment} in {@link ByteBuffer} form
     * @return the fully resolved data in a ByteBuffer or <code>null</code> if fragments are still missing
     */
    abstract ByteBuffer resolve(ByteBuffer byteFragmentTxData);

    /**
     * Get the header and data {@link ByteBuffer} to send from the fragment
     *
     * @param byteArrayFragment
     * @return an array holding the header and data as {@link ByteBuffer} objects
     */
    abstract ByteBuffer[] prepareBuffersToSend(TxByteArrayFragment byteArrayFragment);

    ByteBuffer resolveInternal(final ByteArrayFragment fragment)
    {
        ByteBuffer resolvedData = null;
        try
        {
            if (fragment.isLastElement())
            {
                if (fragment.sequenceId == 0)
                {
                    // the fragment holds the full data (it was never fragmented)
                    resolvedData = fragment.getData();
                }
                else
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
                        current.free();
                    }
                }
                fragment.free();
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
                    fragment.free();
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
