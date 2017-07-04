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
package com.fimtra.datafission.core;

import java.nio.charset.Charset;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecordChange;

/**
 * A codec for messages that are sent between a {@link Publisher} and {@link ProxyContext} using a
 * hybrid protocol of text and binary; text for action-type messages and a binary for atomic change
 * messages.
 * <p>
 * NOTE: the binary protocol uses an <b>unsigned</b> 16-bit data type for each field code and also
 * for the length of the binary data for the field value. This limits this codec to supporting
 * runtimes that have 2^16 - 1 distinct field names (65535) and also limits the maximum size of any
 * field's data to 65536 bytes. If this is too restrictive, the 32-bit {@link HybridProtocolCodec32}
 * can be used (this will have a bigger wire format though).
 * 
 * @see #getTxMessageForAtomicChange(IRecordChange)
 * @author Ramon Servadei
 * @deprecated this will not be supported anylonger and removed in version 4.0.0
 */
@Deprecated
public final class HybridProtocolCodec extends StringProtocolCodec
{
    public HybridProtocolCodec()
    {
    }

    @Override
    public byte[] getTxMessageForAtomicChange(IRecordChange atomicChange)
    {
        throw new UnsupportedOperationException("Not supported anymore");
    }

    @Override
    public IRecordChange getAtomicChangeFromRxMessage(byte[] data)
    {
        throw new UnsupportedOperationException("Not supported anymore");
    }

    @Override
    public ICodec<char[]> newInstance()
    {
        throw new UnsupportedOperationException("Not supported anymore");
    }

    @Override
    public Charset getCharset()
    {
        throw new UnsupportedOperationException("Not supported anymore");
    }
}
