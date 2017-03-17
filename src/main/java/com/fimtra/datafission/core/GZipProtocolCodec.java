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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.GZipUtils;

/**
 * Uses GZIP compression to deflate and inflate the byte[] that is sent/received by a
 * {@link StringProtocolCodec}
 * 
 * @author Ramon Servadei
 */
public class GZipProtocolCodec extends StringProtocolCodec
{
    final static Charset ISO_8859_1 = Charset.forName("ISO-8859-1");
    
    @Override
    public byte[] getTxMessageForAtomicChange(IRecordChange atomicChange)
    {
        return GZipUtils.compress(super.getTxMessageForAtomicChange(atomicChange));
    }

    @Override
    public byte[] getTxMessageForSubscribe(String... names)
    {
        return GZipUtils.compress(super.getTxMessageForSubscribe(names));
    }

    @Override
    public byte[] getTxMessageForUnsubscribe(String... names)
    {
        return GZipUtils.compress(super.getTxMessageForUnsubscribe(names));
    }

    @Override
    public byte[] getTxMessageForIdentify(String proxyIdentity)
    {
        return GZipUtils.compress(super.getTxMessageForIdentify(proxyIdentity));
    }

    @Override
    public byte[] getTxMessageForRpc(String rpcName, IValue[] args, String resultRecordName)
    {
        return GZipUtils.compress(super.getTxMessageForRpc(rpcName, args, resultRecordName));
    }

    @Override
    public char[] decode(byte[] data)
    {
        return ISO_8859_1.decode(ByteBuffer.wrap(GZipUtils.uncompress(this.sessionSyncProtocol.decode(data)))).array();
    }

    @Override
    public FrameEncodingFormatEnum getFrameEncodingFormat()
    {
        return FrameEncodingFormatEnum.LENGTH_BASED;
    }

    @Override
    public ICodec<char[]> newInstance()
    {
        return new GZipProtocolCodec();
    }

    @Override
    public byte[] getTxMessageForResync(String... names)
    {
        return GZipUtils.compress(super.getTxMessageForResync(names));
    }

    @Override
    public Charset getCharset()
    {
        return ISO_8859_1;
    }
}
