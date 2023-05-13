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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.ISessionProtocol;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.ByteArrayPool;
import com.fimtra.util.GZipUtils;

/**
 * Uses GZIP compression to deflate and inflate the byte[] that is sent/received by a
 * {@link StringProtocolCodec}
 * 
 * @author Ramon Servadei
 */
public class GZipProtocolCodec extends StringProtocolCodec
{

    private static Function<ByteBuffer, byte[]> createEncodedBytesHandler()
    {
        return GZipUtils::compress;
    }

    public GZipProtocolCodec()
    {
        super(createEncodedBytesHandler());
    }

    protected GZipProtocolCodec(ISessionProtocol sessionSyncProtocol)
    {
        super(sessionSyncProtocol, createEncodedBytesHandler());
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
    public char[] decode(ByteBuffer data, CharsetDecoder charsetDecoder)
    {
        final ByteBuffer uncompressed = GZipUtils.uncompress(this.sessionSyncProtocol.decode(data));
        if (uncompressed == null)
        {
            throw new RuntimeException("Could not uncompress data");
        }
        final char[] decoded;
        try
        {
            decoded = charsetDecoder.decode(uncompressed).array();
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
        ByteArrayPool.offer(uncompressed.array());
        return decoded;
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
        return StandardCharsets.ISO_8859_1;
    }
}
