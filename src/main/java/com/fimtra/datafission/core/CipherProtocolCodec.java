/*
 * Copyright (c) 2016 Ramon Servadei 
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
import com.fimtra.datafission.core.session.EncryptedSessionSyncAndDataProtocol;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.AsymmetricCipher;
import com.fimtra.util.SymmetricCipher;

/**
 * Extension of the {@link StringProtocolCodec} that uses an {@link AsymmetricCipher} and
 * {@link SymmetricCipher} to encrypt/decrypt data sent/received from another codec.
 * 
 * @author Ramon Servadei
 */
public final class CipherProtocolCodec extends StringProtocolCodec
{
    final static Charset ISO_8859_1 = Charset.forName("ISO-8859-1");

    public CipherProtocolCodec()
    {
        super(new EncryptedSessionSyncAndDataProtocol());
    }

    @Override
    public FrameEncodingFormatEnum getFrameEncodingFormat()
    {
        return FrameEncodingFormatEnum.LENGTH_BASED;
    }

    @Override
    public ICodec<char[]> newInstance()
    {
        return new CipherProtocolCodec();
    }

    @Override
    public Charset getCharset()
    {
        return ISO_8859_1;
    }
}
