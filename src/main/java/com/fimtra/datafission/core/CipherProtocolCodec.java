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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import com.fimtra.datafission.ICodec;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.AsymmetricCipher;
import com.fimtra.util.Pair;
import com.fimtra.util.SerializationUtils;
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

    private static final String SYMMETRIC_TRANSFORMATION = SymmetricCipher.ALGORITHM_AES;

    static AsymmetricCipher createAsymmetricCipher()
        throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException
    {
        // note: 2048 to accomodate sending 128bit symmetric key
        return new AsymmetricCipher();
    }

    static SymmetricCipher createSymmetricCipher(SecretKey key)
        throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException
    {
        return new SymmetricCipher(SYMMETRIC_TRANSFORMATION, key);
    }

    final AsymmetricCipher handshakeCipher;
    final SecretKey txKey;
    final SymmetricCipher txCipher;

    SymmetricCipher rxCipher;
    boolean encryptedSymmetricalKeySent;

    public CipherProtocolCodec()
    {
        super();
        try
        {
            this.handshakeCipher = createAsymmetricCipher();
            this.txKey = SymmetricCipher.generate128BitKey(SYMMETRIC_TRANSFORMATION);
            this.txCipher = createSymmetricCipher(this.txKey);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public char[] decode(byte[] data)
    {
        return ISO_8859_1.decode(ByteBuffer.wrap(this.rxCipher.decrypt(data))).array();
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
    public byte[] getTxMessageForCodecSync(String sessionContext)
    {
        try
        {
            // note: send unencrypted public handshake key
            return SerializationUtils.toByteArray(this.handshakeCipher.getPubKey());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] handleCodecSyncData(byte[] data)
    {
        try
        {
            final Serializable fromByteArray = SerializationUtils.fromByteArray(data);
            if (fromByteArray instanceof Key)
            {
                this.handshakeCipher.setEncryptionKey((Key) fromByteArray);

                this.encryptedSymmetricalKeySent = true;
                return SerializationUtils.toByteArray(new Pair<Key, byte[]>(this.handshakeCipher.getPubKey(),
                    this.handshakeCipher.encrypt(SerializationUtils.toByteArray(this.txKey))));
            }
            else
            {
                // todo pass in sec context
                if (fromByteArray instanceof Pair)
                {
                    final Pair<Key, byte[]> pair = (Pair<Key, byte[]>) fromByteArray;
                    
                    this.handshakeCipher.setEncryptionKey(pair.getFirst());
                    this.rxCipher = new SymmetricCipher(SYMMETRIC_TRANSFORMATION,
                        SerializationUtils.<SecretKey>fromByteArray(this.handshakeCipher.decrypt((pair.getSecond()))));

                    if (!this.encryptedSymmetricalKeySent)
                    {
                        this.encryptedSymmetricalKeySent = true;
                        return SerializationUtils.toByteArray(new Pair<Key, byte[]>(this.handshakeCipher.getPubKey(),
                            this.handshakeCipher.encrypt(SerializationUtils.toByteArray(this.txKey))));
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            throw new IllegalStateException("Incorrect sync data: " + fromByteArray);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Charset getCharset()
    {
        return ISO_8859_1;
    }

    @Override
    public byte[] finalEncode(byte[] data)
    {
        return this.txCipher.encrypt(data);
    }
}
