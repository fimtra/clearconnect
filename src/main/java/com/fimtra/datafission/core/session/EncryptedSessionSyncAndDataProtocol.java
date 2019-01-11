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
package com.fimtra.datafission.core.session;

import java.nio.ByteBuffer;

import javax.crypto.SecretKey;

import com.fimtra.datafission.DataFissionProperties.Values;
import com.fimtra.util.SerializationUtils;
import com.fimtra.util.SymmetricCipher;

/**
 * Synchronises a session using encrypted messages and encrypts all data transfer for a session.
 * 
 * @see {@link EncryptedSessionSyncProtocol}
 * @author Ramon Servadei
 */
public final class EncryptedSessionSyncAndDataProtocol extends EncryptedSessionSyncProtocol
{
    @Deprecated
    private static final String SYMMETRIC_TRANSFORMATION = SymmetricCipher.ALGORITHM_AES;
    static final String TRANSFORMATION = Values.ENCRYPTED_SESSION_TRANSFORMATION;

    final SecretKey txKey;

    SymmetricCipher txCipher;
    SymmetricCipher rxCipher;

    public EncryptedSessionSyncAndDataProtocol()
    {
        super();
        try
        {
            this.txKey = SymmetricCipher.generate128BitKey(SymmetricCipher.ALGORITHM_AES);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }  
    
    @Override
    void handleExtra(FromPublisher fromPublisher)
    {
        try
        {
            if (fromPublisher.dataTransformation != null)
            {
                this.txCipher = new SymmetricCipher(TRANSFORMATION, this.txKey);
                this.rxCipher = new SymmetricCipher(fromPublisher.dataTransformation,
                    SerializationUtils.<SecretKey>fromByteArray(this.handshakeCipher.decrypt(fromPublisher.extra)));
            }
            else
            {
                // pre 3.15.5 support
                this.txCipher = new SymmetricCipher(SYMMETRIC_TRANSFORMATION, this.txKey);
                this.rxCipher = new SymmetricCipher(SYMMETRIC_TRANSFORMATION,
                    SerializationUtils.<SecretKey>fromByteArray(this.handshakeCipher.decrypt(fromPublisher.extra)));
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    void handleExtra(FromPublisher response, FromProxy fromProxy)
    {
        try
        {
            if (fromProxy.dataTransformation != null)
            {
                this.txCipher = new SymmetricCipher(TRANSFORMATION, this.txKey);
                this.rxCipher = new SymmetricCipher(fromProxy.dataTransformation,
                    SerializationUtils.<SecretKey>fromByteArray(this.handshakeCipher.decrypt(fromProxy.extra)));
            }
            else
            {
                // pre 3.15.5 support
                this.txCipher = new SymmetricCipher(SYMMETRIC_TRANSFORMATION, this.txKey);
                this.rxCipher = new SymmetricCipher(SYMMETRIC_TRANSFORMATION,
                    SerializationUtils.<SecretKey>fromByteArray(this.handshakeCipher.decrypt(fromProxy.extra)));
            }

            response.extra = this.handshakeCipher.encrypt(SerializationUtils.toByteArray(this.txKey));
            response.dataTransformation = TRANSFORMATION;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    void prepareExtra(FromProxy response)
    {
        try
        {
            response.extra = this.handshakeCipher.encrypt(SerializationUtils.toByteArray(this.txKey));
            response.dataTransformation = TRANSFORMATION;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] encode(byte[] toSend)
    {
        return this.txCipher.encrypt(toSend);
    }

    @Override
    public ByteBuffer decode(ByteBuffer received)
    {
        byte[] array = received.array();
        // we need an exact sized array for decrypting
        if (!(received.position() == 0 && received.limit() == array.length))
        {
            array = new byte[received.limit() - received.position()];
            System.arraycopy(received.array(), received.position(), array, 0, array.length);
        }
        return ByteBuffer.wrap(this.rxCipher.decrypt(array));
    }
}
