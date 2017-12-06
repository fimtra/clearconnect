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

import javax.crypto.SecretKey;

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
    private static final String SYMMETRIC_TRANSFORMATION = SymmetricCipher.ALGORITHM_AES;

    final SecretKey txKey;
    final SymmetricCipher txCipher;

    SymmetricCipher rxCipher;

    public EncryptedSessionSyncAndDataProtocol()
    {
        super();
        try
        {
            this.txKey = SymmetricCipher.generate128BitKey(SYMMETRIC_TRANSFORMATION);
            this.txCipher = new SymmetricCipher(SYMMETRIC_TRANSFORMATION, this.txKey);
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
            this.rxCipher = new SymmetricCipher(SYMMETRIC_TRANSFORMATION,
                SerializationUtils.<SecretKey>fromByteArray(this.handshakeCipher.decrypt(fromPublisher.extra)));
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
            this.rxCipher = new SymmetricCipher(SYMMETRIC_TRANSFORMATION,
                SerializationUtils.<SecretKey>fromByteArray(this.handshakeCipher.decrypt(fromProxy.extra)));

            response.extra = this.handshakeCipher.encrypt(SerializationUtils.toByteArray(this.txKey));
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
    public byte[] decode(byte[] received)
    {
        return this.rxCipher.decrypt(received);
    }
}
