/*
 * Copyright (c) 2018 Ramon Servadei
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

import com.fimtra.datafission.ISessionProtocol.SyncResponse;
import com.fimtra.datafission.core.CipherProtocolCodec;
import com.fimtra.datafission.core.CipherProtocolCodecTest;
import com.fimtra.datafission.core.session.EncryptedSessionSyncProtocol.FromPublisher;
import com.fimtra.util.SerializationUtils;

/**
 * Exists to provide an initialised {@link CipherProtocolCodec} which has internal package level
 * access issues that prevent being initialised from the {@link CipherProtocolCodecTest} which is in
 * the parent package.
 * 
 * @author Ramon Servadei
 */
public class CipherProtocolCodecTestHelper
{
    public static CipherProtocolCodec getCodec()
    {
        CipherProtocolCodec codec = new CipherProtocolCodec();
        try
        {
            final EncryptedSessionSyncAndDataProtocol protocol =
                (EncryptedSessionSyncAndDataProtocol) codec.getSessionProtocol();

            // initialise the internals
            protocol.getSessionSyncStartMessage("dummy start");

            // simulate first response from the publisher with the handshake public key
            FromPublisher fromPublisher = new FromPublisher();
            fromPublisher.key = protocol.handshakeCipher.getPubKey();
            SyncResponse syncResponse = new SyncResponse(SerializationUtils.toByteArray(fromPublisher));
            protocol.handleSessionSyncData(ByteBuffer.wrap(syncResponse.syncDataResponse));

            // simulate final response from publisher with the encoded key for the data transfer
            fromPublisher = new FromPublisher();
            fromPublisher.dataTransformation = EncryptedSessionSyncAndDataProtocol.TRANSFORMATION;
            fromPublisher.extra = protocol.handshakeCipher.encrypt(SerializationUtils.toByteArray(protocol.txKey));
            fromPublisher.session =
                protocol.handshakeCipher.encrypt(SerializationUtils.toByteArray("some session response!"));
            syncResponse = new SyncResponse(SerializationUtils.toByteArray(fromPublisher));
            protocol.handleSessionSyncData(ByteBuffer.wrap(syncResponse.syncDataResponse));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return codec;
    }

}
