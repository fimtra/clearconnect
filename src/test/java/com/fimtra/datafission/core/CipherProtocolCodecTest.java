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

import java.security.Key;

import com.fimtra.datafission.ICodec;
import com.fimtra.util.Pair;
import com.fimtra.util.SerializationUtils;

/**
 * Tests for the {@link CipherProtocolCodec}
 * 
 * @author Ramon Servadei
 */
public class CipherProtocolCodecTest extends StringProtocolCodecTest
{
    @Override
    ICodec<?> constructCandidate()
    {
        final CipherProtocolCodec codec = new CipherProtocolCodec();
        // todo bit of a cheat here - we use the codec's own public key as the other ends key
        try
        {
            codec.handshakeCipher.setEncryptionKey(codec.handshakeCipher.getPubKey());
            
            Pair<Key, byte[]> data = new Pair<Key, byte[]>(codec.handshakeCipher.getPubKey(), 
                    codec.handshakeCipher.encrypt(SerializationUtils.toByteArray(codec.txKey)));
                    
            codec.handleCodecSyncData(SerializationUtils.toByteArray(data));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return codec;
    }
}
