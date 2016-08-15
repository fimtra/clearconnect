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

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.ISessionProtocol.SyncResponse;

/**
 * Tests for the {@link CipherProtocolCodec}
 * 
 * @author Ramon Servadei
 */
public class CipherProtocolCodecTest extends StringProtocolCodecTest
{
    final CipherProtocolCodec codec;
    
    public CipherProtocolCodecTest()
    {
        super();
        // to speed up tests, re-use the same instance
        codec = new CipherProtocolCodec();
        try
        {
            // todo bit of a cheat here - we use the codec to synchronise with itself
            final byte[] txMessageForCodecSync =
                codec.getSessionProtocol().getSessionSyncStartMessage("CipherProtocolCodecTest");
            SyncResponse response = codec.getSessionProtocol().handleSessionSyncData(txMessageForCodecSync);
            response = codec.getSessionProtocol().handleSessionSyncData(response.syncDataResponse);
            response = codec.getSessionProtocol().handleSessionSyncData(response.syncDataResponse);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Override
    ICodec<?> constructCandidate()
    {
        return codec;
    }
}
