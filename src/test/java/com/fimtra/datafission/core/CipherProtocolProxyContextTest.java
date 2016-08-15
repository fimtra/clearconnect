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

/**
 * Tests using the {@link CipherProtocolCodec}
 * 
 * @author Ramon Servadei
 */
public class CipherProtocolProxyContextTest extends ProxyContextTest
{
    // to speed up tests, re-use the same instance
    private static final CipherProtocolCodec CIPHER_PROTOCOL_CODEC = new CipherProtocolCodec();
    // these port ranges should not clash with any other ProxyContextTests
    static int START_PORT = 34000;
    static int END_PORT = 34100;

    @Override
    int getNextFreePort()
    {
        return START_PORT++;
    }

    @Override
    protected StringProtocolCodec getProtocolCodec()
    {
        return CIPHER_PROTOCOL_CODEC;
    }
}
