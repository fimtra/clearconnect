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

import com.fimtra.datafission.core.GZipProtocolCodec;
import com.fimtra.datafission.core.StringProtocolCodec;

/**
 * Tests using the {@link GZipProtocolCodec}
 * 
 * @author Ramon Servadei
 */
public class GZipProtocolProxyContextTest extends ProxyContextTest
{
    // these port ranges should not clash with any other ProxyContextTests
    static int START_PORT = 30000;
    static int END_PORT = 30100;

    @Override
    int getNextFreePort()
    {
        return START_PORT++;
        // port scanning disabled to speed up tests
        // return TcpChannelUtils.getNextFreeTcpServerPort(null, START_PORT++, END_PORT++);
    }

    @Override
    protected StringProtocolCodec getProtocolCodec()
    {
        return new GZipProtocolCodec();
    }
}
