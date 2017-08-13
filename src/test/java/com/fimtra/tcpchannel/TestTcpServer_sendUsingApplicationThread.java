/*
 * Copyright (c) 2017 Ramon Servadei
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
package com.fimtra.tcpchannel;

/**
 * Tests for the {@link TcpServer} and sending using application thread
 * 
 * @author Ramon Servadei
 */
public class TestTcpServer_sendUsingApplicationThread extends TestTcpServer
{
    static
    {
        PORT = 15000;
    }

    @Override
    public void setUp() throws Exception
    {
        TcpChannelUtils.setWriteToSocketUsingApplicationThread(true);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception
    {
        TcpChannelUtils.setWriteToSocketUsingApplicationThread(TcpChannelProperties.Values.WRITE_TO_SOCKET_USING_APPLICATION_THREAD);
        super.tearDown();
    }
}
