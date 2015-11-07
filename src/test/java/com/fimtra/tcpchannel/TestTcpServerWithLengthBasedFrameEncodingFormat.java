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
package com.fimtra.tcpchannel;

import com.fimtra.tcpchannel.TcpServer;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;

/**
 * Tests the {@link TcpServer} using {@link FrameEncodingFormatEnum#LENGTH_BASED}
 * 
 * @author Ramon Servadei
 */
public class TestTcpServerWithLengthBasedFrameEncodingFormat extends TestTcpServer
{
    static
    {
        PORT = 14000;
    }

    public TestTcpServerWithLengthBasedFrameEncodingFormat()
    {
        super();
        this.frameEncodingFormat = FrameEncodingFormatEnum.LENGTH_BASED;
    }

}
