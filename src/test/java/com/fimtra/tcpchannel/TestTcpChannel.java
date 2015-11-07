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

import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.IReceiver;
import com.fimtra.tcpchannel.TcpChannel;

/**
 * Tests for the {@link TcpChannel}
 * 
 * @author Ramon Servadei
 */
public class TestTcpChannel
{

    @Before
    public void setUp() throws Exception
    {
        ChannelUtils.WATCHDOG.configure(10);
    }

    @After
    public void tearDown() throws Exception
    {
        ChannelUtils.WATCHDOG.configure(5000);
    }

    @SuppressWarnings("unused")
    @Test(expected = IOException.class)
    public void testAttemptConnectToNonExistentServer() throws IOException
    {
        IReceiver receiver = mock(IReceiver.class);
        new TcpChannel("localhost", 20000, receiver);
    }

}
