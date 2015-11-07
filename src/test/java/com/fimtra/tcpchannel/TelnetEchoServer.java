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

import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpServer;

/**
 * Runs a server that echos text input from a telnet session.
 * 
 * @author Ramon Servadei
 */
public class TelnetEchoServer
{
    public static void main(String[] args) throws Exception
    {
        @SuppressWarnings("unused")
        TcpServer telnetServer = new TcpServer(null, 20000, new IReceiver()
        {

            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                source.sendAsync(("\r\n(" + new String(data) + ")").getBytes());
            }

            @Override
            public void onChannelClosed(ITransportChannel tcpChannel)
            {
            }

            @Override
            public void onChannelConnected(ITransportChannel tcpChannel)
            {
            }
        });
        System.out.println("Ready...press a key to finish");
        System.in.read();
    }
}
