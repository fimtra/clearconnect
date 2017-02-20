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
package com.fimtra.ipcchannel;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpChannel;

/**
 * @author Ramon Servadei
 */
public class SocketWriterTest
{
    public static void main(String[] args) throws Exception
    {
        TcpChannel channel = new TcpChannel(SocketReaderTest.ADDR, 12345, new IReceiver()
        {
            
            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                // TODO Auto-generated method stub
                
            }
            
            @Override
            public void onChannelConnected(ITransportChannel channel)
            {
                // TODO Auto-generated method stub
                
            }
            
            @Override
            public void onChannelClosed(ITransportChannel channel)
            {
                // TODO Auto-generated method stub
                
            }
        });
        while(true)
        {
            final byte[] bytes = ("hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-" + new Date()).getBytes();

//            final byte[] bytes = ("hello-" + new Date()).getBytes();
            channel.sendAsync(bytes);
        }
    }
}
