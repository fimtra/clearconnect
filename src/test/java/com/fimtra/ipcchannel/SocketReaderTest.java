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
import java.util.concurrent.TimeUnit;

import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.tcpchannel.TcpServer;
import com.fimtra.util.ThreadUtils;

/**
 * @author Ramon Servadei
 */
public class SocketReaderTest
{
    static final String ADDR = "127.0.0.1";
//    static final String ADDR = TcpChannelUtils.LOCALHOST_IP;

    public static void main(String[] args) throws Exception
    {
        TcpServer server = new TcpServer(ADDR, 12345, new IReceiver()
        {
            int rxCount = 0;
            {
                ThreadUtils.newScheduledExecutorService("measurer", 1).scheduleAtFixedRate(new Runnable()
                {
                    long start;
                    int old;

                    @Override
                    public void run()
                    {
                        if (old == 0)
                        {
                            old = rxCount;
                            start = System.currentTimeMillis();
                        }
                        System.err.println(
                            "rx=" + rxCount + ", avg/s = " + (rxCount / ((System.currentTimeMillis() - start) / 1000)));
                    }
                }, 1, 1, TimeUnit.SECONDS);
            }

            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                rxCount++;
            }

            @Override
            public void onChannelConnected(ITransportChannel channel)
            {
            }

            @Override
            public void onChannelClosed(ITransportChannel channel)
            {
                // TODO Auto-generated method stub

            }
        });       
        System.err.println("Server ready " + server);
        System.in.read();
    }
}
