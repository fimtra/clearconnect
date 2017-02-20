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
import java.util.Random;

import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;

/**
 * @author Ramon Servadei
 */
public class WriterTest
{
    public static void main(String[] args) throws Exception
    {
        IpcChannel writer = new IpcChannel("out", "in", new IReceiver()
        {

            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {

            }

            @Override
            public void onChannelConnected(ITransportChannel channel)
            {
                System.err.println("CONNECTED: " + channel);
            }

            @Override
            public void onChannelClosed(ITransportChannel channel)
            {
                System.err.println("DISCONNECTED: " + channel);
            }
        });
        int i = 0;
        int j = 0;
        Random rnd = new Random();
        int loopCount = 0;
        while (loopCount++ < 1000)
        {
            for (i = 0; i < 1000; i++)
            {
                final byte[] bytes = (new Date().toString()
                    // +
                    // "-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello-hello"

                    + " " + j++).getBytes();
                // final byte[] bytes = ("hello-" + new Date()).getBytes();
                synchronized (writer.writeData)
                {
                    if (i++ % 1000 == 0)
                    {
                        if (writer.writeData.size() > 1000)
                        {
                            System.err.println(writer.writeData.size());
                        }
                    }
                    writer.sendAsync(bytes);
                }
            }
            Thread.sleep(1);
        }
        System.err.println("DONE");
        System.in.read();
    }
}
