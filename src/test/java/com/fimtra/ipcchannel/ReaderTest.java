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
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.util.ThreadUtils;

/**
 * @author Ramon Servadei
 */
public class ReaderTest
{
    public static void main(String[] args) throws Exception
    {

        IReceiver receiver = new IReceiver()
        {
            long inCount = 0;
            byte[] data;

            {
                ThreadUtils.newScheduledExecutorService("measurer", 1).scheduleAtFixedRate(new Runnable()
                {
                    long start;
                    long old;

                    @Override
                    public void run()
                    {
                        if (this.old == 0)
                        {
                            this.old = inCount;
                            this.start = System.currentTimeMillis();
                        }
                        System.err.println("rx=" + inCount + ", avg/s = "
                            + (inCount / ((System.currentTimeMillis() - this.start) / 1000)));
                    }
                }, 1, 1, TimeUnit.SECONDS);
            }

            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
//                System.err.println(data == null ? "null" : new String(data));
                if (Arrays.equals(data, this.data))
                {
                    System.err.println("NOT UPDATED! " + (data == null ? "null" : new String(data)));
                }
                this.data = data;
                inCount++;
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
        };
        IpcChannel reader = new IpcChannel("in", "out", receiver);

        System.in.read();
    }
}
