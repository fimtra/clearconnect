/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
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
package com.fimtra.clearconnect.benchmark;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Tests what the maximum possible network throughput
 * 
 * @author Ramon Servadei
 */
public class NetworkTest
{
    public static void main(String[] args) throws Exception
    {
        byte[] data = new byte[200];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = (byte) i;
        }

        final ServerSocket soc = new ServerSocket();
        InetSocketAddress endpoint = new InetSocketAddress("127.0.0.1", 22230);
        soc.bind(endpoint);

        Thread thread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Socket socket = soc.accept();
                    socket.setTcpNoDelay(true);
                    InputStream inputStream = socket.getInputStream();
                    OutputStream outputStream = socket.getOutputStream();
                    byte[] rxData = new byte[200];
                    int read = 0;
                    int size = 0;
                    while ((read = inputStream.read(rxData)) != -1)
                    {
                        size += read;
                        if (size != 200)
                        {
                            continue;
                        }
                        size = 0;
                        outputStream.write(rxData);
                        outputStream.flush();
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }, "Server-socket");
        thread.setDaemon(true);
        thread.start();

        Socket client = new Socket(endpoint.getAddress(), endpoint.getPort());
        client.setTcpNoDelay(true);
        InputStream inputStream = client.getInputStream();
        OutputStream outputStream = client.getOutputStream();
        long start = System.currentTimeMillis();
        long counter = 0;
        byte[] rxData = new byte[200];
        int read = 0;
        int size = 0;
        while ((System.currentTimeMillis() - start < 60000))
        {
            counter++;
            counter++;
            if(counter % 100000 == 0)
            {
                System.err.println("Count=" + counter);
            }
            outputStream.write(data);
            outputStream.flush();
            while ((read = inputStream.read(rxData)) != -1)
            {
                size += read;
                if (size != 200)
                {
                    continue;
                }
                size = 0;
                break;
            }
        }
        System.err.println("Network msgs/sec:" + counter / 60);
    }
}
