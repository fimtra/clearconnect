/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.benchmark;

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
