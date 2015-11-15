/*
 * Copyright (c) 2014 Ramon Servadei 
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
package com.fimtra.channel;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;

/**
 * An enum that encapsulates constructing the correct {@link ITransportChannelBuilderFactory} and
 * {@link IEndPointServiceBuilder} to support a specific transport technology.
 * 
 * @author Ramon Servadei
 */
public enum TransportTechnologyEnum
{
    // specify the classes used per transport technology
        TCP("com.fimtra.tcpchannel.TcpChannelBuilderFactory", "com.fimtra.tcpchannel.TcpServerBuilder"),

        SOLACE("com.fimtra.channel.solace.SolaceChannelBuilderFactory",
            "com.fimtra.channel.solace.SolaceServiceBuilder");

    public static final String SYSTEM_PROPERTY = "transport";

    final String endPointServiceBuilderClassName;
    final String transportChannelBuilderFactoryLoaderClassName;

    TransportTechnologyEnum(String transportChannelBuilderFactoryLoaderClassName, String endPointServiceBuilderClassName)
    {
        this.transportChannelBuilderFactoryLoaderClassName = transportChannelBuilderFactoryLoaderClassName;
        this.endPointServiceBuilderClassName = endPointServiceBuilderClassName;
    }

    public ITransportChannelBuilderFactory constructTransportChannelBuilderFactory(
        FrameEncodingFormatEnum frameEncodingFormat, IEndPointAddressFactory endPoints)
    {
        try
        {
            final Class<?> builderClass = Class.forName(this.transportChannelBuilderFactoryLoaderClassName);
            switch(this)
            {
                case TCP:
                    return (ITransportChannelBuilderFactory) builderClass.getConstructor(FrameEncodingFormatEnum.class,
                        IEndPointAddressFactory.class).newInstance(frameEncodingFormat, endPoints);
                case SOLACE:
                    return (ITransportChannelBuilderFactory) builderClass.getConstructor(IEndPointAddressFactory.class).newInstance(
                        endPoints);
                default :
                    throw new IllegalArgumentException("Unsupported transport technology: " + this);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not construct TransportChannelBuilderFactory from class name "
                + ObjectUtils.safeToString(this.transportChannelBuilderFactoryLoaderClassName), e);
        }
    }

    public IEndPointServiceBuilder constructEndPointServiceBuilder(FrameEncodingFormatEnum frameEncodingFormat,
        EndPointAddress endPoint)
    {
        try
        {
            final Class<?> builderClass = Class.forName(this.endPointServiceBuilderClassName);
            switch(this)
            {
                case TCP:
                    return (IEndPointServiceBuilder) builderClass.getConstructor(FrameEncodingFormatEnum.class,
                        EndPointAddress.class).newInstance(frameEncodingFormat, endPoint);
                case SOLACE:
                    return (IEndPointServiceBuilder) builderClass.getConstructor(EndPointAddress.class).newInstance(
                        endPoint);
                default :
                    throw new IllegalArgumentException("Unsupported transport technology: " + this);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not construct EndPointServiceBuilder from class name "
                + ObjectUtils.safeToString(this.endPointServiceBuilderClassName), e);
        }
    }

    /**
     * For the transport technology, finds an available "service" port to use.
     * <ul>
     * <li>For {@link TransportTechnologyEnum#TCP} (<code>-Dtransport=TCP</code>) this will perform
     * proper TCP port scanning to find an available port.
     * <li>For {@link TransportTechnologyEnum#SOLACE} (<code>-Dtransport=SOLACE</code>) this method
     * will simply return a unique integer.
     * </ul>
     * 
     * @param hostName
     *            (TCP usage only) the hostname to use to find the next free default TCP server
     * @param startPortRangeInclusive
     *            (TCP usage only) the start port to use for the free server socket scan
     * @param endPortRangeExclusive
     *            (TCP usage only) the end port <b>exclusive</b> to use for the free server socket
     *            scan
     * @return the server port to use, -1 if no port is available
     */
    public synchronized int getNextAvailableServicePort(String hostName, int startPortRangeInclusive,
        int endPortRangeExclusive)
    {
        switch(this)
        {
            case SOLACE:
                // number of millis since ~15:21 on 15-Nov-2015 GMT
                return System.identityHashCode(SOLACE) + (int) (System.currentTimeMillis() - 1447600859369l);
            case TCP:
            {
                String hostAddress = TcpChannelUtils.LOCALHOST_IP;
                try
                {
                    hostAddress = InetAddress.getByName(hostName).getHostAddress();
                }
                catch (UnknownHostException e)
                {
                }
                for (int i = startPortRangeInclusive; i < endPortRangeExclusive; i++)
                {
                    try
                    {
                        Log.log(this, "Trying ", hostAddress, ":", Integer.toString(i));
                        final ServerSocket serverSocket = new ServerSocket();
                        serverSocket.bind(new InetSocketAddress(hostAddress, i));
                        serverSocket.close();
                        // now ensure the server socket is closed before saying we can use it
                        try
                        {
                            int j = 0;
                            while (j++ < 10)
                            {
                                new Socket(hostAddress, i).close();
                                Thread.sleep(100);
                            }
                        }
                        catch (Exception e)
                        {
                        }
                        Log.log(this, "Using ", hostAddress, ":", Integer.toString(i));
                        return i;
                    }
                    catch (IOException e)
                    {
                        Log.log(this, e.getMessage());
                    }
                }
                throw new RuntimeException("No free TCP port available betwen " + startPortRangeInclusive + " and "
                    + endPortRangeExclusive);
            }
            default :
                throw new IllegalArgumentException("Unsupported transport technology: " + this);
        }
    }
}
