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

import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
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

    /**
     * @return the {@link TransportTechnologyEnum} defined by the system property
     *         {@link #SYSTEM_PROPERTY}, defaulting to {@link #TCP} if not defined.
     */
    public static TransportTechnologyEnum getDefaultFromSystemProperty()
    {
        Object tte = System.getProperties().get(TransportTechnologyEnum.SYSTEM_PROPERTY);
        if (tte != null)
        {
            return valueOf(tte.toString());
        }
        else
        {
            return TCP;
        }
    }

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
     * <li>For {@link TransportTechnologyEnum#TCP} (<code>-Dtransport=TCP</code>) this will use an
     * ephemeral port (0).
     * <li>For {@link TransportTechnologyEnum#SOLACE} (<code>-Dtransport=SOLACE</code>) this method
     * will simply return a unique integer.
     * </ul>
     * 
     * @return the service port to use for the transport technology.
     */
    public synchronized int getNextAvailableServicePort()
    {
        switch(this)
        {
            case SOLACE:
                // number of millis since ~15:21 on 15-Nov-2015 GMT
                return System.identityHashCode(SOLACE) + (int) (System.currentTimeMillis() - 1447600859369l);
            case TCP:
                // use an ephemeral port
                return 0;
            default :
                throw new IllegalArgumentException("Unsupported transport technology: " + this);
        }
    }
}
