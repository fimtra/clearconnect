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
}
