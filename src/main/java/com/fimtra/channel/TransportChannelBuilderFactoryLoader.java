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
import com.fimtra.tcpchannel.TcpChannelBuilderFactory;
import com.fimtra.util.ObjectUtils;

/**
 * This initialises the correct type of {@link ITransportChannelBuilderFactory} for the environment.
 * The default is a TCP variant.
 * 
 * @author Ramon Servadei
 */
public final class TransportChannelBuilderFactoryLoader
{
    // todo the frame encoding is for TCP only...
    public static ITransportChannelBuilderFactory load(FrameEncodingFormatEnum frameEncodingFormat,
        EndPointAddress... endPoints)
    {
        return load(frameEncodingFormat, new StaticEndPointAddressFactory(endPoints));
    }

    public static ITransportChannelBuilderFactory load(FrameEncodingFormatEnum frameEncodingFormat,
        IEndPointAddressFactory endPoints)
    {
        Object tte = System.getProperties().get(TransportTechnologyEnum.SYSTEM_PROPERTY);
        if (tte != null)
        {
            try
            {
                return TransportTechnologyEnum.valueOf(tte.toString()).constructTransportChannelBuilderFactory(
                    frameEncodingFormat, endPoints);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Could not construct TransportChannelBuilderFactory from transport enum "
                    + ObjectUtils.safeToString(tte), e);
            }
        }
        else
        {
            // default is TCP
            return new TcpChannelBuilderFactory(frameEncodingFormat, endPoints);
        }
    }
}
