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
package com.fimtra.tcpchannel;

import com.fimtra.channel.IEndPointAddressFactory;
import com.fimtra.channel.ITransportChannelBuilder;
import com.fimtra.channel.ITransportChannelBuilderFactory;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;

/**
 * A factory for creating TCP channel builders.
 * 
 * @author Ramon Servadei
 */
public final class TcpChannelBuilderFactory implements ITransportChannelBuilderFactory
{
    final IEndPointAddressFactory endPoints;
    final FrameEncodingFormatEnum frameEncodingFormat;

    public TcpChannelBuilderFactory(FrameEncodingFormatEnum frameEncodingFormat, IEndPointAddressFactory endPoints)
    {
        this.endPoints = endPoints;
        this.frameEncodingFormat = frameEncodingFormat;
    }

    @Override
    public ITransportChannelBuilder nextBuilder()
    {
        return new TcpChannelBuilder(this.frameEncodingFormat, this.endPoints.next());
    }

    @Override
    public String toString()
    {
        return "TcpChannelBuilderFactory [endPoints=" + this.endPoints + ", frameEncodingFormat="
            + this.frameEncodingFormat + "]";
    }
}
