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

import java.io.IOException;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannelBuilder;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;

/**
 * Builds {@link TcpChannel} objects
 * 
 * @author Ramon Servadei
 */
public final class TcpChannelBuilder implements ITransportChannelBuilder
{
    final EndPointAddress endPointAddress;
    final FrameEncodingFormatEnum frameEncodingFormat;

    public TcpChannelBuilder(FrameEncodingFormatEnum frameEncodingFormat, EndPointAddress endPointAddress)
    {
        this.endPointAddress = endPointAddress;
        this.frameEncodingFormat = frameEncodingFormat;
    }

    @Override
    public TcpChannel buildChannel(IReceiver receiver) throws IOException
    {
        return new TcpChannel(this.endPointAddress.getNode(), this.endPointAddress.getPort(), receiver, this.frameEncodingFormat);
    }
    
    @Override
    public EndPointAddress getEndPointAddress()
    {
        return this.endPointAddress;
    }

    @Override
    public String toString()
    {
        return "TcpChannelBuilder [" + this.endPointAddress + ", frameEncodingFormat="
            + this.frameEncodingFormat + "]";
    }
}
