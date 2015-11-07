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

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IEndPointServiceBuilder;
import com.fimtra.channel.IReceiver;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;

/**
 * Builds {@link TcpServer} instances
 * 
 * @author Ramon Servadei
 */
public final class TcpServerBuilder implements IEndPointServiceBuilder
{
    final FrameEncodingFormatEnum frameEncoding;
    final EndPointAddress endPointAddress;

    public TcpServerBuilder(FrameEncodingFormatEnum frameEncoding, EndPointAddress endPointAddress)
    {
        super();
        this.frameEncoding = frameEncoding;
        this.endPointAddress = endPointAddress;
    }

    @Override
    public IEndPointService buildService(IReceiver receiver)
    {
        return new TcpServer(this.endPointAddress.getNode(), this.endPointAddress.getPort(), receiver, this.frameEncoding);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.endPointAddress == null) ? 0 : this.endPointAddress.hashCode());
        result = prime * result + ((this.frameEncoding == null) ? 0 : this.frameEncoding.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TcpServerBuilder other = (TcpServerBuilder) obj;
        if (this.endPointAddress == null)
        {
            if (other.endPointAddress != null)
                return false;
        }
        else if (!this.endPointAddress.equals(other.endPointAddress))
            return false;
        if (this.frameEncoding != other.frameEncoding)
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "TcpServerBuilder [frameEncoding=" + this.frameEncoding + ", endPointAddress=" + this.endPointAddress
            + "]";
    }

}
