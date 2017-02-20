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

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.channel.ITransportChannelBuilder;

/**
 * Builds {@link IpcChannel} instances
 * 
 * @author Ramon Servadei
 */
public class IpcChannelBuilder implements ITransportChannelBuilder
{
    final EndPointAddress endPointAddress;

    public IpcChannelBuilder(EndPointAddress endPointAddress)
    {
        this.endPointAddress = endPointAddress;
    }

    @Override
    public ITransportChannel buildChannel(IReceiver receiver) throws IOException
    {

        final String portSubDir = this.endPointAddress.getNode() + "/" + this.endPointAddress.getPort() + "/";
        final String randomUUID = UUID.randomUUID().toString();
        
        // IPC file format:
        // client (outFile): UUID.port
        // server (inFile) : port.UUID
        final String outFileName =
            new File(portSubDir + randomUUID + "." + this.endPointAddress.getPort()).getAbsolutePath();
        final String inFileName =
            new File(portSubDir + this.endPointAddress.getPort() + "." + randomUUID).getAbsolutePath();
        return new IpcChannel(outFileName, inFileName, receiver);
    }

    @Override
    public EndPointAddress getEndPointAddress()
    {
        return this.endPointAddress;
    }

    @Override
    public String toString()
    {
        return "IpcChannelBuilder [" + this.endPointAddress + "]";
    }

}
