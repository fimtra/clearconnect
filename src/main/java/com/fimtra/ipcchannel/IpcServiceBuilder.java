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

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IEndPointServiceBuilder;
import com.fimtra.channel.IReceiver;

/**
 * Builder for an {@link IpcService}
 * 
 * @author Ramon Servadei
 */
public class IpcServiceBuilder implements IEndPointServiceBuilder
{
    final EndPointAddress endPointAddress;

    /**
     * Construct the builder for the service.
     * 
     * @param endPointAddress
     *            The end-point address expresses a directory as the node and the port will become a
     *            sub-directory of the node directory. The port sub-directory is where the IO files
     *            will exist for the {@link IpcChannel} instances that are created when connecting
     *            to this service.
     */
    public IpcServiceBuilder(EndPointAddress endPointAddress)
    {
        this.endPointAddress = endPointAddress;
    }

    @Override
    public IEndPointService buildService(IReceiver receiver)
    {
        return new IpcService(this.endPointAddress, receiver);
    }
}
