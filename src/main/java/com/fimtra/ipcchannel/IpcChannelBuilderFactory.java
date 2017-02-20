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

import com.fimtra.channel.IEndPointAddressFactory;
import com.fimtra.channel.ITransportChannelBuilder;
import com.fimtra.channel.ITransportChannelBuilderFactory;

/**
 * A factory for obtaining {@link IpcChannelBuilder} instances
 * 
 * @author Ramon Servadei
 */
public class IpcChannelBuilderFactory implements ITransportChannelBuilderFactory
{
    final IEndPointAddressFactory endPoints;

    public IpcChannelBuilderFactory(IEndPointAddressFactory endPoints)
    {
        this.endPoints = endPoints;
    }

    @Override
    public ITransportChannelBuilder nextBuilder()
    {
        return new IpcChannelBuilder(this.endPoints.next());
    }

    @Override
    public String toString()
    {
        return "IpcChannelBuilderFactory [endPoints=" + this.endPoints + "]";
    }
}
