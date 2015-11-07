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

/**
 * Encapsulates building a specific type of transport channel
 * 
 * @author Ramon Servadei
 */
public interface ITransportChannelBuilder
{
    /**
     * Build an instance of the {@link ITransportChannel} this builder works for
     * 
     * @throws IOException
     *             if the channel could not be built
     */
    ITransportChannel buildChannel(IReceiver receiver) throws IOException;

    /**
     * @return the {@link EndPointAddress} used by this builder for constructing channels in the
     *         {@link #buildChannel(IReceiver)} method
     */
    EndPointAddress getEndPointAddress();
}
