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

/**
 * An end-point for a channel to connect to. This is synonymous with a server.
 * 
 * @author Ramon Servadei
 */
public interface IEndPointService
{
    /**
     * @return the end-point address
     */
    EndPointAddress getEndPointAddress();

    /**
     * Destroy the instance
     */
    void destroy();

    /**
     * Broadcast the message to the {@link ITransportChannel} clients.
     * 
     * @return the number of messages sent (some services may not have a true broadcast transport)
     */
    int broadcast(String messageContext, byte[] txMessage, ITransportChannel[] clients);

    /**
     * Inform the service that broadcasting for the message context has stopped. This allows the
     * service to perform any clean-up of resources per broadcast context.
     * 
     * @param messageContext
     *            the context for the broadcast that has stopped
     */
    void endBroadcast(String messageContext);
}
