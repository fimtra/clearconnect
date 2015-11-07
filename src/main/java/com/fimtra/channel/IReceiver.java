/*
 * Copyright (c) 2013 Ramon Servadei 
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
 * The object that receives communication data from the remote end of a channel
 * 
 * @author Ramon Servadei
 */
public interface IReceiver
{
    /**
     * Called when a channel is connected.ITransportChannel
     * 
     * @param channel
     *            the channel that has connected
     */
    void onChannelConnected(ITransportChannel channel);

    /**
     * Called when data is received from the remote end of a channel
     * 
     * @param data
     *            the data received
     * @param source
     *            the channel that sent the data. This can be used to send response data.
     */
    void onDataReceived(byte[] data, ITransportChannel source);

    /**
     * Called when a channel is closed
     * 
     * @param channel
     *            the channel that has closed
     */
    void onChannelClosed(ITransportChannel channel);
}
