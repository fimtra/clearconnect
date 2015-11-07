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
 * A transport channel provides the means to send data through a logical channel. This is an
 * abstraction of an underlying transport protocol messaging system.
 * 
 * @author Ramon Servadei
 */
public interface ITransportChannel
{
    /**
     * Sends the byte[] to the receiver end of this channel. The data is added to the transmission
     * queue to be processed asynchronously. This method does not block.
     * 
     * @param toSend
     *            the byte[] to send
     * @return <code>true</code> if the data was added to the transmission queue and will be sent,
     *         <code>false</code> if the data cannot be sent because the channel is closed
     */
    boolean sendAsync(byte[] toSend);

    /**
     * Closes this {@link ITransportChannel} and releases all underlying resources supporting the
     * communication. This is idempotent.
     */
    void destroy(String reason, Exception... e);

    /**
     * Guaranteed to never throw an exception
     * 
     * @return <code>true</code> if the channel is active and can be used to send and receive
     */
    boolean isConnected();

    /**
     * @return a string representing the connection details of the 'remote end' of this channel
     */
    String getEndPointDescription();

    /**
     * @return a short-hand string describing both connection points of this channel
     */
    String getDescription();

    /**
     * Used primarily for watchdog purposes. Calling this should reset any flags tracking whether
     * data has been received.
     * 
     * @see ChannelWatchdog
     * @return <code>true</code> if the channel has received data since the last call to this
     *         method.
     */
    boolean hasRxData();
}
