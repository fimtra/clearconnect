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
package com.fimtra.datafission;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.rmi.Remote;
import java.util.List;

import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;

/**
 * A codec provides methods to decode and encode messages between {@link IPublisherContext} and
 * {@link IObserverContext} objects.
 * <p>
 * A codec uses an {@link ISessionProtocol} to provide a session link with another codec instance.
 * 
 * @see ISessionProtocol
 * @param <T>
 *            the type of data object the codec works with
 * @author Ramon Servadei
 */
public interface ICodec<T>
{
    public enum CommandEnum
    {
            NOOP, SUBSCRIBE, UNSUBSCRIBE, RPC, IDENTIFY, RESYNC
    }

    /**
     * Interpret the command from the received data
     * 
     * @param decodedMessage
     *            the decoded data for the command
     * @throws IllegalArgumentException
     *             if the command cannot be interpreted
     */
    CommandEnum getCommand(T decodedMessage);

    /**
     * Get the identity argument from the identity command
     * 
     * @param decodedMessage
     *            the decoded data for the identity command
     * @return the identity for the connecting proxy context
     */
    String getIdentityArgumentFromDecodedMessage(T decodedMessage);

    /**
     * Get the names of the records to subscribe for in the command.
     * <p>
     * Note: the first item is the permission token
     * 
     * @param decodedMessage
     *            the decoded data for the subscribe command
     * @return the names of the records in the subscribe command, <b>the first item in this list is
     *         the permission token for the subscribe operation</b>
     * @see IObserverContext#addObserver(String, IRecordListener, String...)
     */
    List<String> getSubscribeArgumentsFromDecodedMessage(T decodedMessage);

    /**
     * Get the names of the records to unsubscribe for in the command
     * 
     * @param decodedMessage
     *            the decoded data for the subscribe command
     * @return the names of the records in the unsubscribe command
     */
    List<String> getUnsubscribeArgumentsFromDecodedMessage(T decodedMessage);

    /**
     * Get the names of the records to resync
     * 
     * @param decodedMessage
     *            the decoded data for the resync command
     * @return the names of the records in the resync command
     */
    List<String> getResyncArgumentsFromDecodedMessage(T decodedMessage);

    /**
     * @return the message byte[] to send that represents the atomic change
     */
    byte[] getTxMessageForAtomicChange(IRecordChange atomicChange);

    /**
     * @param data
     *            the {@link ByteBuffer} holding an atomic change
     * @return the atomic change read from the decoded message
     */
    IRecordChange getAtomicChangeFromRxMessage(ByteBuffer data);

    /**
     * @return the message byte[] to send that represents an identity call (this identifies the
     *         connecting proxy to the context)
     */
    byte[] getTxMessageForIdentify(String proxyIdentity);

    /**
     * @param names
     *            the list of record names to subscribe for. <b>Note:</b> the first item is a
     *            permission token.
     * 
     * @return the message byte[] to send that represents a subscribe for the named records
     * @see #getSubscribeArgumentsFromDecodedMessage(Object)
     */
    byte[] getTxMessageForSubscribe(String... names);

    /**
     * @return the message byte[] to send that represents an unsubscribe for the named records
     */
    byte[] getTxMessageForUnsubscribe(String... names);

    /**
     * @return the message byte[] to send that represents a resync for the named records
     */
    byte[] getTxMessageForResync(String... names);

    /**
     * Create an {@link IRecordChange} representing the RPC and encode into a byte[]
     * 
     * @param rpcName
     *            the RPC name
     * @param args
     *            the RPC arguments
     * @param resultRecordName
     *            the record name for the RPC result
     * @return the byte[] for the RPC call and details
     * @see Remote for a description of the atomic change structure
     * @see #getRpcFromRxMessage(byte[])
     */
    byte[] getTxMessageForRpc(String rpcName, IValue[] args, String resultRecordName);

    /**
     * Resolve an {@link IRecordChange} representing an RPC from the decoded message
     * 
     * @param decodedMessage
     *            the message to resolve the RPC from
     * @return an {@link IRecordChange} holding the RPC details
     * @see Remote for a description of the atomic change structure
     * @see #getAtomicChangeFromRxMessage(byte[])
     */
    IRecordChange getRpcFromRxMessage(T decodedMessage);

    /**
     * Decode the ByteBuffer into the data object type
     */
    T decode(ByteBuffer rxMessage);

    /**
     * @return the frame encoding format for the wire protocol for this codec
     */
    FrameEncodingFormatEnum getFrameEncodingFormat();

    /**
     * @return a new instance of this codec
     */
    ICodec<T> newInstance();

    /**
     * @return the charset used by this code
     */
    Charset getCharset();

    /**
     * Called just before sending on the wire, allows session protocol specific encoding actions to
     * be performed.
     * 
     * @param data
     *            the prepared data to send
     * @return the final data to send
     */
    byte[] finalEncode(byte[] data);

    /**
     * @return the session protocol instance for this codec
     */
    ISessionProtocol getSessionProtocol();
}
