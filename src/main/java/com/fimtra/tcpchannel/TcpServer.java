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
package com.fimtra.tcpchannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.CollectionUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;

/**
 * A TCP server socket component. A TcpServer is constructed with an {@link IReceiver} that will be
 * used to handle <b>all</b> client socket communication.
 * <p>
 * <h5>Threading</h5> All client messages are received using the thread of the
 * {@link TcpChannelUtils#READER}. The reader thread will invoke the
 * {@link IReceiver#onDataReceived(byte[], TcpChannel)} method for every TCP message received from a
 * connected client socket. Therefore the receiver implementation must be efficient so as not to
 * block other client messages from being processed.
 * 
 * @author Ramon Servadei
 */
public class TcpServer implements IEndPointService
{
    final static int DEFAULT_SERVER_RX_BUFFER_SIZE = 65535;

    final ServerSocketChannel serverSocketChannel;

    final List<ITransportChannel> clients = new CopyOnWriteArrayList<ITransportChannel>();

    final InetSocketAddress localSocketAddress;

    final Set<Pattern> aclPatterns;

    /**
     * Construct the TCP server with default server and client receive buffer sizes and frame format
     * as {@link FrameEncodingFormatEnum#TERMINATOR_BASED}.
     * 
     * @see #TcpServer(String, int, IReceiver, int, int, FrameEncodingFormatEnum)
     */
    public TcpServer(String address, int port, final IReceiver clientSocketReceiver)
    {
        this(address, port, clientSocketReceiver, FrameEncodingFormatEnum.TERMINATOR_BASED,
            DEFAULT_SERVER_RX_BUFFER_SIZE, TcpChannelProperties.Values.RX_BUFFER_SIZE,
            TcpChannelProperties.Values.SERVER_SOCKET_REUSE_ADDR);
    }

    /**
     * Construct the TCP server with default server and client receive buffer sizes and server
     * socket re-use address.
     * 
     * @see #TcpServer(String, int, IReceiver, int, int, FrameEncodingFormatEnum)
     */
    public TcpServer(String address, int port, final IReceiver clientSocketReceiver,
        TcpChannel.FrameEncodingFormatEnum frameEncodingFormat)
    {
        this(address, port, clientSocketReceiver, frameEncodingFormat, DEFAULT_SERVER_RX_BUFFER_SIZE,
            TcpChannelProperties.Values.RX_BUFFER_SIZE, TcpChannelProperties.Values.SERVER_SOCKET_REUSE_ADDR);
    }

    /**
     * Construct the TCP server
     * 
     * @param address
     *            the server socket address or host name, <code>null</code> to use the local host
     * @param port
     *            the server socket TCP port
     * @param clientSocketReceiver
     *            the receiver to attach to each new {@link TcpChannel} client that connects
     * @param frameEncodingFormat
     *            the frame encoding format for the TCP sockets for this server connection
     * @param clientSocketRxBufferSize
     *            the size (in bytes) of the receive buffer for the client {@link TcpChannel} in
     *            bytes
     * @param serverRxBufferSize
     *            the size of the receive buffer for the server socket
     * @param reuseAddress
     *            whether the server socket can re-use the address, see
     *            {@link Socket#setReuseAddress(boolean)}
     */
    public TcpServer(String address, int port, final IReceiver clientSocketReceiver,
        final FrameEncodingFormatEnum frameEncodingFormat, final int clientSocketRxBufferSize, int serverRxBufferSize,
        boolean reuseAddress)
    {
        super();
        try
        {
            final String acl = System.getProperty(TcpChannelProperties.Names.PROPERTY_NAME_SERVER_ACL, ".*");
            this.aclPatterns = Collections.unmodifiableSet(constructPatterns(CollectionUtils.newSetFromString(acl, ";")));
            Log.log(this, "ACL is: ", this.aclPatterns.toString());
            this.serverSocketChannel = ServerSocketChannel.open();
            this.serverSocketChannel.configureBlocking(false);

            this.serverSocketChannel.socket().setReuseAddress(reuseAddress);
            this.serverSocketChannel.socket().setReceiveBufferSize(serverRxBufferSize);

            this.serverSocketChannel.socket().bind(
                new InetSocketAddress(address == null ? TcpChannelUtils.LOCALHOST_IP : address, port));

            TcpChannelUtils.ACCEPT_PROCESSOR.register(this.serverSocketChannel, new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        SocketChannel socketChannel = TcpServer.this.serverSocketChannel.accept();
                        if (socketChannel == null)
                        {
                            return;
                        }
                        Log.log(this, ObjectUtils.safeToString(TcpServer.this), " (<-) accepted inbound ",
                            ObjectUtils.safeToString(socketChannel));
                        if (TcpServer.this.aclPatterns.size() > 0)
                        {
                            final SocketAddress remoteAddress = socketChannel.socket().getRemoteSocketAddress();
                            if (remoteAddress instanceof InetSocketAddress)
                            {
                                boolean matched = false;
                                String hostAddress = ((InetSocketAddress) remoteAddress).getAddress().getHostAddress();
                                for (Pattern pattern : TcpServer.this.aclPatterns)
                                {
                                    if (pattern.matcher(hostAddress).matches())
                                    {
                                        Log.log(this, "IP address ", hostAddress, " matches ACL entry ",
                                            pattern.toString());
                                        matched = true;
                                    }
                                }
                                if (!matched)
                                {
                                    Log.log(this, "*** ACCESS VIOLATION *** IP address ", hostAddress,
                                        " does not match any ACL pattern");
                                    socketChannel.close();
                                    return;
                                }
                            }
                        }

                        socketChannel.configureBlocking(false);
                        TcpServer.this.clients.add(new TcpChannel(socketChannel, new IReceiver()
                        {
                            @Override
                            public void onDataReceived(byte[] data, ITransportChannel source)
                            {
                                clientSocketReceiver.onDataReceived(data, source);
                            }

                            @Override
                            public void onChannelConnected(ITransportChannel tcpChannel)
                            {
                                clientSocketReceiver.onChannelConnected(tcpChannel);
                            }

                            @Override
                            public void onChannelClosed(ITransportChannel tcpChannel)
                            {
                                TcpServer.this.clients.remove(tcpChannel);
                                clientSocketReceiver.onChannelClosed(tcpChannel);
                            }
                        }, clientSocketRxBufferSize, frameEncodingFormat));
                    }
                    catch (Exception e)
                    {
                        Log.log(this, ObjectUtils.safeToString(TcpServer.this) + " could not accept client connection",
                            e);
                    }
                }
            });
            this.localSocketAddress = (InetSocketAddress) this.serverSocketChannel.socket().getLocalSocketAddress();
            Log.log(this, "Constructed ", ObjectUtils.safeToString(this));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not create " + ObjectUtils.safeToString(this) + " at " + address + ":"
                + port, e);
        }
    }

    private static Set<? extends Pattern> constructPatterns(Set<String> set)
    {
        Set<Pattern> patterns = new HashSet<Pattern>(set.size());
        for (String template : set)
        {
            patterns.add(Pattern.compile(template));
        }
        return patterns;
    }

    /**
     * Destroy this TCP server. This is idempotent; calling this multiple times has no extra effect.
     */
    @Override
    public void destroy()
    {
        Log.log(TcpChannelUtils.class, "Closing ", ObjectUtils.safeToString(this.serverSocketChannel));
        try
        {
            this.serverSocketChannel.socket().close();
            this.serverSocketChannel.close();
        }
        catch (IOException e)
        {
            Log.log(TcpChannelUtils.class, "Could not close " + ObjectUtils.safeToString(this.serverSocketChannel), e);
        }
        
        TcpChannelUtils.ACCEPT_PROCESSOR.cancel(this.serverSocketChannel);

        for (ITransportChannel client : this.clients)
        {
            client.destroy("TcpServer shutting down");
        }
        this.clients.clear();
    }

    @Override
    public String toString()
    {
        return "TcpServer [" + getEndPointAddress() + "]";
    }

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        destroy();
    }

    @Override
    public EndPointAddress getEndPointAddress()
    {
        return new EndPointAddress(this.localSocketAddress.getAddress().getHostAddress(),
            this.localSocketAddress.getPort());
    }

    @Override
    public int broadcast(String messageContext, byte[] txMessage, ITransportChannel[] clients)
    {
        for (int i = 0; i < clients.length; i++)
        {
            try
            {
                clients[i].sendAsync(txMessage);
            }
            catch (Exception e)
            {
                Log.log(this, "Could no send broadcast message to ", ObjectUtils.safeToString(clients[i]));
            }
        }
        return clients.length;
    }

    @Override
    public void endBroadcast(String messageContext)
    {
        // noop for a TcpServer
    }
}
