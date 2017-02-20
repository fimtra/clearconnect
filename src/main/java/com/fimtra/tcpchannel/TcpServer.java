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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.CollectionUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.Pair;

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

    final static long MIN_SOCKET_ALIVE_TIME_MILLIS = 1000;
    final static int BLACKLIST_TIME_MILLIS = 300000;
    final static int MAX_SHORT_LIVED_SOCKET_TRIES = 5;

    final ServerSocketChannel serverSocketChannel;

    final Map<ITransportChannel, Pair<String, Long>> clients =
        Collections.synchronizedMap(new HashMap<ITransportChannel, Pair<String, Long>>());

    /** Key=host IP, value=Pair<number of short-lived socket connections, last short-lived time> */
    final Map<String, Pair<Long, Long>> potentialBlacklistHosts =
        Collections.synchronizedMap(new HashMap<String, Pair<Long, Long>>());
    /** Key=host IP, value=system time IP was blacklisted */
    final Map<String, Long> blacklistHosts = Collections.synchronizedMap(new HashMap<String, Long>());

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
            this.aclPatterns =
                Collections.unmodifiableSet(constructPatterns(CollectionUtils.newSetFromString(acl, ";")));
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
                        String hostAddress = null;
                        if (TcpServer.this.aclPatterns.size() > 0)
                        {
                            final SocketAddress remoteAddress = socketChannel.socket().getRemoteSocketAddress();
                            if (remoteAddress instanceof InetSocketAddress)
                            {
                                hostAddress = ((InetSocketAddress) remoteAddress).getAddress().getHostAddress();
                                boolean matched = false;
                                if (isBlacklistedHost(hostAddress))
                                {
                                    socketChannel.close();
                                    return;
                                }
                                for (Pattern pattern : TcpServer.this.aclPatterns)
                                {
                                    if (pattern.matcher(hostAddress).matches())
                                    {
                                        Log.log(TcpServer.this, "IP address ", hostAddress, " matches ACL entry ",
                                            pattern.toString());
                                        matched = true;
                                    }
                                }
                                if (!matched)
                                {
                                    Log.log(TcpServer.this, "*** ACCESS VIOLATION *** IP address ", hostAddress,
                                        " does not match any ACL pattern");
                                    socketChannel.close();
                                    return;
                                }
                            }
                            else
                            {
                                Log.log(TcpServer.this,
                                    "*** WARNING *** rejecting connection, unhandled socket type (this should never happen!)");
                                socketChannel.close();
                                return;
                            }
                        }

                        socketChannel.configureBlocking(false);
                        TcpServer.this.clients.put(new TcpChannel(socketChannel, new IReceiver()
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
                                final Pair<String, Long> hostAndStartTime = TcpServer.this.clients.remove(tcpChannel);
                                try
                                {
                                    clientSocketReceiver.onChannelClosed(tcpChannel);
                                }
                                finally
                                {
                                    if (System.currentTimeMillis()
                                        - hostAndStartTime.getSecond().longValue() < MIN_SOCKET_ALIVE_TIME_MILLIS)
                                    {
                                        Pair<Long, Long> shortLivedCountAndLastTime =
                                            TcpServer.this.potentialBlacklistHosts.get(hostAndStartTime.getFirst());
                                        if (shortLivedCountAndLastTime == null)
                                        {
                                            shortLivedCountAndLastTime = new Pair<Long, Long>(0l, 0l);
                                        }
                                        shortLivedCountAndLastTime = new Pair<Long, Long>(
                                            Long.valueOf(shortLivedCountAndLastTime.getFirst().longValue() + 1),
                                            Long.valueOf(System.currentTimeMillis()));
                                        TcpServer.this.potentialBlacklistHosts.put(hostAndStartTime.getFirst(),
                                            shortLivedCountAndLastTime);

                                        if (shortLivedCountAndLastTime.getFirst().intValue() > MAX_SHORT_LIVED_SOCKET_TRIES)
                                        {
                                            Log.log(TcpServer.this,
                                                "*** WARNING *** too many short-lived connections, blacklisting ",
                                                hostAndStartTime.getFirst(), " for the next ",
                                                Long.toString(TimeUnit.MINUTES.convert(BLACKLIST_TIME_MILLIS,
                                                    TimeUnit.MILLISECONDS)),
                                                " mins");
                                            TcpServer.this.blacklistHosts.put(hostAndStartTime.getFirst(),
                                                Long.valueOf(System.currentTimeMillis()));
                                        }
                                    }
                                    else
                                    {
                                        // todo can we clear the potential blacklisting?
                                    }
                                }
                            }
                        }, clientSocketRxBufferSize, frameEncodingFormat),
                            new Pair<String, Long>(hostAddress, Long.valueOf(System.currentTimeMillis())));
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
            throw new RuntimeException(
                "Could not create " + ObjectUtils.safeToString(this) + " at " + address + ":" + port, e);
        }
    }

    boolean isBlacklistedHost(String hostAddress)
    {
        synchronized (this.blacklistHosts)
        {
            Long blacklistStartTimeMillis = this.blacklistHosts.get(hostAddress);
            {
                if (blacklistStartTimeMillis != null)
                {
                    if (System.currentTimeMillis() - blacklistStartTimeMillis.longValue() < BLACKLIST_TIME_MILLIS)
                    {
                        Log.log(this, "*** WARNING *** rejected connection from blacklisted ", hostAddress);
                        return true;
                    }
                    else
                    {
                        Log.log(this, "Clearing blacklist status for ", hostAddress);
                        this.blacklistHosts.remove(hostAddress);
                        this.potentialBlacklistHosts.remove(hostAddress);
                    }
                }
            }
        }
        return false;
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

        final Set<ITransportChannel> connections;
        synchronized (this.clients)
        {
            connections = new HashSet<ITransportChannel>(this.clients.keySet());
        }
        for (ITransportChannel client : connections)
        {
            client.destroy("TcpServer shutting down");
        }
        this.clients.clear();

        // kick off a thread to try a connection to ensure no connection exists
        try
        {
            new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    Socket socket = null;
                    try
                    {
                        socket = new Socket(TcpServer.this.localSocketAddress.getAddress(),
                            TcpServer.this.localSocketAddress.getPort());
                    }
                    catch (Exception e)
                    {
                        // don't care
                    }
                    finally
                    {
                        if (socket != null)
                        {
                            try
                            {
                                socket.close();
                            }
                            catch (IOException e)
                            {
                                // don't care
                            }
                        }
                    }
                }
            }, "socket-destroy-" + this.localSocketAddress.getAddress() + ":"
                + this.localSocketAddress.getPort()).start();
        }
        catch (Exception e)
        {
            // do we care about this...
        }
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
