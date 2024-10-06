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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.CollectionUtils;
import com.fimtra.util.FileUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.Pair;
import com.fimtra.util.ThreadUtils;
import com.fimtra.util.UtilProperties;

/**
 * A TCP server socket component. A TcpServer is constructed with an {@link IReceiver} that will be
 * used to handle <b>all</b> client socket communication.
 * <p>
 * <h5>Threading</h5> All client messages are received using one of the
 * {@link TcpChannelUtils#READERS} threads. The reader thread will invoke the
 * {@link IReceiver#onDataReceived} method for every TCP message received from a
 * connected client socket. Therefore the receiver implementation must be efficient so as not to
 * block other client messages from being processed.
 *
 * @author Ramon Servadei
 */
public class TcpServer implements IEndPointService
{
    static final Set<TcpServer> LIVE_INSTANCES = Collections.synchronizedSet(new HashSet<>());

    static
    {
        if (TcpChannelProperties.Values.SERVER_CONNECTION_LOGGING)
        {
            final Runnable connectionDumpTask = new Runnable()
            {
                final File serverConnectionsFile = FileUtils.createLogFile_yyyyMMddHHmmss(UtilProperties.Values.LOG_DIR,
                        ThreadUtils.getMainMethodClassSimpleName() + "-serverConnections");

                @Override
                public void run()
                {
                    try (PrintWriter pw = new PrintWriter(new FileWriter(this.serverConnectionsFile)))
                    {
                        final Set<TcpServer> currentSet;
                        synchronized (LIVE_INSTANCES)
                        {
                            currentSet = new HashSet<>(LIVE_INSTANCES);
                        }
                        pw.println("HOST IP, CONNECTION COUNT, BLOCKED, BLACKLISTED");
                        for (TcpServer liveInstance : currentSet)
                        {
                            final Map<String, AtomicInteger> connectedHostsCopy;
                            synchronized (liveInstance.connectedHosts)
                            {
                                connectedHostsCopy = new HashMap<>(liveInstance.connectedHosts);
                            }
                            final List<String> sortedIps = new LinkedList<>(connectedHostsCopy.keySet());
                            Collections.sort(sortedIps);
                            final int localPort = liveInstance.serverSocketChannel.socket()
                                    .getLocalPort();
                            for (String IP : sortedIps)
                            {
                                pw.print(IP + "->:" + localPort);
                                pw.print(", ");
                                pw.print(connectedHostsCopy.get(IP));
                                pw.print(", ");
                                pw.print(Boolean.TRUE.equals(liveInstance.blockedHosts.get(IP)) ? "BLOCKED" :
                                        "");
                                pw.print(", ");
                                pw.println(Boolean.TRUE.equals(liveInstance.tempBlacklistedHosts.get(IP)) ?
                                        "BLACKLISTED (temp)" : "");
                            }
                        }
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(
                                "Could not create " + ObjectUtils.safeToString(this.serverConnectionsFile),
                                e);
                    }
                }
            };
            ThreadUtils.newScheduledExecutorService("server-connections", 1).scheduleAtFixedRate(connectionDumpTask, 1,
                    1, TimeUnit.MINUTES);
        }
    }

    static List<Long> getSortedConnectionTimes(Map<ITransportChannel, Pair<String, Long>> clients,
            String hostAddress, long gracePeriodEndMillis)
    {
        return clients.values()
                .stream()
                // only count connections after the grace period
                .filter(p -> p.getSecond() > gracePeriodEndMillis)
                .filter(p -> hostAddress.equals(p.getFirst()))
                .map(Pair::getSecond)
                .sorted()
                .collect(Collectors.toList());
    }

    static Map<Long, Long> getConnectionHistogram(final List<Long> timeRange)
    {
        // work out the diffs between connections
        final AtomicLong last = new AtomicLong(timeRange.get(0));
        final List<Long> diffIn10SecIntervals = timeRange.stream()
                .map(t -> (((t - last.getAndSet(t)) / 10_000) * 10L))
                .collect(Collectors.toList());
        // get the histogram of connection diffs
        return diffIn10SecIntervals.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    private static void blackListConnection(SocketChannel socketChannel, String hostAddress,
            ConcurrentMap<String, Boolean> blacklistedHosts) throws IOException
    {
        socketChannel.close();
        blacklistedHosts.putIfAbsent(hostAddress, Boolean.TRUE);
    }

    final static int DEFAULT_SERVER_RX_BUFFER_SIZE = 65535;

    final ServerSocketChannel serverSocketChannel;

    final Map<ITransportChannel, Pair<String, Long>> clients =
            Collections.synchronizedMap(new HashMap<>());

    /**
     * Key=host IP, value=number of short-lived socket connections<br>
     * Synchronise access using {@link #blacklistHosts}.
     */
    final Map<String, AtomicLong> shortLivedSocketCountPerHost = new HashMap<>();
    /**
     * Key=host IP, value=system time IP was blacklisted<br>
     * Synchronise access using blacklistHosts.
     */
    final Map<String, Long> blacklistHosts = new HashMap<>();

    final InetSocketAddress localSocketAddress;

    final Set<Pattern> whitelistAclPatterns;
    final Set<Pattern> blacklistAclPatterns;
    final long gracePeriodEndMillis;
    final Map<String, AtomicInteger> connectedHosts = Collections.synchronizedMap(new HashMap<>());
    final ConcurrentMap<String, Boolean> blockedHosts = new ConcurrentHashMap<>();
    final ConcurrentMap<String, Boolean> tempBlacklistedHosts = new ConcurrentHashMap<>();

    /**
     * Construct the TCP server with default server and client receive buffer sizes and frame format
     * as {@link FrameEncodingFormatEnum#TERMINATOR_BASED}.
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
            this.gracePeriodEndMillis = System.currentTimeMillis()
                    + TcpChannelProperties.Values.SERVER_SUSPICIOUS_CONNECTION_GRACE_PERIOD_MILLIS;
            LIVE_INSTANCES.add(this);

            final String whitelistAcl = System.getProperty(TcpChannelProperties.Names.PROPERTY_NAME_SERVER_ACL, ".*");
            final String blacklistAcl = System.getProperty(TcpChannelProperties.Names.PROPERTY_NAME_SERVER_BLACKLIST_ACL);
            this.whitelistAclPatterns =
                    Collections.unmodifiableSet(constructPatterns(CollectionUtils.newSetFromString(whitelistAcl, ";")));
            this.blacklistAclPatterns =
                    Collections.unmodifiableSet(constructPatterns(CollectionUtils.newSetFromString(blacklistAcl, ";")));
            Log.log(this, "WHITELIST ACL is: ", this.whitelistAclPatterns.toString());
            Log.log(this, "BLACKLIST ACL is: ", this.blacklistAclPatterns.toString());
            this.serverSocketChannel = ServerSocketChannel.open();
            this.serverSocketChannel.configureBlocking(false);

            this.serverSocketChannel.socket().setReuseAddress(port == 0 ? false : reuseAddress);
            this.serverSocketChannel.socket().setReceiveBufferSize(serverRxBufferSize);

            TcpChannelUtils.bind(this.serverSocketChannel.socket(), address, port);

            TcpChannelUtils.ACCEPT_PROCESSOR.register(this.serverSocketChannel, () -> {
                try
                {
                    if (!this.serverSocketChannel.isOpen())
                    {
                        Log.log(TcpServer.this, ObjectUtils.safeToString(TcpServer.this), " server socket closed");
                        return;
                    }

                    final SocketChannel socketChannel = this.serverSocketChannel.accept();
                    if (socketChannel == null)
                    {
                        return;
                    }
                    Log.log(TcpServer.this, ObjectUtils.safeToString(TcpServer.this), " (<-) accepted inbound ",
                            ObjectUtils.safeToString(socketChannel));
                    final SocketAddress remoteAddress = socketChannel.socket()
                            .getRemoteSocketAddress();
                    final String hostAddress = remoteAddress instanceof InetSocketAddress ?
                            ((InetSocketAddress) remoteAddress).getAddress()
                                    .getHostAddress() : null;
                    if (hostAddress == null)
                    {
                        Log.log(TcpServer.this,
                                "*** WARNING *** rejecting connection, unhandled socket type (this should never happen!): ",
                                ObjectUtils.safeToString(socketChannel));
                        socketChannel.close();
                        return;
                    }

                    final long nowMillis = System.currentTimeMillis();
                    if (isBlacklistedHost(hostAddress, nowMillis))
                    {
                        blackListConnection(socketChannel, hostAddress, tempBlacklistedHosts);
                        return;
                    }

                    if (!this.whitelistAclPatterns.isEmpty() || !this.blacklistAclPatterns.isEmpty())
                    {
                        boolean whitelisted = false;
                        boolean blacklisted = false;
                        for (Pattern pattern : this.blacklistAclPatterns)
                        {
                            if (pattern.matcher(hostAddress)
                                    .matches())
                            {
                                blacklisted = true;
                                break;
                            }
                        }
                        if (!blacklisted)
                        {
                            for (Pattern pattern : this.whitelistAclPatterns)
                            {
                                if (pattern.matcher(hostAddress)
                                        .matches())
                                {
                                    whitelisted = true;
                                    break;
                                }
                            }
                        }
                        if (!whitelisted)
                        {
                            Log.log(TcpServer.this, "*** ACCESS VIOLATION *** IP address ", hostAddress,
                                    (!blacklisted ? " does not match any ACL pattern" : " is blacklisted"));
                            blackListConnection(socketChannel, hostAddress, blockedHosts);
                            return;
                        }
                    }

                    if (suspiciousConnectionTrend(hostAddress, nowMillis))
                    {
                        Log.log(TcpServer.this, "*** SUSPICIOUS ACTIVITY *** IP address ", hostAddress,
                                " is making too many periodic connections");
                        // NOTE: after the SLS_BLACKLIST_TIME_MILLIS expires, the host will be removed from
                        // the black list, but may fail on suspicious activity again
                        blackListConnection(socketChannel, hostAddress, tempBlacklistedHosts);
                        return;
                    }

                    connectedHosts.computeIfAbsent(hostAddress, h -> new AtomicInteger(0))
                            .incrementAndGet();

                    socketChannel.configureBlocking(false);
                    this.clients.put(new TcpChannel(socketChannel, new IReceiver()
                            {
                                @Override
                                public void onDataReceived(ByteBuffer data, ITransportChannel source)
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
                                        if (hostAndStartTime != null)
                                        {
                                            checkShortLivedSocket(hostAndStartTime);
                                        }
                                        synchronized (connectedHosts)
                                        {
                                            final AtomicInteger connectionCount =
                                                    connectedHosts.get(hostAddress);
                                            if (connectionCount != null)
                                            {
                                                if (connectionCount.decrementAndGet() == 0)
                                                {
                                                    connectedHosts.remove(hostAddress);
                                                }
                                            }
                                        }
                                    }
                                }
                            }, clientSocketRxBufferSize, frameEncodingFormat),
                            new Pair<>(hostAddress, nowMillis));
                }
                catch (Exception e)
                {
                    Log.log(TcpServer.this,
                            ObjectUtils.safeToString(TcpServer.this) + " could not accept client connection", e);
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

    /**
     * This detects suspicious connection activity, e.g. slow increase in counts at regular periods
     *
     * @return true if the host is exhibiting abnormal connection patterns
     */
    boolean suspiciousConnectionTrend(String hostAddress, long nowMillis)
    {
        // we have a grace period to allow a valid "burst" connection on server bounce
        if (nowMillis < gracePeriodEndMillis)
        {
            return false;
        }

        final List<Long> sortedConnectionTimes;
        synchronized (clients)
        {
            sortedConnectionTimes = getSortedConnectionTimes(clients, hostAddress, gracePeriodEndMillis);
        }

        // if more than X connections started AFTER grace period, we check for periodic connections...
        if (sortedConnectionTimes.size() > TcpChannelProperties.Values.SERVER_SUSPICIOUS_CONNECTION_LIMIT)
        {
            final Map<Long, Long> histogram = getConnectionHistogram(sortedConnectionTimes);
            if (histogram.size() < 4)
            {
                // we have very regular connection periods in 10 sec intervals (0, 10, 20 say)
                Log.log(this, "Detected repeating and regular connection periods {period=counts,...}=",
                        histogram.toString());
                return true;
            }
        }
        return false;
    }

    boolean isBlacklistedHost(String hostAddress, long nowMillis)
    {
        synchronized (this.blacklistHosts)
        {
            final Long blacklistStartTimeMillis = this.blacklistHosts.get(hostAddress);
            if (blacklistStartTimeMillis != null)
            {
                if (nowMillis
                        - blacklistStartTimeMillis < TcpChannelProperties.Values.SLS_BLACKLIST_TIME_MILLIS)
                {
                    Log.log(this, "*** WARNING *** rejected connection from blacklisted ", hostAddress);
                    return true;
                }
                else
                {
                    Log.log(this, "Clearing blacklist status for ", hostAddress);
                    this.blacklistHosts.remove(hostAddress);
                    this.shortLivedSocketCountPerHost.remove(hostAddress);
                }
            }
            return false;
        }
    }

    private static Set<? extends Pattern> constructPatterns(Set<String> set)
    {
        Set<Pattern> patterns = new HashSet<>(set.size());
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

        TcpChannelUtils.ACCEPT_PROCESSOR.cancel(this.serverSocketChannel);
        LIVE_INSTANCES.remove(this);

        try
        {
            this.serverSocketChannel.socket().close();
            this.serverSocketChannel.close();
        }
        catch (IOException e)
        {
            Log.log(TcpChannelUtils.class, "Could not close " + ObjectUtils.safeToString(this.serverSocketChannel), e);
        }

        final Set<ITransportChannel> connections;
        synchronized (this.clients)
        {
            connections = new HashSet<>(this.clients.keySet());
        }
        for (ITransportChannel client : connections)
        {
            client.destroy("TcpServer shutting down");
        }
        this.clients.clear();

        // kick off a thread to try a connection to ensure no connection exists
        new Thread(() -> {
            try (Socket ignored = new Socket(TcpServer.this.localSocketAddress.getAddress(),
                    TcpServer.this.localSocketAddress.getPort()))
            {
            }
            catch (Exception e)
            {
                // don't care
            }
        }, "socket-destroy-" + this.localSocketAddress.getAddress() + ":"
                + this.localSocketAddress.getPort()).start();
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
                clients[i].send(txMessage);
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

    void checkShortLivedSocket(final Pair<String, Long> hostAndStartTime)
    {
        long nowMillis = System.currentTimeMillis();
        synchronized (this.blacklistHosts)
        {
            AtomicLong shortLivedSocketCount = this.shortLivedSocketCountPerHost.get(hostAndStartTime.getFirst());

            if (nowMillis - hostAndStartTime.getSecond()
                    < TcpChannelProperties.Values.SLS_MIN_SOCKET_ALIVE_TIME_MILLIS)
            {
                if (shortLivedSocketCount == null)
                {
                    shortLivedSocketCount = new AtomicLong(0);
                    this.shortLivedSocketCountPerHost.put(hostAndStartTime.getFirst(), shortLivedSocketCount);
                }

                if (shortLivedSocketCount.incrementAndGet() > TcpChannelProperties.Values.SLS_MAX_SHORT_LIVED_SOCKET_TRIES)
                {
                    Log.log(this, "*** WARNING *** too many short-lived connections, blacklisting ",
                            hostAndStartTime.getFirst(), " for the next ",
                            Long.toString(TimeUnit.MINUTES.convert(TcpChannelProperties.Values.SLS_BLACKLIST_TIME_MILLIS,
                                    TimeUnit.MILLISECONDS)),
                            " mins");
                    this.blacklistHosts.put(hostAndStartTime.getFirst(), nowMillis);
                }
            }
            else
            {
                if (shortLivedSocketCount != null)
                {
                    if (shortLivedSocketCount.decrementAndGet() == 0)
                    {
                        this.shortLivedSocketCountPerHost.remove(hostAndStartTime.getFirst());
                    }
                }
            }
        }
    }
}
