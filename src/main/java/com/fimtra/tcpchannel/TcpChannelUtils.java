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
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;

/**
 * Utility methods for a {@link TcpChannel}
 * <p>
 * Socket options for the runtime can be set using system properties in the form
 * <code>tcpchannel.{SocketOptions_constant}</code> e.g.
 * 
 * <pre>
 * -Dtcpchannel.TCP_NODELAY=true -Dtcpchannel.SO_RCVBUF=65535
 * </pre>
 * 
 * @author Ramon Servadei
 */
public abstract class TcpChannelUtils
{
    static final Map<String, String> SOCKET_OPTIONS = new HashMap<String, String>();
    static
    {
        Map.Entry<Object, Object> entry = null;
        Object key = null;
        Object value = null;
        for (Iterator<Map.Entry<Object, Object>> it = System.getProperties().entrySet().iterator(); it.hasNext();)
        {
            try
            {
                entry = it.next();
                key = entry.getKey();
                value = entry.getValue();
                if (key.toString().startsWith("tcpchannel."))
                {
                    SOCKET_OPTIONS.put(key.toString().substring("tcpchannel.".length()), value.toString());
                }
            }
            catch (Exception e)
            {
                Log.log(TcpChannelUtils.class, "Could not read socket option " + ObjectUtils.safeToString(key)
                    + " from system properties", e);
            }
        }
        Log.log(TcpChannelUtils.class, "Socket options: ", ObjectUtils.safeToString(SOCKET_OPTIONS));
    }

    /**
     * Thrown when a TCP channel buffer cannot hold the incoming message. In these situations, the
     * {@link TcpChannel} can be constructed with a bigger receive buffer.
     * 
     * @see TcpChannel#TcpChannel(String, int, IReceiver, int)
     * @author Ramon Servadei
     */
    public static class BufferOverflowException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        public BufferOverflowException(String message)
        {
            super(message);
        }
    }

    /**
     * Get the next free TCP server port in the passed in ranges for the given host.
     * 
     * @param hostName
     *            the hostname to use to find the next free default TCP server
     * @return a free TCP server port that can have a TCP server socket bound to it, -1 if there is
     *         not a free port
     */
    public synchronized static int getNextFreeTcpServerPort(String hostName, int startPortRangeInclusive,
        int endPortRangeExclusive)
    {
        if (ChannelUtils.TRANSPORT == TransportTechnologyEnum.SOLACE)
        {
            return System.identityHashCode(Runtime.getRuntime()) + (int) System.nanoTime();
        }

        String hostAddress = TcpChannelUtils.LOCALHOST_IP;
        try
        {
            hostAddress = InetAddress.getByName(hostName).getHostAddress();
        }
        catch (UnknownHostException e)
        {
        }
        for (int i = startPortRangeInclusive; i < endPortRangeExclusive; i++)
        {
            try
            {
                Log.log(TcpChannelUtils.class, "Trying ", hostAddress, ":", Integer.toString(i));
                final ServerSocket serverSocket = new ServerSocket();
                serverSocket.bind(new InetSocketAddress(hostAddress, i));
                serverSocket.close();
                // now ensure the server socket is closed before saying we can use it
                try
                {
                    int j = 0;
                    while (j++ < 10)
                    {
                        new Socket(hostAddress, i).close();
                        Thread.sleep(100);
                    }
                }
                catch (Exception e)
                {
                }
                Log.log(TcpChannelUtils.class, "Using ", hostAddress, ":", Integer.toString(i));
                return i;
            }
            catch (IOException e)
            {
                Log.log(TcpChannelUtils.class, e.getMessage());
            }
        }
        throw new RuntimeException("No free TCP port available betwen " + startPortRangeInclusive + " and "
            + endPortRangeExclusive);
    }

    /**
     * Handles the connection result for a call to
     * {@link TcpChannelUtils#connectSocketChannelInNonBlockingMode(SocketChannel, String, int, SelectorProcessor, IConnectionResultProcessor)}
     */
    static interface IConnectionResultProcessor
    {
        void onConnectionSuccess();

        void onConnectionFailed(Exception e);
    }

    /** Handles all socket read operations for all {@link TcpChannel} instances */
    final static SelectorProcessor READER = new SelectorProcessor("tcp-channel-reader", SelectionKey.OP_READ);

    /** Handles all socket write operations for all {@link TcpChannel} instances */
    final static SelectorProcessor WRITER = new SelectorProcessor("tcp-channel-writer", SelectionKey.OP_WRITE);

    /** Handles all socket accept operations for all {@link TcpServer} instances */
    final static SelectorProcessor ACCEPT_PROCESSOR = new SelectorProcessor("tcp-channel-accept",
        SelectionKey.OP_ACCEPT);

    /**
     * The public facing IP for the local host
     */
    public final static String LOCALHOST_IP;
    static
    {
        String hostAddress = null;
        try
        {
            hostAddress = InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Could not get host address", e);
        }
        LOCALHOST_IP = hostAddress;
    }

    private TcpChannelUtils()
    {
        super();
    }

    /**
     * Given a ByteBuffer of data, this method decodes it into a List of byte[] objects. Data
     * between {@link TcpChannel} objects is encoded in the following format (ABNF notation):
     * 
     * <pre>
     *  stream = 1*frame
     *  frame = length data
     * 
     *  length = 4OCTET ; big-endian integer indicating the length of the data
     *  data = 1*OCTET ; the data
     * </pre>
     * 
     * @param frames
     *            the queue to place the decoded frames resolved from the buffer. Any incomplete
     *            frames are left in the buffer.
     * @param buffer
     *            a bytebuffer holding any number of frames or <b>partial frames</b>
     * @throws BufferOverflowException
     *             if the buffer size cannot hold a complete frame
     */
    static void decode(Queue<byte[]> frames, ByteBuffer buffer) throws BufferOverflowException
    {
        buffer.flip();
        // the TCP message could be a concatenation of multiple ones
        // the format is: <4 bytes len><data><4 bytes len><data>
        int len;
        byte[] message;
        int position;
        while (buffer.position() < buffer.limit())
        {
            try
            {
                len = buffer.getInt();
            }
            catch (BufferUnderflowException e)
            {
                break;
            }

            position = buffer.position();
            if (position + len > buffer.limit())
            {
                if (position == 4)
                {
                    if (position + len > buffer.capacity())
                    {
                        // the buffer is full and the length indicates that there is more data than
                        // the buffer can hold. We cannot extract the data from the buffer so we
                        // need indicate the buffer is full.
                        final String overflowMessage =
                            "Need to read " + len + " but buffer has no more space from current position: "
                                + buffer.toString() + ", read=" + asString(frames) + ", buffer="
                                + new String(buffer.array());
                        buffer.clear();
                        throw new BufferOverflowException(overflowMessage);
                    }
                }
                // we're waiting for more data to read
                // set the position back
                buffer.position(buffer.position() - 4);
                break;
            }
            else
            {
                message = new byte[len];
                System.arraycopy(buffer.array(), position, message, 0, len);
                buffer.position(position + len);
                frames.add(message);
            }
        }
        buffer.compact();
    }

    private static String asString(Queue<byte[]> values)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (byte[] bs : values)
        {
            if (first)
            {
                first = false;
            }
            else
            {
                sb.append(", ");
            }
            sb.append(bs.length).append(" bytes");
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Given a ByteBuffer of data containing frames delimited by a termination byte, this method
     * decodes it into a List of byte[] objects, one for each frame. This method assumes data is
     * encoded in the following format (ABNF notation):
     * 
     * <pre>
     * stream = 1*frame
     * 
     * frame = data terminator
     * data = 1*OCTET ; the data
     * 
     * terminator = OCTET OCTET ; an ASCII control code, possibly 0x3 for ETX (end of text)
     * </pre>
     * 
     * @param frames
     *            the queue to place the decoded frames resolved from the buffer. Any incomplete
     *            frames are left in the buffer.
     * @param buffer
     *            a bytebuffer holding any number of frames or <b>partial frames</b>
     * @param terminator
     *            the byte sequence for the end of a frame
     * @throws BufferOverflowException
     *             if the buffer size cannot hold a complete frame
     */
    static void decodeUsingTerminator(Queue<byte[]> frames, ByteBuffer buffer, byte[] terminator)
        throws BufferOverflowException
    {
        buffer.flip();
        final byte[] bufferArray = buffer.array();
        int[] terminatorIndex = new int[2];
        int terminatorIndexPtr = 0;
        final int limit = buffer.limit();
        int i = 0;
        for (i = 0; i < limit; i++)
        {
            if (bufferArray[i] == terminator[0] && (i + 1 < limit) && bufferArray[i + 1] == terminator[1])
            {
                // mark the terminator index, expand the terminatorIndex array if we're at its limit
                terminatorIndex[terminatorIndexPtr++] = i;
                if (terminatorIndexPtr == terminatorIndex.length)
                {
                    // (always +2 as a heuristic for reducing constant resizing)
                    terminatorIndex = Arrays.copyOf(terminatorIndex, terminatorIndex.length + 2);
                }
            }
        }

        if (terminatorIndexPtr > 0)
        {
            int len;
            int lastTerminatorIndex = 0;
            byte[] frame;
            for (i = 0; i < terminatorIndexPtr; i++)
            {
                len = terminatorIndex[i] - lastTerminatorIndex;
                frame = new byte[len];
                System.arraycopy(bufferArray, lastTerminatorIndex, frame, 0, len);
                frames.add(frame);
                lastTerminatorIndex = terminatorIndex[i] + terminator.length;
            }
            buffer.position(lastTerminatorIndex);
        }
        else
        {
            // no terminator found, is the buffer totally full?
            if (buffer.limit() == buffer.capacity())
            {
                final String overflowMessage =
                    "No frame terminator found and buffer is at its limit: " + buffer.toString();
                buffer.clear();
                throw new BufferOverflowException(overflowMessage);
            }
        }
        buffer.compact();
    }

    /**
     * Create a {@link SocketChannel}, connect in blocking mode to the given TCP server host and
     * port and then set the socket channel to non-blocking mode.
     * 
     * @param host
     *            the host name of the target TCP server socket to connect to
     * @param port
     *            the port of the target TCP server socket to connect to
     * @return a connected socket channel in <b>non-blocking</b> mode
     * @throws ConnectException
     *             if the socket could not be created or connected
     */
    static SocketChannel createAndConnectNonBlockingSocketChannel(String host, int port) throws ConnectException
    {
        final SocketChannel socketChannel;
        try
        {
            socketChannel = SocketChannel.open();
        }
        catch (IOException e)
        {
            final String message = "Could not create socket channel for " + host + ":" + port;
            Log.log(TcpChannelUtils.class, message, e);
            throw new ConnectException(message);
        }
        try
        {
            socketChannel.configureBlocking(true);
            socketChannel.connect(new InetSocketAddress(host, port));
            socketChannel.configureBlocking(false);
            return socketChannel;
        }
        catch (ConnectException e)
        {
            closeChannel(socketChannel);
            final String message =
                "Could not connect socket channel to " + host + ":" + port + " (" + e.toString() + ")";
            throw new ConnectException(message);

        }
        catch (Exception e)
        {
            final String message =
                "Could not connect socket channel to " + host + ":" + port + " (" + e.toString() + ")";
            Log.log(TcpChannelUtils.class, message, e);
            closeChannel(socketChannel);
            throw new ConnectException(message);
        }
    }

    /**
     * Close the channel
     */
    static void closeChannel(final SocketChannel channel)
    {
        Log.log(TcpChannelUtils.class, "Closing ", ObjectUtils.safeToString(channel));
        try
        {
            channel.socket().close();
            channel.close();
        }
        catch (IOException e)
        {
            Log.log(TcpChannelUtils.class, "Could not close " + ObjectUtils.safeToString(channel), e);
        }
    }

    /**
     * Sets TCP socket options on the channel that are found in system properties.
     * <p>
     * e.g. -Dtcpchannel.TCP_NODELAY=true -Dtcpchannel.SO_RCVBUF=65535
     * 
     * @param socketChannel
     *            the socket to configure options for
     */
    static void setOptions(SocketChannel socketChannel)
    {
        Map.Entry<String, String> entry = null;
        String key = null;
        String value = null;
        for (Iterator<Map.Entry<String, String>> it = SOCKET_OPTIONS.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            key = entry.getKey();
            value = entry.getValue();
            try
            {
                if (key.equals("TCP_NODELAY"))
                {
                    socketChannel.socket().setTcpNoDelay(Boolean.valueOf(value).booleanValue());
                    continue;
                }
                if (key.equals("SO_REUSEADDR"))
                {
                    socketChannel.socket().setReuseAddress(Boolean.valueOf(value).booleanValue());
                    continue;
                }
                if (key.equals("IP_TOS"))
                {
                    socketChannel.socket().setTrafficClass(Integer.valueOf(value).intValue());
                    continue;
                }
                if (key.equals("SO_LINGER"))
                {
                    socketChannel.socket().setSoLinger(true, Integer.valueOf(value).intValue());
                    continue;
                }
                if (key.equals("SO_TIMEOUT"))
                {
                    socketChannel.socket().setSoTimeout(Integer.valueOf(value).intValue());
                    continue;
                }
                if (key.equals("SO_SNDBUF"))
                {
                    socketChannel.socket().setSendBufferSize(Integer.valueOf(value).intValue());
                    continue;
                }
                if (key.equals("SO_RCVBUF"))
                {
                    socketChannel.socket().setReceiveBufferSize(Integer.valueOf(value).intValue());
                    continue;
                }
                if (key.equals("SO_KEEPALIVE"))
                {
                    socketChannel.socket().setKeepAlive(Boolean.valueOf(value).booleanValue());
                    continue;
                }
                if (key.equals("SO_OOBINLINE"))
                {
                    socketChannel.socket().setOOBInline(Boolean.valueOf(value).booleanValue());
                    continue;
                }
                Log.log(TcpChannelUtils.class, "Unhandled socket option: ", key);
            }
            catch (Exception e)
            {
                Log.log(TcpChannelUtils.class, "Could not set socket option " + ObjectUtils.safeToString(key) + " on "
                    + ObjectUtils.safeToString(socketChannel), e);
            }
        }
    }
}
