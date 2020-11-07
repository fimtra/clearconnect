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
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.core.PlatformUtils;
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
    static final Map<String, String> SOCKET_OPTIONS = new HashMap<>();

    static
    {
        Map.Entry<Object, Object> entry;
        Object key = null;
        Object value;
        for (Iterator<Map.Entry<Object, Object>> it =
             System.getProperties().entrySet().iterator(); it.hasNext(); )
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
     * @author Ramon Servadei
     * @see TcpChannel#TcpChannel(String, int, IReceiver, int)
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
     * <b>None of the parameters are used.</b>
     *
     * @see TransportTechnologyEnum#getNextAvailableServicePort()
     * @deprecated use {@link ChannelUtils#getNextAvailableServicePort()}
     */
    @SuppressWarnings("unused")
    @Deprecated
    public static int getNextFreeTcpServerPort(String hostName, int startPortRangeInclusive,
            int endPortRangeExclusive)
    {
        return ChannelUtils.getNextAvailableServicePort();
    }

    /**
     * Handles socket read operations for {@link TcpChannel} instances.
     */
    static final SelectorProcessor[]  READER = new SelectorProcessor[TcpChannelProperties.Values.READER_THREAD_COUNT];
    private static int currentReader = -1;

    static synchronized SelectorProcessor nextReader()
    {
        if (++currentReader == READER.length)
        {
            currentReader = 0;
        }
        SelectorProcessor reader = READER[currentReader];
        if (reader == null)
        {
            reader = new SelectorProcessor("tcp-channel-reader-" + currentReader, SelectionKey.OP_READ);
            READER[currentReader] = reader;
        }
        return reader;
    }

    static synchronized void freeReader(SelectorProcessor reader)
    {
        for (int i = 0; i < READER.length; i++)
        {
            if (reader == READER[i])
            {
                currentReader = i - 1;
            }
        }
    }

    /**
     * Handles all socket accept operations for all {@link TcpServer} instances
     */
    final static SelectorProcessor ACCEPT_PROCESSOR =
            new SelectorProcessor("tcp-channel-accept", SelectionKey.OP_ACCEPT);

    /**
     * The public facing IP for the local host
     */
    public final static String LOCALHOST_IP;

    static
    {
        String hostAddress;
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
     * Given a ByteBuffer of data, this method decodes it into an array of {@link ByteBuffer}
     * objects. Data between {@link TcpChannel} objects is encoded in the following format (ABNF
     * notation):
     *
     * <pre>
     *  stream = 1*frame
     *  frame = length data
     *
     *  length = 4OCTET ; big-endian integer indicating the length of the data
     *  data = 1*OCTET ; the data
     * </pre>
     *
     * @param frames      the reference for the array to place the decoded frames resolved from the buffer.
     *                    Any incomplete frames are left in the buffer.
     * @param framesSize  used as a reference to pass back the size of the array
     * @param buffer      a bytebuffer holding any number of frames or <b>partial frames</b>
     * @param bufferArray the array of bytes extracted from the buffer
     * @return the array with the decoded frames, different to the frames argument if the array was
     * resized
     * @throws BufferOverflowException if the buffer size cannot hold a complete frame
     */
    static ByteBuffer[] decode(ByteBuffer[] frames, int[] framesSize, ByteBuffer buffer, byte[] bufferArray)
            throws BufferOverflowException
    {
        ByteBuffer[] decoded = frames;
        framesSize[0] = 0;
        buffer.flip();
        // the TCP message could be a concatenation of multiple ones
        // the format is: <4 bytes len><data><4 bytes len><data>
        int len;
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
                        final String overflowMessage = "Need to read " + len
                                + " but buffer has no more space from current position: " + buffer.toString();
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
                if (framesSize[0] == decoded.length)
                {
                    decoded = Arrays.copyOf(decoded, decoded.length + 2);
                }
                decoded[framesSize[0]++] = ByteBuffer.wrap(bufferArray, position, len);
                buffer.position(position + len);
            }
        }
        return decoded;
    }

    /**
     * Given a ByteBuffer of data containing frames delimited by a termination byte, this method
     * decodes it into an array of {@link ByteBuffer} objects, one for each frame. This method
     * assumes data is encoded in the following format (ABNF notation):
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
     * @param frames      the reference for the array to place the decoded frames resolved from the buffer.
     *                    Any incomplete frames are left in the buffer.
     * @param framesSize  used as a reference to pass back the size of the array
     * @param buffer      a bytebuffer holding any number of frames or <b>partial frames</b>
     * @param bufferArray the array of bytes extracted from the buffer
     * @param terminator  the byte sequence for the end of a frame
     * @return the array with the decoded frames, different to the frames argument if the array was
     * resized
     * @throws BufferOverflowException if the buffer size cannot hold a complete frame
     */
    static ByteBuffer[] decodeUsingTerminator(ByteBuffer[] frames, int[] framesSize, ByteBuffer buffer,
            byte[] bufferArray, byte[] terminator) throws BufferOverflowException
    {
        ByteBuffer[] decoded = frames;
        framesSize[0] = 0;
        buffer.flip();
        int[] terminatorIndex = new int[2];
        int terminatorIndexPtr = 0;
        final int limit = buffer.limit();
        int i;
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
            for (i = 0; i < terminatorIndexPtr; i++)
            {
                len = terminatorIndex[i] - lastTerminatorIndex;
                if (framesSize[0] == decoded.length)
                {
                    decoded = Arrays.copyOf(decoded, decoded.length + 2);
                }
                decoded[framesSize[0]++] = ByteBuffer.wrap(bufferArray, lastTerminatorIndex, len);
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
        return decoded;
    }

    /**
     * Create a {@link SocketChannel}, connect in blocking mode to the given TCP server host and
     * port and then set the socket channel to non-blocking mode.
     *
     * @param host the host name of the target TCP server socket to connect to
     * @param port the port of the target TCP server socket to connect to
     * @return a connected socket channel in <b>non-blocking</b> mode
     * @throws ConnectException if the socket could not be created or connected
     */
    static SocketChannel createAndConnectNonBlockingSocketChannel(String host, int port)
            throws ConnectException
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
     * @param socketChannel the socket to configure options for
     */
    static void setOptions(SocketChannel socketChannel)
    {
        String key;
        String value;
        for (Map.Entry<String, String> entry : SOCKET_OPTIONS.entrySet())
        {
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
                Log.log(TcpChannelUtils.class,
                        "Could not set socket option " + ObjectUtils.safeToString(key) + " on "
                                + ObjectUtils.safeToString(socketChannel), e);
            }
        }
    }

    private static int lastEphemeralPort = -1;

    /**
     * Bind the server socket to the IP address and port. If the address is <code>null</code>, uses
     * the localhost IP. If the port is 0 AND the system properties for the ephemeral port range
     * start-end are set then an ephemeral port within the range is used. If the port is 0 and no
     * system properties are set, an ephemeral port is allocated.
     */
    static void bind(ServerSocket socket, String address, int port) throws IOException
    {
        if (port == 0 && TcpChannelProperties.Values.EPHEMERAL_PORT_RANGE_START > -1
                && TcpChannelProperties.Values.EPHEMERAL_PORT_RANGE_END > -1)
        {
            bindWithinRange(socket, address, TcpChannelProperties.Values.EPHEMERAL_PORT_RANGE_START,
                    TcpChannelProperties.Values.EPHEMERAL_PORT_RANGE_END);
        }
        else
        {
            socket.bind(
                    new InetSocketAddress(address == null ? TcpChannelUtils.LOCALHOST_IP : address, port));
        }
    }

    /**
     * Bind the server socket to an ephemeral port within the range start and end, inclusive. If the
     * address is <code>null</code>, uses the localhost IP.
     */
    static synchronized void bindWithinRange(ServerSocket socket, String address,
            final int ephemeralRangeStart, final int ephemeralRangeEnd) throws IOException
    {
        int loop = 0;

        while (true)
        {
            if (lastEphemeralPort == -1 || lastEphemeralPort < ephemeralRangeStart)
            {
                lastEphemeralPort = ephemeralRangeStart;
            }
            else
            {
                lastEphemeralPort++;
            }
            if (lastEphemeralPort > ephemeralRangeEnd)
            {
                lastEphemeralPort = ephemeralRangeStart;
                if (++loop > 1)
                {
                    throw new IOException(
                            "No free port found between " + ephemeralRangeStart + "-" + ephemeralRangeEnd);
                }
            }
            try
            {
                final InetSocketAddress endpoint =
                        new InetSocketAddress(address == null ? TcpChannelUtils.LOCALHOST_IP : address,
                                lastEphemeralPort);
                socket.bind(endpoint);
                break;
            }
            catch (IOException e)
            {
                Log.log(TcpChannelUtils.class, "Could not bind to socket: " + lastEphemeralPort);
            }
        }
    }
}
