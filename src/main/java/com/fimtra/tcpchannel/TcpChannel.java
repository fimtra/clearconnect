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
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpChannelProperties.Values;
import com.fimtra.tcpchannel.TcpChannelUtils.BufferOverflowException;
import com.fimtra.util.CollectionUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;

/**
 * Provides the ability to send and receive data over TCP to a peer.
 * <p>
 * TCP socket options are configured by system properties and are applied by the
 * {@link TcpChannelUtils#setOptions(SocketChannel)} method.
 * 
 * @author Ramon Servadei
 */
public class TcpChannel implements ITransportChannel
{
    private static final double _INVERSE_1000000 = 1 / 1000000d;

    /** Expresses the encoding format for the data frames */
    public static enum FrameEncodingFormatEnum
    {
            /**
             * Length-based frame encoding, in the following format (ABNF notation):
             * 
             * <pre>
             *  stream = 1*frame
             *  frame = length data
             * 
             *  length = 4OCTET ; big-endian integer indicating the length of the data
             *  data = 1*OCTET ; the data
             * </pre>
             */
            LENGTH_BASED,

            /**
             * Terminator-based frame encoding, in the following format (ABNF notation):
             * 
             * <pre>
             * stream = 1*frame
             * 
             * frame = data terminator
             * data = 1*OCTET ; the data
             * 
             * terminator = OCTET OCTET ; an ASCII control code, possibly 0x3 for ETX (end of text)
             * </pre>
             */
            TERMINATOR_BASED;

        /**
         * @param tcpChannel
         *            the channel to create a frame reader-writer for
         * @return the reader-writer that supports this frame format
         */
        public IFrameReaderWriter getFrameReaderWriter(TcpChannel tcpChannel)
        {
            switch(this)
            {
                case LENGTH_BASED:
                    return new LengthBasedWriter(tcpChannel);
                case TERMINATOR_BASED:
                    return new TerminatorBasedReaderWriter(tcpChannel);
                default :
                    throw new IllegalStateException("No support for " + this);
            }
        }
    }

    private enum StateEnum
    {
            DESTROYED, IDLE, SENDING;
    }

    private static final String TCP_CHANNEL_CLOSED = "TcpChannel [closed ";
    private static final String TCP_CHANNEL_PENDING = "TcpChannel [pending ";
    private static final String TCP_CHANNEL_CONNECTED = "TcpChannel [connected ";

    /** Represents the ASCII code for CRLF */
    static final byte[] TERMINATOR = { 0xd, 0xa };

    /**
     * Holds a linked list (the chain) of channels that want to send. All the channels are bound to
     * the same {@link SelectorProcessor}.
     * 
     * @author Ramon Servadei
     */
    private static final class SendChannelChain
    {
        SendChannelChain()
        {
        }
        
        /**
         * This is the start of the logical linked-list of TcpChannels with data to send. This is an
         * equal-sending-opportunity mechanism. The list is traversed from start to end, each
         * channel has one frame sent, looping back to the start and continuing sending one frame
         * for each channel. If a channel has no more frames to sent, it is unlinked by
         * {@link #unlinkChannel(TcpChannel)}
         */
        volatile TcpChannel first;
        /**
         * The end of the logical linked-list of TcpChannels with data to send.
         * 
         * @see #first
         */
        volatile TcpChannel last;
    }

    private final static Map<SelectorProcessor, SendChannelChain> sendChannelChains =
        new HashMap<SelectorProcessor, SendChannelChain>();

    private synchronized static final void linkChannel(TcpChannel channel)
    {
        SendChannelChain chain = sendChannelChains.get(channel.writer);
        if (chain == null)
        {
            chain = new SendChannelChain();
            sendChannelChains.put(channel.writer, chain);
        }
        if (channel.prev == null && channel.next == null && chain.first != channel)
        {
            if (chain.first == null)
            {
                chain.first = channel;
                chain.last = channel;
                channel.prev = null;
                channel.next = null;
            }
            else
            {
                chain.last.next = channel;
                channel.prev = chain.last;
                channel.next = null;
                chain.last = channel;
            }
        }
    }

    private synchronized static final void unlinkChannel(TcpChannel channel)
    {
        SendChannelChain chain = sendChannelChains.get(channel.writer);
        if (chain == null)
        {
            chain = new SendChannelChain();
            sendChannelChains.put(channel.writer, chain);
        }
        if (channel.next != null)
        {
            channel.next.prev = channel.prev;
        }
        if (channel.prev != null)
        {
            channel.prev.next = channel.next;
        }
        if (channel == chain.first)
        {
            chain.first = channel.next;
        }
        if (channel == chain.last)
        {
            chain.last = channel.prev;
        }
        channel.next = null;
        channel.prev = null;
    }

    TcpChannel next;
    TcpChannel prev;

    final SelectorProcessor reader;
    final SelectorProcessor writer;
    int rxData;
    final IReceiver receiver;
    final ByteBuffer rxBytes;
    ByteBuffer[] readFrames = new ByteBuffer[10];
    byte[][] resolvedFrames = new byte[10][];
    final int[] readFramesSize = new int[1];
    final Queue<ByteBuffer[]> txFrames = CollectionUtils.newDeque();
    final ByteBuffer[][] txFrameBufferPool = new ByteBuffer[][] { new ByteBuffer[2], new ByteBuffer[2],
        new ByteBuffer[2], new ByteBuffer[2], new ByteBuffer[2], new ByteBuffer[2], };
    int txFrameBufferPoolPtr = 0;
    final SocketChannel socketChannel;
    final IFrameReaderWriter readerWriter;
    final ByteArrayFragmentResolver byteArrayFragmentResolver;
    /** The details of the end-point socket connection */
    final String endPointSocketDescription;
    /** The short-hand description of the end-point connections */
    final String shortSocketDescription;
    /**
     * Tracks if the {@link IReceiver#onChannelConnected(ITcpChannel)} has been called. Ensures its
     * only called once.
     */
    private boolean onChannelConnectedCalled;
    /**
     * Tracks if the {@link IReceiver#onChannelClosed(ITcpChannel)} has been called. Ensures its
     * only called once.
     */
    private final AtomicBoolean onChannelClosedCalled;
    private final Object lock = new Object();

    private final boolean writeToSocketUsingApplicationThread;

    StateEnum state = StateEnum.IDLE;

    /**
     * Construct a {@link TcpChannel} with a default receive buffer size and default frame encoding
     * format.
     * 
     * @see TcpChannelProperties#FRAME_ENCODING
     * @see TcpChannelProperties#RX_BUFFER_SIZE
     * @see #TcpChannel(String, int, IReceiver, int, FrameEncodingFormatEnum)
     */
    public TcpChannel(String serverHost, int serverPort, IReceiver receiver) throws ConnectException
    {
        this(serverHost, serverPort, receiver, TcpChannelProperties.Values.RX_BUFFER_SIZE);
    }

    /**
     * Construct a {@link TcpChannel} with a specific receive buffer size and default frame encoding
     * format.
     * 
     * @see TcpChannelProperties#FRAME_ENCODING
     * @see #TcpChannel(String, int, IReceiver, int, FrameEncodingFormatEnum)
     */
    public TcpChannel(String serverHost, int serverPort, IReceiver receiver, int rxBufferSize) throws ConnectException
    {
        this(serverHost, serverPort, receiver, rxBufferSize, TcpChannelProperties.Values.FRAME_ENCODING,
            TcpChannelProperties.Values.WRITE_TO_SOCKET_USING_APPLICATION_THREAD);
    }

    /**
     * Construct a {@link TcpChannel} with a default receive buffer size and specific frame encoding
     * format.
     * 
     * @see TcpChannelProperties#RX_BUFFER_SIZE
     * @see #TcpChannel(String, int, IReceiver, int, FrameEncodingFormatEnum)
     */
    public TcpChannel(String serverHost, int serverPort, IReceiver receiver,
        FrameEncodingFormatEnum frameEncodingFormat) throws ConnectException
    {
        this(serverHost, serverPort, receiver, TcpChannelProperties.Values.RX_BUFFER_SIZE, frameEncodingFormat,
            TcpChannelProperties.Values.WRITE_TO_SOCKET_USING_APPLICATION_THREAD);
    }

    /**
     * Construct a {@link TcpChannel} with a default receive buffer size and specific frame encoding
     * format.
     * 
     * @see TcpChannelProperties#RX_BUFFER_SIZE
     * @see #TcpChannel(String, int, IReceiver, int, FrameEncodingFormatEnum)
     */
    public TcpChannel(String serverHost, int serverPort, IReceiver receiver,
        FrameEncodingFormatEnum frameEncodingFormat, boolean writeToSocketUsingApplicationThread)
        throws ConnectException
    {
        this(serverHost, serverPort, receiver, TcpChannelProperties.Values.RX_BUFFER_SIZE, frameEncodingFormat,
            writeToSocketUsingApplicationThread);
    }

    /**
     * Construct a client {@link TcpChannel} that connects to a TCP server. This establishes a TCP
     * connection to the indicated host and port. After the constructor completes, the channel is
     * ready for use however the underlying {@link SocketChannel} may not yet be connected. This
     * does not stop the channel from being used; any data queued up to send via the
     * {@link #send(byte[])} method will be sent when the connection completes.
     * 
     * @param serverHost
     *            the target host for the TCP connection
     * @param port
     *            the target port for the TCP connection
     * @param receiver
     *            the object that will receive all the communication data from the target host
     * @param rxBufferSize
     *            the size of the receive buffer in bytes
     * @param frameEncodingFormat
     *            the enum for the frame format for this channel
     * @param writeToSocketUsingApplicationThread
     *            flag to indicate if frames are written to the TCP socket using the application
     *            thread calling {@link #send(byte[])} or by the TCP writer thread.
     * @throws ConnectException
     *             if the TCP connection could not be established
     */
    public TcpChannel(final String serverHost, final int serverPort, final IReceiver receiver, int rxBufferSize,
        FrameEncodingFormatEnum frameEncodingFormat, boolean writeToSocketUsingApplicationThread)
        throws ConnectException
    {
        this.onChannelClosedCalled = new AtomicBoolean();
        this.rxBytes = ByteBuffer.wrap(new byte[rxBufferSize]);
        this.byteArrayFragmentResolver = ByteArrayFragmentResolver.newInstance(frameEncodingFormat);
        this.writeToSocketUsingApplicationThread = writeToSocketUsingApplicationThread;
        this.receiver = receiver;
        this.reader = TcpChannelUtils.nextReader();
        this.writer = TcpChannelUtils.nextWriter();
        this.readerWriter = frameEncodingFormat.getFrameReaderWriter(this);
        this.endPointSocketDescription = serverHost + ":" + serverPort;
        this.socketChannel = TcpChannelUtils.createAndConnectNonBlockingSocketChannel(serverHost, serverPort);
        this.shortSocketDescription =
            this.socketChannel.socket().getLocalSocketAddress() + "->" + getEndPointDescription();
        finishConstruction();
    }

    /** Internally used constructor for server-side channels */
    TcpChannel(SocketChannel socketChannel, IReceiver receiver, int rxBufferSize,
        FrameEncodingFormatEnum frameEncodingFormat, boolean writeToSocketUsingApplicationThread)
        throws ConnectException
    {
        this.onChannelClosedCalled = new AtomicBoolean();
        this.socketChannel = socketChannel;
        this.rxBytes = ByteBuffer.wrap(new byte[rxBufferSize]);
        this.byteArrayFragmentResolver = ByteArrayFragmentResolver.newInstance(frameEncodingFormat);
        this.writeToSocketUsingApplicationThread = writeToSocketUsingApplicationThread;
        this.receiver = receiver;
        this.reader = TcpChannelUtils.nextReader();
        this.writer = TcpChannelUtils.nextWriter();
        this.readerWriter = frameEncodingFormat.getFrameReaderWriter(this);

        final Socket socket = this.socketChannel.socket();
        this.endPointSocketDescription = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
        this.shortSocketDescription = socket.getLocalSocketAddress() + "<-" + getEndPointDescription();

        finishConstruction();
    }

    private void finishConstruction() throws ConnectException
    {
        // this can be overridden by system properties
        try
        {
            this.socketChannel.socket().setTcpNoDelay(true);
        }
        catch (SocketException e1)
        {
            Log.log(TcpChannel.this, "Could not set TCP_NODELAY option on " + ObjectUtils.safeToString(this), e1);
        }

        TcpChannelUtils.setOptions(this.socketChannel);

        try
        {
            this.writer.register(this.socketChannel, new Runnable()
            {
                @Override
                public void run()
                {
                    writeFrames(TcpChannel.this.writer);
                }
            });

            this.writer.resetInterest(this.socketChannel);
        }
        catch (Exception e)
        {
            String message = this + " could not register for write operations";
            Log.log(this, message, e);
            throw new ConnectException(message);
        }

        try
        {
            this.reader.register(this.socketChannel, new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        readFrames();
                    }
                    catch (BufferOverflowException e)
                    {
                        TcpChannel.this.destroy("Buffer overflow during frame decode", e);
                        throw e;
                    }
                }
            });
        }
        catch (Exception e)
        {
            String message = this + " could not register for read operations";
            Log.log(this, message, e);
            throw new ConnectException(message);
        }

        ChannelUtils.WATCHDOG.addChannel(this);

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this));

    }

    @Override
    public boolean send(byte[] toSend)
    {
        try
        {
            final ByteBuffer[] byteFragmentsToSend =
                this.byteArrayFragmentResolver.getByteFragmentsToSend(toSend, TcpChannelProperties.Values.TX_SEND_SIZE);

            synchronized (this.lock)
            {
                ByteBuffer[] buffer;
                for (int i = 0; i < byteFragmentsToSend.length;)
                {
                    if (this.writeToSocketUsingApplicationThread)
                    {
                        ((AbstractFrameReaderWriter) this.readerWriter).writeNextFrame(byteFragmentsToSend[i++],
                            byteFragmentsToSend[i++]);
                    }
                    else
                    {
                        if (this.txFrameBufferPoolPtr < this.txFrameBufferPool.length)
                        {
                            buffer = this.txFrameBufferPool[this.txFrameBufferPoolPtr++];
                        }
                        else
                        {
                            buffer = new ByteBuffer[2];
                        }
                        buffer[0] = byteFragmentsToSend[i++];
                        buffer[1] = byteFragmentsToSend[i++];
                        this.txFrames.add(buffer);
                    }
                }

                if (this.writeToSocketUsingApplicationThread)
                {
                    return true;
                }

                switch(this.state)
                {
                    case DESTROYED:
                        throw new ClosedChannelException();
                    case IDLE:
                        this.state = StateEnum.SENDING;
                        linkChannel(this);
                        this.writer.setInterest(this.socketChannel);
                        break;
                    case SENDING:
                    default :
                        break;
                }

                if (TcpChannelProperties.Values.SEND_WAIT_FACTOR_MILLIS != 0)
                {
                    // NATURAL THROTTLE
                    try
                    {
                        // the tcp-writer will notify when the frames are empty
                        this.lock.wait((byteFragmentsToSend.length == 0 ? 1 : byteFragmentsToSend.length)
                            * TcpChannelProperties.Values.SEND_WAIT_FACTOR_MILLIS);
                    }
                    catch (InterruptedException e)
                    {
                        // don't care
                    }
                }
            }
            return true;
        }
        catch (Exception e)
        {
            destroy("Could not send data", e);
            return false;
        }
    }

    @Override
    @Deprecated
    public boolean sendAsync(byte[] toSend)
    {
        return send(toSend);
    }

    @Override
    public String toString()
    {
        return (this.onChannelConnectedCalled
            ? (this.onChannelClosedCalled.get() ? TCP_CHANNEL_CLOSED : TCP_CHANNEL_CONNECTED) : TCP_CHANNEL_PENDING)
            + getDescription() + "]";
    }

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        destroy("finalize");
    }

    void readFrames()
    {
        long start = System.nanoTime();
        long socketRead = 0;
        long connected = 0;
        long decodeFrames = 0;
        long resolveFrames = 0;
        long processFrames = 0;
        int size = 0;
        try
        {
            this.rxBytes.compact();

            final int readCount = this.socketChannel.read(this.rxBytes);

            switch(readCount)
            {
                case -1:
                    destroy("End-of-stream reached");
                    return;
                case 0:
                    return;
                default :
                    this.rxData++;
            }
            socketRead = System.nanoTime();

            // mark connected when we receive the first message (all channels send a heartbeat or
            // data as the first action they perform)
            if (!this.onChannelConnectedCalled)
            {
                this.onChannelConnectedCalled = true;
                Log.log(this, "Connected ", ObjectUtils.safeToString(this.socketChannel));
                try
                {
                    this.receiver.onChannelConnected(TcpChannel.this);
                }
                catch (Exception e)
                {
                    Log.log(this, ObjectUtils.safeToString(this) + " receiver "
                        + ObjectUtils.safeToString(this.receiver) + " threw exception during onChannelConnected", e);
                }
                connected = System.nanoTime();
            }

            this.readFrames = this.readerWriter.readFrames(this.rxBytes, this.readFrames, this.readFramesSize);
            decodeFrames = System.nanoTime();

            ByteBuffer frame;
            byte[] data;
            size = this.readFramesSize[0];
            int i = 0;
            int resolvedFramesSize = 0;
            for (i = 0; i < size; i++)
            {
                frame = this.readFrames[i];
                if ((data = this.byteArrayFragmentResolver.resolve(frame)) != null)
                {
                    if (resolvedFramesSize == this.resolvedFrames.length)
                    {
                        this.resolvedFrames = Arrays.copyOf(this.resolvedFrames, this.resolvedFrames.length + 2);
                    }
                    this.resolvedFrames[resolvedFramesSize++] = data;
                }
            }
            resolveFrames = System.nanoTime();

            for (i = 0; i < resolvedFramesSize; i++)
            {
                data = this.resolvedFrames[i];
                switch(data.length)
                {
                    case 1:
                        if (ChannelUtils.isHeartbeatSignal(data))
                        {
                            ChannelUtils.WATCHDOG.onHeartbeat(TcpChannel.this);
                            break;
                        }
                        //$FALL-THROUGH$
                    default :
                        try
                        {
                            this.receiver.onDataReceived(data, this);
                        }
                        catch (Exception e)
                        {
                            Log.log(this, ObjectUtils.safeToString(this) + " receiver "
                                + ObjectUtils.safeToString(this.receiver) + " threw exception during onDataReceived",
                                e);
                        }
                }
            }
            processFrames = System.nanoTime();
        }
        catch (IOException e)
        {
            destroy("Could not read from socket (" + e.toString() + ")");
        }
        finally
        {
            final long elapsedTimeNanos = System.nanoTime() - start;
            if (elapsedTimeNanos > Values.SLOW_RX_FRAME_THRESHOLD_NANOS)
            {
                if (connected == 0)
                {
                    Log.log(this, "*** SLOW RX FRAME HANDLING *** ", ObjectUtils.safeToString(this), " took ",
                        Long.toString(((long) (elapsedTimeNanos * _INVERSE_1000000))), "ms [socketRead=",
                        Long.toString(((long) ((socketRead - start) * _INVERSE_1000000))), "ms, decodeFrames(", Integer.toString(size), ")=",
                        Long.toString(((long) ((decodeFrames - socketRead) * _INVERSE_1000000))), "ms, resolveFrames=",
                        Long.toString(((long) ((resolveFrames - decodeFrames) * _INVERSE_1000000))), "ms, processFrames=",
                        Long.toString(((long) ((processFrames - resolveFrames) * _INVERSE_1000000))), "ms]");
                }
                else
                {
                    Log.log(this, "*** SLOW RX FRAME HANDLING *** ", ObjectUtils.safeToString(this), " took ",
                        Long.toString(((long) (elapsedTimeNanos * _INVERSE_1000000))), "ms [socketRead=",
                        Long.toString(((long) ((socketRead - start) * _INVERSE_1000000))), "ms, onChannelConnected=",
                        Long.toString(((long) ((connected - socketRead) * _INVERSE_1000000))), "ms, decodeFrames(", Integer.toString(size), ")=",
                        Long.toString(((long) ((decodeFrames - connected) * _INVERSE_1000000))), "ms, resolveFrames=",
                        Long.toString(((long) ((resolveFrames - decodeFrames) * _INVERSE_1000000))), "ms, processFrames=",
                        Long.toString(((long) ((processFrames - resolveFrames) * _INVERSE_1000000))), "ms]");
                }
            }
        }
    }

    static final void writeFrames(SelectorProcessor writer)
    {
        /*
         * This code logic will give equal opportunity for sending across all channels in the
         * runtime. This prevents one channel starving out other channels by having a queue that is
         * being fed as fast as it can be dequeued, which leads to starvation of other channels.
         */
        ByteBuffer[] data = null;
        TcpChannel channel = null;
        final SendChannelChain chain;
        synchronized (TcpChannel.class)
        {
            chain = sendChannelChains.get(writer);
        }
        // should never happen but do this to be defensive
        if (chain == null)
        {
            return;
        }
        while (chain.first != null)
        {
            if (channel == null)
            {
                channel = chain.first;
            }
            else
            {
                channel = channel.next;
            }
            if (channel == null)
            {
                break;
            }

            synchronized (channel.lock)
            {
                try
                {
                    if (channel.state == StateEnum.DESTROYED)
                    {
                        unlinkChannel(channel);
                        continue;
                    }
                    data = channel.txFrames.poll();
                    if (data != null)
                    {
                        try
                        {
                            ((AbstractFrameReaderWriter) channel.readerWriter).writeNextFrame(data[0], data[1]);
                            if (channel.txFrames.peek() == null)
                            {
                                setChannelIdle(channel);
                            }
                        }
                        catch (IOException e)
                        {
                            channel.destroy("Could not write frames (" + e.toString() + ")");
                        }
                        catch (Exception e)
                        {
                            channel.destroy("Could not write frames", e);
                        }
                        finally
                        {
                            data[0] = null;
                            data[1] = null;
                            if (channel.txFrameBufferPoolPtr > 0)
                            {
                                channel.txFrameBufferPool[--channel.txFrameBufferPoolPtr] = data;
                            }
                        }
                    }
                    else
                    {
                        setChannelIdle(channel);
                    }
                }
                finally
                {
                    if (TcpChannelProperties.Values.SEND_WAIT_FACTOR_MILLIS != 0)
                    {
                        channel.lock.notify();
                    }
                }
            }
        }
    }

    private static final void setChannelIdle(TcpChannel channel)
    {
        channel.state = StateEnum.IDLE;
        unlinkChannel(channel);
        try
        {
            channel.writer.resetInterest(channel.socketChannel);
        }
        catch (CancelledKeyException e)
        {
            channel.destroy("Socket has been closed", e);
        }
    }

    @Override
    public boolean isConnected()
    {
        try
        {
            return this.socketChannel.isConnected() && this.socketChannel.isOpen();
        }
        catch (Exception e)
        {
            Log.log(this, "Could not determine connected state", e);
            return false;
        }
    }

    @Override
    public void destroy(String reason, Exception... e)
    {
        if (this.onChannelClosedCalled.getAndSet(true))
        {
            return;
        }

        if (e == null || e.length == 0)
        {
            Log.log(this, reason, ", destroying ", ObjectUtils.safeToString(this));
        }
        else
        {
            Log.log(this, reason + ", destroying " + ObjectUtils.safeToString(this), e[0]);
        }

        try
        {
            synchronized (this.lock)
            {
                this.state = StateEnum.DESTROYED;
                this.txFrames.clear();
                this.rxBytes.clear();

                unlinkChannel(this);
            }

            if (this.socketChannel != null)
            {
                TcpChannelUtils.closeChannel(this.socketChannel);
                this.reader.cancel(this.socketChannel);
                this.writer.cancel(this.socketChannel);
            }

            this.receiver.onChannelClosed(this);
        }
        catch (Exception e1)
        {
            Log.log(this, "Could not destroy " + ObjectUtils.safeToString(this), e1);
        }
    }

    @Override
    public String getEndPointDescription()
    {
        return this.endPointSocketDescription;
    }

    @Override
    public String getDescription()
    {
        return this.shortSocketDescription;
    }

    @Override
    public boolean hasRxData()
    {
        final boolean hasRxData = this.rxData > 0;
        this.rxData = 0;
        return hasRxData;
    }

    @Override
    public int getTxQueueSize()
    {
        return this.txFrames.size();
    }
}

/**
 * Interface for the component that reads and writes transmission frames for a {@link TcpChannel}
 * 
 * @author Ramon Servadei
 */
interface IFrameReaderWriter
{
    /**
     * @param rxBytes
     *            the raw bytes read from a TCP socket
     * @param frames
     *            the reference to the buffer to use for holding the decoded frames read from the
     *            raw bytes
     * @param framesSize
     *            int[] to allow the result array size to be reported
     * @return the frame buffer with all the decoded frames
     */
    ByteBuffer[] readFrames(ByteBuffer rxBytes, ByteBuffer[] frames, int[] framesSize);
}

/**
 * Base class for {@link IFrameReaderWriter} implementations. This class handles partial writing of
 * data buffers to a socket. The {@link #writeBuffersToSocket()} method will continually attempt to
 * write the buffers to the socket until all data has been sent.
 * 
 * @author Ramon Servadei
 */
abstract class AbstractFrameReaderWriter implements IFrameReaderWriter
{
    int txLength;
    final ByteBuffer[] buffers;
    final TcpChannel tcpChannel;

    AbstractFrameReaderWriter(TcpChannel tcpChannel)
    {
        super();
        this.buffers = new ByteBuffer[3];
        this.tcpChannel = tcpChannel;
    }

    /**
     * Write the internal {@link #buffers} to the socket. This will continually try to write the
     * buffer to the socket until all the data has been written.
     * 
     * @throws IOException
     */
    void writeBuffersToSocket() throws IOException
    {
        while (this.buffers[0].hasRemaining() || this.buffers[1].hasRemaining())
        {
            this.tcpChannel.socketChannel.write(this.buffers);
        }
    }

    abstract void writeNextFrame(ByteBuffer header, ByteBuffer data) throws Exception;
}

/**
 * Handles frame encoding using a terminator field after each frame.
 * 
 * @see TcpChannelUtils#decodeUsingTerminator(List, ByteBuffer, byte[])
 * @author Ramon Servadei
 */
final class TerminatorBasedReaderWriter extends AbstractFrameReaderWriter
{
    private final ByteBuffer terminatorByteBuffer = ByteBuffer.wrap(TcpChannel.TERMINATOR);

    TerminatorBasedReaderWriter(TcpChannel tcpChannel)
    {
        super(tcpChannel);
        this.buffers[2] = this.terminatorByteBuffer;
    }

    @Override
    public ByteBuffer[] readFrames(ByteBuffer rxBytes, ByteBuffer[] frames, int[] framesSize)
    {
        return TcpChannelUtils.decodeUsingTerminator(frames, framesSize, rxBytes, TcpChannel.TERMINATOR);
    }

    @Override
    void writeNextFrame(ByteBuffer header, ByteBuffer data) throws IOException
    {
        final int frameLen = (header.limit() - header.position()) + (data.limit() - data.position());
        this.txLength = frameLen + TcpChannel.TERMINATOR.length;
        this.buffers[0] = header;
        this.buffers[1] = data;
        writeBuffersToSocket();
        this.terminatorByteBuffer.flip();
    }
}

/**
 * Handles frame encoding with a 4-byte length field before each frame.
 * 
 * @see TcpChannelUtils#decode(ByteBuffer)
 * @author Ramon Servadei
 */
final class LengthBasedWriter extends AbstractFrameReaderWriter
{
    private final ByteBuffer txLengthBuffer = ByteBuffer.wrap(new byte[4]);

    LengthBasedWriter(TcpChannel tcpChannel)
    {
        super(tcpChannel);
        this.buffers[0] = this.txLengthBuffer;
    }

    @Override
    public ByteBuffer[] readFrames(ByteBuffer rxBytes, ByteBuffer[] frames, int[] framesSize)
    {
        return TcpChannelUtils.decode(frames, framesSize, rxBytes);
    }

    @Override
    void writeNextFrame(ByteBuffer header, ByteBuffer data) throws IOException
    {
        final int frameLen = (header.limit() - header.position()) + (data.limit() - data.position());
        this.txLength = frameLen + 4;
        this.txLengthBuffer.putInt(0, frameLen);
        this.buffers[1] = header;
        this.buffers[2] = data;
        writeBuffersToSocket();
        this.txLengthBuffer.flip();
    }
}
