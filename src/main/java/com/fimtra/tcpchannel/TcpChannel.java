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
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpChannelProperties.Values;
import com.fimtra.tcpchannel.TcpChannelUtils.BufferOverflowException;
import com.fimtra.util.CollectionUtils;
import com.fimtra.util.Log;
import com.fimtra.util.LowGcLinkedList;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.ThreadUtils;

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
    private static final boolean TX_SEND_QUEUE_THRESHOLD_ACTIVE =
        TcpChannelProperties.Values.TX_SEND_QUEUE_THRESHOLD > 0;
    static final double _INVERSE_1000000 = 1 / 1000000d;

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

    /** The chain per writer - acces must be synchronized on the TcpChannel class */
    private final static Map<SelectorProcessor, SendChannelChain> sendChannelChains =
        new HashMap<SelectorProcessor, SendChannelChain>();

    /**
     * Add the channel into the chain. If it already there, does nothing.
     * 
     * @param channel
     *            the channel to add
     * @return <code>true</code> if this is the first channel to be added into the chain - and thus
     *         the {@link SelectorProcessor} needs to be triggered for writing
     */
    private synchronized static final boolean linkChannel(TcpChannel channel)
    {
        final SendChannelChain chain = channel.sendChannelChain;
        if (channel.prev == null && channel.next == null && chain.first != channel)
        {
            if (chain.first == null)
            {
                chain.first = channel;
                chain.last = channel;
                channel.prev = null;
                channel.next = null;
                return true;
            }
            else
            {
                chain.last.next = channel;
                channel.prev = chain.last;
                channel.next = null;
                chain.last = channel;
                return false;
            }
        }
        return false;
    }

    private synchronized static final void unlinkChannel(TcpChannel channel)
    {
        final SendChannelChain chain = channel.sendChannelChain;
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
    volatile int rxData;
    final IReceiver receiver;
    final ByteBuffer rxByteBuffer;
    final byte[] rxBytes;
    ByteBuffer[] readFrames = new ByteBuffer[10];
    ByteBuffer[] resolvedFrames = new ByteBuffer[10];
    final int[] readFramesSize = new int[1];
    final Queue<TxByteArrayFragment> txFrames = CollectionUtils.newDeque();
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
    private final Lock lock = new ReentrantLock();
    private final Condition signal = this.lock.newCondition();

    private final boolean writeToSocketUsingApplicationThread;

    /** Counter to indicate if a send operation expects to be signalled when sending is complete */
    volatile int waitingForNotify;

    StateEnum state = StateEnum.IDLE;

    SelectionKey writerKey;
    final SendChannelChain sendChannelChain;

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
        this.rxByteBuffer = ByteBuffer.wrap(new byte[rxBufferSize]);
        this.rxBytes = this.rxByteBuffer.array();
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

        SendChannelChain chain;
        synchronized (TcpChannel.class)
        {
            chain = sendChannelChains.get(this.writer);
            if (chain == null)
            {
                chain = new SendChannelChain();
                sendChannelChains.put(this.writer, chain);
            }
        }
        this.sendChannelChain = chain;

        finishConstruction();
    }

    /** Internally used constructor for server-side channels */
    TcpChannel(SocketChannel socketChannel, IReceiver receiver, int rxBufferSize,
        FrameEncodingFormatEnum frameEncodingFormat, boolean writeToSocketUsingApplicationThread)
        throws ConnectException
    {
        this.onChannelClosedCalled = new AtomicBoolean();
        this.socketChannel = socketChannel;
        this.rxByteBuffer = ByteBuffer.wrap(new byte[rxBufferSize]);
        this.rxBytes = this.rxByteBuffer.array();
        this.byteArrayFragmentResolver = ByteArrayFragmentResolver.newInstance(frameEncodingFormat);
        this.writeToSocketUsingApplicationThread = writeToSocketUsingApplicationThread;
        this.receiver = receiver;
        this.reader = TcpChannelUtils.nextReader();
        this.writer = TcpChannelUtils.nextWriter();
        this.readerWriter = frameEncodingFormat.getFrameReaderWriter(this);
        final Socket socket = this.socketChannel.socket();
        this.endPointSocketDescription = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
        this.shortSocketDescription = socket.getLocalSocketAddress() + "<-" + getEndPointDescription();

        SendChannelChain chain;
        synchronized (TcpChannel.class)
        {
            chain = sendChannelChains.get(this.writer);
            if (chain == null)
            {
                chain = new SendChannelChain();
                sendChannelChains.put(this.writer, chain);
            }
        }
        this.sendChannelChain = chain;

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

        if (!this.writeToSocketUsingApplicationThread)
        {
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

                this.writerKey = this.writer.getKeyFor(this.socketChannel);
                SelectorProcessor.resetInterest(this.writerKey);
            }
            catch (Exception e)
            {
                String message = this + " could not register for write operations";
                Log.log(this, message, e);
                throw new ConnectException(message);
            }
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

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this),
            this.socketChannel.isBlocking() ? " blocking mode" : " non-blocking mode");
    }
    
    @Override
    public boolean send(byte[] toSend)
    {
        long start = System.nanoTime();
        try
        {
            final TxByteArrayFragment[] byteFragmentsToSend =
                TxByteArrayFragment.getFragmentsForTxData(toSend, TcpChannelProperties.Values.TX_SEND_SIZE);
            final ByteBuffer[][] buffers = new ByteBuffer[byteFragmentsToSend.length][];
            for (int i = 0; i < byteFragmentsToSend.length; i++)
            {
                buffers[i] = this.byteArrayFragmentResolver.prepareBuffersToSend(byteFragmentsToSend[i]);
            }
            
            this.lock.lock();
            try
            {
                if (this.writeToSocketUsingApplicationThread)
                {
                    for (int i = 0; i < buffers.length; i++)
                    {
                        ((AbstractFrameReaderWriter) this.readerWriter).writeNextFrame(buffers[i][0], buffers[i][1]);
                        byteFragmentsToSend[i].free();
                    }
                }
                else
                {
                    for (int i = 0; i < byteFragmentsToSend.length; i++)
                    {
                        this.txFrames.add(byteFragmentsToSend[i]);
                    }

                    switch(this.state)
                    {
                        case DESTROYED:
                            throw new ClosedChannelException();
                        case IDLE:
                            this.state = StateEnum.SENDING;
                            linkChannel(this);
                            this.writer.setInterest(this.writerKey);
                            break;
                        case SENDING:
                        default :
                            break;
                    }

                    if (TX_SEND_QUEUE_THRESHOLD_ACTIVE
                        && this.txFrames.size() > TcpChannelProperties.Values.TX_SEND_QUEUE_THRESHOLD)
                    {
                        this.waitingForNotify++;
                        // the tcp-writer will notify after next send
                        this.signal.await(10, TimeUnit.MILLISECONDS);
                        this.waitingForNotify--;
                    }
                }

            }
            finally
            {
                this.lock.unlock();
            }
            return true;
        }
        catch (Exception e)
        {
            destroy("Could not send data", e);
            return false;
        }
        finally
        {
            start = System.nanoTime() - start;
            if (start > Values.SLOW_TX_FRAME_THRESHOLD_NANOS)
            {
                Log.log(this, "SLOW SEND: ", this.toString(), " took ",
                    Long.toString((long) (start * _INVERSE_1000000)), "ms");
            }
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
            this.rxByteBuffer.compact();

            final int readCount = this.socketChannel.read(this.rxByteBuffer);

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

            this.readFrames =
                this.readerWriter.readFrames(this.rxByteBuffer, this.rxBytes, this.readFrames, this.readFramesSize);
            decodeFrames = System.nanoTime();

            ByteBuffer data;
            size = this.readFramesSize[0];
            int i = 0;
            int resolvedFramesSize = 0;
            for (i = 0; i < size; i++)
            {
                if ((data = this.byteArrayFragmentResolver.resolve(this.readFrames[i])) != null)
                {
                    if (resolvedFramesSize == this.resolvedFrames.length)
                    {
                        this.resolvedFrames = Arrays.copyOf(this.resolvedFrames, this.resolvedFrames.length + 2);
                    }
                    this.resolvedFrames[resolvedFramesSize++] = data;
                }
                this.readFrames[i] = null;
            }
            resolveFrames = System.nanoTime();

            for (i = 0; i < resolvedFramesSize; i++)
            {
                // NOTE: we know that all ByteBuffers coming in here have position=0
                // (because they come from ByteArrayFragment.getData()
                // so limit() is the length
                switch(this.resolvedFrames[i].limit())
                {
                    case 1:
                        if (ChannelUtils.HEARTBEAT_SIGNAL[0] == this.resolvedFrames[i].get(0))
                        {
                            ChannelUtils.WATCHDOG.onHeartbeat(TcpChannel.this);
                            break;
                        }
                        //$FALL-THROUGH$
                    default :
                        try
                        {
                            this.receiver.onDataReceived(this.resolvedFrames[i], this);
                        }
                        catch (Exception e)
                        {
                            Log.log(this, ObjectUtils.safeToString(this) + " receiver "
                                + ObjectUtils.safeToString(this.receiver) + " threw exception during onDataReceived",
                                e);
                        }
                }
                this.resolvedFrames[i] = null;
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
                        Long.toString(((long) ((socketRead - start) * _INVERSE_1000000))), "ms, decodeFrames(",
                        Integer.toString(size), ")=",
                        Long.toString(((long) ((decodeFrames - socketRead) * _INVERSE_1000000))), "ms, resolveFrames=",
                        Long.toString(((long) ((resolveFrames - decodeFrames) * _INVERSE_1000000))),
                        "ms, processFrames=",
                        Long.toString(((long) ((processFrames - resolveFrames) * _INVERSE_1000000))), "ms]");
                }
                else
                {
                    Log.log(this, "*** SLOW RX FRAME HANDLING *** ", ObjectUtils.safeToString(this), " took ",
                        Long.toString(((long) (elapsedTimeNanos * _INVERSE_1000000))), "ms [socketRead=",
                        Long.toString(((long) ((socketRead - start) * _INVERSE_1000000))), "ms, onChannelConnected=",
                        Long.toString(((long) ((connected - socketRead) * _INVERSE_1000000))), "ms, decodeFrames(",
                        Integer.toString(size), ")=",
                        Long.toString(((long) ((decodeFrames - connected) * _INVERSE_1000000))), "ms, resolveFrames=",
                        Long.toString(((long) ((resolveFrames - decodeFrames) * _INVERSE_1000000))),
                        "ms, processFrames=",
                        Long.toString(((long) ((processFrames - resolveFrames) * _INVERSE_1000000))), "ms]");
                }
            }
        }
    }

    @SuppressWarnings("null")
    static final void writeFrames(SelectorProcessor writer)
    {
        /*
         * This code logic will give equal opportunity for sending across all channels in the
         * runtime. This prevents one channel starving out other channels by having a queue that is
         * being fed as fast as it can be dequeued, which leads to starvation of other channels.
         */
        TxByteArrayFragment data = null;
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

        final Map<TcpChannel, Deque<TxByteArrayFragment>> dataListPerChannel =
            new HashMap<TcpChannel, Deque<TxByteArrayFragment>>();
        Deque<TxByteArrayFragment> dataList;
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
                continue;
            }

            try
            {
                // minimise locking by checking what we have grabbed previously, only if its empty
                // do we go into here
                dataList = dataListPerChannel.get(channel);
                if (dataList == null)
                {
                    dataList = new LowGcLinkedList<TxByteArrayFragment>();
                    dataListPerChannel.put(channel, dataList);
                }
                if (dataList.size() == 0)
                {
                    channel.lock.lock();
                    try
                    {
                        if (channel.state == StateEnum.DESTROYED)
                        {
                            unlinkChannel(channel);
                            continue;
                        }
                        while ((data = channel.txFrames.poll()) != null)
                        {
                            dataList.add(data);
                        }
                        if (dataList.size() == 0)
                        {
                            setChannelIdle(channel);
                        }
                    }
                    finally
                    {
                        channel.lock.unlock();
                    }
                }

                if (dataList.size() != 0)
                {
                    try
                    {
                        data = dataList.poll();
                        ((AbstractFrameReaderWriter) channel.readerWriter).writeNextFrame(data.txDataWithHeader[0],
                            data.txDataWithHeader[1]);
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
                        data.free();
                    }
                }
            }
            finally
            {
                if (channel.waitingForNotify > 0)
                {
                    channel.lock.lock();
                    try
                    {
                        channel.signal.signalAll();
                    }
                    finally
                    {
                        channel.lock.unlock();
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
            SelectorProcessor.resetInterest(channel.writerKey);
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
            this.lock.lock();
            try
            {
                this.state = StateEnum.DESTROYED;
                this.txFrames.clear();
                this.rxByteBuffer.clear();

                unlinkChannel(this);
            }
            finally
            {
                this.lock.unlock();
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
     * @param rxByteBuffer
     *            the byte buffer read from a TCP socket
     * @param rxBytes
     *            the raw bytes extracted from the buffer
     * @param frames
     *            the reference to the buffer to use for holding the decoded frames read from the
     *            raw bytes
     * @param framesSize
     *            int[] to allow the result array size to be reported
     * @return the frame buffer with all the decoded frames
     */
    ByteBuffer[] readFrames(ByteBuffer rxByteBuffer, byte[] rxBytes, ByteBuffer[] frames, int[] framesSize);
}

/**
 * Base class for {@link IFrameReaderWriter} implementations. This class handles partial writing of
 * data buffers to a socket. The {@link #writeBufferToSocket()} method will continually attempt to
 * write the buffers to the socket until all data has been sent.
 * 
 * @author Ramon Servadei
 */
abstract class AbstractFrameReaderWriter implements IFrameReaderWriter
{
    static final long SLOW_FRAME_WRITE_TIME_NANOS = 10000000;
    
    // note: the header will never by this large, but we just use it to ensure we have enough so the
    // txBuffer never resizes
    private static final int HEADER_SPACE = 1024;
    

    final TcpChannel tcpChannel;
    final ByteBuffer txBuffer = ByteBuffer.allocateDirect(TcpChannelProperties.Values.TX_SEND_SIZE + HEADER_SPACE);
    AbstractFrameReaderWriter(TcpChannel tcpChannel)
    {
        super();
        this.tcpChannel = tcpChannel;
    }

    /**
     * Write the internal {@link #buffers} to the socket. This will continually try to write the
     * buffer to the socket until all the data has been written.
     * 
     * @throws IOException
     */
    void writeBufferToSocket() throws IOException
    {
        long time = System.nanoTime();
        this.txBuffer.flip();
        try
        {
            int zeroBytesWritten = 0;
            while (this.txBuffer.hasRemaining())
            {
                if (this.tcpChannel.socketChannel.write(this.txBuffer) == 0)
                {
                    if (zeroBytesWritten++ % 10 == 0)
                    {
                        Log.log(this, "Wrote 0 bytes, attempts=" + zeroBytesWritten);
                        ThreadUtils.sleep(10);
                    }
                }
            }
        }
        finally
        {
            this.txBuffer.compact();
        }
        time = System.nanoTime() - time;
        if (time > Values.SLOW_TX_FRAME_THRESHOLD_NANOS)
        {
            Log.log(this.tcpChannel, "SLOW FRAME WRITE: ", this.tcpChannel.toString(), " took ",
                Long.toString((long) (time * TcpChannel._INVERSE_1000000)), "ms");
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
    TerminatorBasedReaderWriter(TcpChannel tcpChannel)
    {
        super(tcpChannel);
    }

    @Override
    public ByteBuffer[] readFrames(ByteBuffer rxByteBuffer, byte[] rxBytes, ByteBuffer[] frames, int[] framesSize)
    {
        return TcpChannelUtils.decodeUsingTerminator(frames, framesSize, rxByteBuffer, rxBytes, TcpChannel.TERMINATOR);
    }

    @Override
    void writeNextFrame(ByteBuffer header, ByteBuffer data) throws IOException
    {
        final int dataLen = data.limit() - data.position();
        final int headerLen = header.limit() - header.position();

        this.txBuffer.put(header.array(), header.position(), headerLen);
        this.txBuffer.put(data.array(), data.position(), dataLen);
        this.txBuffer.put(TcpChannel.TERMINATOR);

        writeBufferToSocket();
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
    LengthBasedWriter(TcpChannel tcpChannel)
    {
        super(tcpChannel);
    }

    @Override
    public ByteBuffer[] readFrames(ByteBuffer rxByteBuffer, byte[] rxBytes, ByteBuffer[] frames, int[] framesSize)
    {
        return TcpChannelUtils.decode(frames, framesSize, rxByteBuffer, rxBytes);
    }

    @Override
    void writeNextFrame(ByteBuffer header, ByteBuffer data) throws IOException
    {        
        final int dataLen = data.limit() - data.position();
        final int headerLen = header.limit() - header.position();

        this.txBuffer.putInt(headerLen + dataLen);
        this.txBuffer.put(header.array(), header.position(), headerLen);
        this.txBuffer.put(data.array(), data.position(), dataLen);

        writeBufferToSocket();
    }
}
