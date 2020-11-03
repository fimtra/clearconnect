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

import static com.fimtra.tcpchannel.TcpChannelProperties.Values.FRAME_ENCODING;
import static com.fimtra.tcpchannel.TcpChannelProperties.Values.RX_BUFFER_SIZE;
import static com.fimtra.tcpchannel.TcpChannelProperties.Values.SEND_QUEUE_THRESHOLD;
import static com.fimtra.tcpchannel.TcpChannelProperties.Values.SEND_QUEUE_THRESHOLD_BREACH_MILLIS;
import static com.fimtra.tcpchannel.TcpChannelProperties.Values.SLOW_RX_FRAME_THRESHOLD_NANOS;
import static com.fimtra.tcpchannel.TcpChannelProperties.Values.SLOW_TX_FRAME_THRESHOLD_NANOS;
import static com.fimtra.tcpchannel.TcpChannelProperties.Values.TX_SEND_SIZE;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpChannelUtils.BufferOverflowException;
import com.fimtra.thimble.ContextExecutorFactory;
import com.fimtra.thimble.ICoalescingRunnable;
import com.fimtra.thimble.IContextExecutor;
import com.fimtra.thimble.ISequentialRunnable;
import com.fimtra.util.CollectionUtils;
import com.fimtra.util.Log;
import com.fimtra.util.MultiThreadReusableObjectPool;
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
    private static final boolean TX_SEND_QUEUE_THRESHOLD_ACTIVE = SEND_QUEUE_THRESHOLD > 0;

    static final double _INVERSE_1000000 = 1 / 1000000d;
    private static final Deque<TxByteArrayFragment> NOOP_DEQUE = CollectionUtils.noopDeque();

    /**
     * Expresses the encoding format for the data frames
     */
    public enum FrameEncodingFormatEnum
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
         * @param tcpChannel the channel to create a frame reader-writer for
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
                default:
                    throw new IllegalStateException("No support for " + this);
            }
        }
    }

    private enum StateEnum
    {
        DESTROYED, IDLE, SENDING
    }

    private static final String TCP_CHANNEL_CLOSED = "TcpChannel [closed ";
    private static final String TCP_CHANNEL_PENDING = "TcpChannel [pending ";
    private static final String TCP_CHANNEL_CONNECTED = "TcpChannel [connected ";

    /**
     * Represents the ASCII code for CRLF
     */
    static final byte[] TERMINATOR = { 0xd, 0xa };

    /**
     * Re-usable class for resolving frames read from the socket. Has an internal buffer for reading
     * from the socket and logic to resolve a frame from the fragments received from the socket
     * buffer.
     *
     * @author Ramon Servadei
     */
    private static final class RxFrameResolver implements ISequentialRunnable
    {
        /**
         * direct byte buffer to optimise reading
         */
        final ByteBuffer buffer = ByteBuffer.allocateDirect(AbstractFrameReaderWriter.BUFFER_SIZE);
        long socketRead;
        TcpChannel channel;

        RxFrameResolver()
        {
        }

        @Override
        public void run()
        {
            try
            {
                final long start = System.nanoTime();
                final int readCount = this.channel.socketChannel.read(this.buffer);
                switch(readCount)
                {
                    case -1:
                        this.channel.destroy("End-of-stream reached");
                        return;
                    case 0:
                        return;
                    default:
                        this.channel.rxData = true;
                }

                this.socketRead = System.nanoTime() - start;

                this.channel.resolveFrameFromBuffer(this.socketRead, this.buffer);
            }
            catch (IOException e)
            {
                this.channel.destroy("Could not read from socket (" + e.toString() + ")");
            }
            catch (BufferOverflowException e)
            {
                this.channel.destroy("Buffer overflow during frame decode", e);
            }
            finally
            {
                RX_FRAME_RESOLVER_POOL.offer(this);
            }
        }

        @Override
        public Object context()
        {
            return this.channel;
        }
    }

    /**
     * Exclusively used to handle frames dispatched from the TCP socket reads.
     */
    private static final IContextExecutor RX_FRAME_PROCESSOR =
            ContextExecutorFactory.create("rxframe-processor",
                    TcpChannelProperties.Values.READER_THREAD_COUNT);

    static final MultiThreadReusableObjectPool<RxFrameResolver> RX_FRAME_RESOLVER_POOL =
            new MultiThreadReusableObjectPool<>("RxFrameResolverPool", RxFrameResolver::new, (instance) -> {
                instance.buffer.clear();
                instance.channel = null;
            }, TcpChannelProperties.Values.RX_FRAME_RESOLVER_POOL_MAX_SIZE);

    private static final IContextExecutor TX_FRAME_PROCESSOR =
            ContextExecutorFactory.create("txframe-processor",
                    TcpChannelProperties.Values.WRITER_THREAD_COUNT);

    final SelectorProcessor reader;
    boolean rxData;
    final IReceiver receiver;
    final ByteBuffer rxByteBuffer;
    final byte[] rxBytes;
    ByteBuffer[] fragments = new ByteBuffer[10];
    ByteBuffer[] resolvedFrames = new ByteBuffer[10];
    final int[] fragmentsSize = new int[1];
    @SuppressWarnings("unchecked")
    final Deque<TxByteArrayFragment> txFrames[] =
            new Deque[] { CollectionUtils.newDeque(), CollectionUtils.newDeque() };
    int pendingQueue = 0;
    int sendingQueue = 1;
    final SocketChannel socketChannel;
    final IFrameReaderWriter readerWriter;
    final ByteArrayFragmentResolver byteArrayFragmentResolver;
    /**
     * The details of the end-point socket connection
     */
    final String endPointSocketDescription;
    /**
     * The short-hand description of the end-point connections
     */
    final String shortSocketDescription;
    /**
     * Tracks if the {@link IReceiver#onChannelConnected(ITransportChannel)} has been called. Ensures its
     * only called once.
     */
    private boolean onChannelConnectedCalled;
    /**
     * Tracks if the {@link IReceiver#onChannelClosed(ITransportChannel)} has been called. Ensures its
     * only called once.
     */
    private final AtomicBoolean onChannelClosedCalled;
    private final Object lock = new Object();

    /**
     * The channel state - always access using the {@link #lock}
     */
    volatile StateEnum state = StateEnum.IDLE;

    SelectionKey writerKey;
    final QueueThresholdMonitor sendQueueMonitor;

    final ICoalescingRunnable socketWriteTask = new ICoalescingRunnable()
    {
        @Override
        public Object context()
        {
            return TcpChannel.this;
        }

        @Override
        public void run()
        {
            try
            {
                writeFrameToSocket();
            }
            catch (Exception e)
            {
                destroy("Could not write frames to socket", e);
            }
        }
    };

    /**
     * Construct a {@link TcpChannel} with a default receive buffer size and default frame encoding
     * format.
     *
     * @see TcpChannelProperties.Values#FRAME_ENCODING
     * @see TcpChannelProperties.Values#RX_BUFFER_SIZE
     * @see #TcpChannel(String, int, IReceiver, int, FrameEncodingFormatEnum)
     */
    public TcpChannel(String serverHost, int serverPort, IReceiver receiver) throws ConnectException
    {
        this(serverHost, serverPort, receiver, RX_BUFFER_SIZE);
    }

    /**
     * Construct a {@link TcpChannel} with a specific receive buffer size and default frame encoding
     * format.
     *
     * @see TcpChannelProperties.Values#FRAME_ENCODING
     * @see #TcpChannel(String, int, IReceiver, int, FrameEncodingFormatEnum)
     */
    public TcpChannel(String serverHost, int serverPort, IReceiver receiver, int rxBufferSize)
            throws ConnectException
    {
        this(serverHost, serverPort, receiver, rxBufferSize, FRAME_ENCODING);
    }

    /**
     * Construct a {@link TcpChannel} with a default receive buffer size and specific frame encoding
     * format.
     *
     * @see TcpChannelProperties.Values#RX_BUFFER_SIZE
     * @see #TcpChannel(String, int, IReceiver, int, FrameEncodingFormatEnum)
     */
    public TcpChannel(String serverHost, int serverPort, IReceiver receiver,
            FrameEncodingFormatEnum frameEncodingFormat) throws ConnectException
    {
        this(serverHost, serverPort, receiver, RX_BUFFER_SIZE, frameEncodingFormat);
    }

    /**
     * Construct a client {@link TcpChannel} that connects to a TCP server. This establishes a TCP
     * connection to the indicated host and port. After the constructor completes, the channel is
     * ready for use however the underlying {@link SocketChannel} may not yet be connected. This
     * does not stop the channel from being used; any data queued up to send via the
     * {@link #send(byte[])} method will be sent when the connection completes.
     *
     * @param serverHost          the target host for the TCP connection
     * @param serverPort          the target port for the TCP connection
     * @param receiver            the object that will receive all the communication data from the target host
     * @param rxBufferSize        the size of the receive buffer in bytes
     * @param frameEncodingFormat the enum for the frame format for this channel
     * @throws ConnectException if the TCP connection could not be established
     */
    public TcpChannel(final String serverHost, final int serverPort, final IReceiver receiver,
            int rxBufferSize, FrameEncodingFormatEnum frameEncodingFormat) throws ConnectException
    {
        this.onChannelClosedCalled = new AtomicBoolean();
        this.rxByteBuffer = ByteBuffer.wrap(new byte[rxBufferSize]);
        this.rxBytes = this.rxByteBuffer.array();
        this.byteArrayFragmentResolver = ByteArrayFragmentResolver.newInstance(frameEncodingFormat);
        this.receiver = receiver;
        this.reader = TcpChannelUtils.nextReader();
        this.readerWriter = frameEncodingFormat.getFrameReaderWriter(this);
        this.endPointSocketDescription = serverHost + ":" + serverPort;
        this.socketChannel = TcpChannelUtils.createAndConnectNonBlockingSocketChannel(serverHost, serverPort);
        this.shortSocketDescription =
                this.socketChannel.socket().getLocalSocketAddress() + "->" + getEndPointDescription();
        this.sendQueueMonitor = new QueueThresholdMonitor(this, SEND_QUEUE_THRESHOLD_BREACH_MILLIS);

        finishConstruction();
    }

    /**
     * Internally used constructor for server-side channels
     */
    TcpChannel(SocketChannel socketChannel, IReceiver receiver, int rxBufferSize,
            FrameEncodingFormatEnum frameEncodingFormat) throws ConnectException
    {
        this.onChannelClosedCalled = new AtomicBoolean();
        this.socketChannel = socketChannel;
        this.rxByteBuffer = ByteBuffer.wrap(new byte[rxBufferSize]);
        this.rxBytes = this.rxByteBuffer.array();
        this.byteArrayFragmentResolver = ByteArrayFragmentResolver.newInstance(frameEncodingFormat);
        this.receiver = receiver;
        this.reader = TcpChannelUtils.nextReader();
        this.readerWriter = frameEncodingFormat.getFrameReaderWriter(this);
        final Socket socket = this.socketChannel.socket();
        this.endPointSocketDescription = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
        this.shortSocketDescription = socket.getLocalSocketAddress() + "<-" + getEndPointDescription();
        this.sendQueueMonitor = new QueueThresholdMonitor(this, SEND_QUEUE_THRESHOLD_BREACH_MILLIS);

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
            Log.log(TcpChannel.this, "Could not set TCP_NODELAY option on " + ObjectUtils.safeToString(this),
                    e1);
        }

        TcpChannelUtils.setOptions(this.socketChannel);

        try
        {
            this.reader.register(this.socketChannel, this::readFrames);
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
        try
        {
            if (this.state == StateEnum.DESTROYED)
            {
                throw new ClosedChannelException();
            }

            if (TX_SEND_QUEUE_THRESHOLD_ACTIVE)
            {
                if (this.sendQueueMonitor.checkQueueSize(this.txFrames[0], this.txFrames[1]))
                {
                    throw new Exception("Queue too large");
                }
            }

            final TxByteArrayFragment[] byteFragmentsToSend =
                    TxByteArrayFragment.getFragmentsForTxData(toSend, TX_SEND_SIZE);

            for (int i = 0; i < byteFragmentsToSend.length; i++)
            {
                this.byteArrayFragmentResolver.prepareBuffersToSend(byteFragmentsToSend[i]);
            }

            synchronized (this.lock)
            {
                final Deque<TxByteArrayFragment> pendingTxFrames = this.txFrames[this.pendingQueue];
                for (int i = 0; i < byteFragmentsToSend.length; i++)
                {
                    pendingTxFrames.offer(byteFragmentsToSend[i]);
                }

                switch(this.state)
                {
                    case DESTROYED:
                        throw new ClosedChannelException();
                    case IDLE:
                        this.state = StateEnum.SENDING;
                        TX_FRAME_PROCESSOR.execute(this.socketWriteTask);
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
        final StringBuilder sb = new StringBuilder(80);
        sb.append(this.onChannelConnectedCalled ?
                (this.onChannelClosedCalled.get() ? TCP_CHANNEL_CLOSED : TCP_CHANNEL_CONNECTED) :
                TCP_CHANNEL_PENDING).append(getDescription()).append("]");
        return sb.toString();
    }

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        destroy("finalize");
    }

    void readFrames()
    {
        final RxFrameResolver frameResolver = RX_FRAME_RESOLVER_POOL.get();
        frameResolver.channel = this;
        RX_FRAME_PROCESSOR.execute(frameResolver);
    }

    void resolveFrameFromBuffer(long socketRead, ByteBuffer buffer)
    {
        long connected = 0;
        long decodeFrames = 0;
        long resolveFrames = 0;
        long processFrames = 0;
        int size = 0;

        final long start = System.nanoTime();

        // prepare the main buffer
        this.rxByteBuffer.compact();
        if (this.rxByteBuffer.position() == this.rxByteBuffer.limit())
        {
            this.rxByteBuffer.clear();
        }

        // read the input buffer into the main buffer
        buffer.flip();
        this.rxByteBuffer.put(buffer);

        try
        {
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
                    Log.log(this, ObjectUtils.safeToString(this) + " receiver " + ObjectUtils.safeToString(
                            this.receiver) + " threw exception during onChannelConnected", e);
                }
                connected = System.nanoTime();
            }

            this.fragments = this.readerWriter.readFrames(this.rxByteBuffer, this.rxBytes, this.fragments,
                    this.fragmentsSize);
            decodeFrames = System.nanoTime();

            ByteBuffer data;
            size = this.fragmentsSize[0];
            int i;
            int resolvedFramesSize = 0;
            for (i = 0; i < size; i++)
            {
                if ((data = this.byteArrayFragmentResolver.resolve(this.fragments[i])) != null)
                {
                    if (resolvedFramesSize == this.resolvedFrames.length)
                    {
                        this.resolvedFrames =
                                Arrays.copyOf(this.resolvedFrames, this.resolvedFrames.length + 2);
                    }
                    this.resolvedFrames[resolvedFramesSize++] = data;
                }
                this.fragments[i] = null;
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
                    default:
                        try
                        {
                            this.receiver.onDataReceived(this.resolvedFrames[i], this);
                        }
                        catch (Exception e)
                        {
                            Log.log(this,
                                    ObjectUtils.safeToString(this) + " receiver " + ObjectUtils.safeToString(
                                            this.receiver) + " threw exception during onDataReceived", e);
                        }
                }
                this.resolvedFrames[i] = null;
            }
            processFrames = System.nanoTime();
        }
        catch (Exception e)
        {
            destroy("Could not read from socket (" + e.toString() + ")");
        }
        finally
        {
            final long elapsedTimeNanos = System.nanoTime() - start + socketRead;
            if (elapsedTimeNanos > SLOW_RX_FRAME_THRESHOLD_NANOS)
            {
                if (connected == 0)
                {
                    Log.log(this, "SLOW FRAME: ",
                            Long.toString(((long) (elapsedTimeNanos * _INVERSE_1000000))), "ms [socketRead=",
                            Long.toString(((long) ((socketRead) * _INVERSE_1000000))), "ms, decode(",
                            Integer.toString(size), ")=",
                            Long.toString(((long) ((decodeFrames - start) * _INVERSE_1000000))),
                            "ms, resolve=",
                            Long.toString(((long) ((resolveFrames - decodeFrames) * _INVERSE_1000000))),
                            "ms, process=",
                            Long.toString(((long) ((processFrames - resolveFrames) * _INVERSE_1000000))),
                            "ms] for ", ObjectUtils.safeToString(this));
                }
                else
                {
                    Log.log(this, "SLOW FRAME: ",
                            Long.toString(((long) (elapsedTimeNanos * _INVERSE_1000000))), "ms [socketRead=",
                            Long.toString(((long) ((socketRead) * _INVERSE_1000000))),
                            "ms, onChannelConnected=",
                            Long.toString(((long) ((connected - start) * _INVERSE_1000000))), "ms, decode(",
                            Integer.toString(size), ")=",
                            Long.toString(((long) ((decodeFrames - connected) * _INVERSE_1000000))),
                            "ms, resolve=",
                            Long.toString(((long) ((resolveFrames - decodeFrames) * _INVERSE_1000000))),
                            "ms, process=",
                            Long.toString(((long) ((processFrames - resolveFrames) * _INVERSE_1000000))),
                            "ms] for ", ObjectUtils.safeToString(this));
                }
            }
        }
    }

    void writeFrameToSocket() throws Exception
    {
        final AbstractFrameReaderWriter readerWriter = (AbstractFrameReaderWriter) this.readerWriter;
        // did a previous write fail to complete...
        if (readerWriter.writeInProgress)
        {
            readerWriter.writeBufferToSocket();
        }
        else
        {
            final TxByteArrayFragment data;
            synchronized (this.lock)
            {
                if (this.txFrames[this.sendingQueue].size() == 0)
                {
                    if (this.state != StateEnum.DESTROYED)
                    {
                        // swap txFrames
                        final int temp = this.sendingQueue;
                        this.sendingQueue = this.pendingQueue;
                        this.pendingQueue = temp;
                    }
                }
                if ((data = this.txFrames[this.sendingQueue].poll()) == null)
                {
                    this.state = StateEnum.IDLE;
                    return;
                }
            }
            try
            {
                readerWriter.writeNextFrame(data.txDataWithHeader[0], data.txDataWithHeader[1]);
            }
            finally
            {
                data.free();
            }
        }

        if (this.state == StateEnum.SENDING)
        {
            TX_FRAME_PROCESSOR.execute(this.socketWriteTask);
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
                this.txFrames[this.pendingQueue].clear();
                this.txFrames[this.sendingQueue].clear();
                this.txFrames[this.pendingQueue] = NOOP_DEQUE;
                this.txFrames[this.sendingQueue] = NOOP_DEQUE;
                this.rxByteBuffer.clear();
            }

            if (this.socketChannel != null)
            {
                TcpChannelUtils.closeChannel(this.socketChannel);
                this.reader.cancel(this.socketChannel);
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
        final boolean hasRxData = this.rxData;
        this.rxData = false;
        return hasRxData;
    }

    @Override
    public int getTxQueueSize()
    {
        return this.txFrames[0].size() + this.txFrames[1].size();
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
     * @param rxByteBuffer the byte buffer read from a TCP socket
     * @param rxBytes      the raw bytes extracted from the buffer
     * @param frames       the reference to the buffer to use for holding the decoded frames read from the
     *                     raw bytes
     * @param framesSize   int[] to allow the result array size to be reported
     * @return the frame buffer with all the decoded frames
     */
    ByteBuffer[] readFrames(ByteBuffer rxByteBuffer, byte[] rxBytes, ByteBuffer[] frames, int[] framesSize);
}

/**
 * Base class for {@link IFrameReaderWriter} implementations. This class handles partial writing of
 * data buffers to a socket.
 *
 * @author Ramon Servadei
 */
abstract class AbstractFrameReaderWriter implements IFrameReaderWriter
{
    // note: the header will never by this large, but we just use it to ensure we have enough so the
    // txBuffer never resizes
    static final int HEADER_SPACE = 1024;
    static final int BUFFER_SIZE = TX_SEND_SIZE + HEADER_SPACE;

    final TcpChannel tcpChannel;
    final ByteBuffer txBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    volatile boolean writeInProgress = false;
    long zeroByteWriteTimeNanos;

    AbstractFrameReaderWriter(TcpChannel tcpChannel)
    {
        super();
        this.tcpChannel = tcpChannel;
    }

    /**
     * Write the internal {@link #txBuffer} to the socket. This will try to write the
     * buffer to the socket and set {@link #writeInProgress} if the write does not fully complete.
     */
    final void writeBufferToSocket() throws IOException
    {
        long time = System.nanoTime();

        if (!this.writeInProgress)
        {
            // only flip if there is not a previous write in progress - we are still reading from
            // the buffer if a write is in progress
            this.txBuffer.flip();
            this.writeInProgress = true;
        }

        final int byteCount = this.txBuffer.remaining();
        final int bytesWritten;
        if ((bytesWritten = this.tcpChannel.socketChannel.write(this.txBuffer)) != byteCount)
        {
            final long current = System.nanoTime();
            if (current - this.zeroByteWriteTimeNanos > 5000000000L)
            {
                this.zeroByteWriteTimeNanos = current;
                Log.log(this.tcpChannel, "SLOW SOCKET: wrote ", Integer.toString(bytesWritten), "/",
                        Integer.toString(byteCount), " bytes at least once in the last 5 secs to ",
                        this.tcpChannel.toString());
            }

            // don't reset the writeInProgress flag and don't compact the buffer
            // as we need to continue reading it on the next run
        }
        else
        {
            this.writeInProgress = false;
            this.txBuffer.compact();
        }

        time = System.nanoTime() - time;
        if (time > SLOW_TX_FRAME_THRESHOLD_NANOS)
        {
            Log.log(this.tcpChannel, "SLOW FRAME WRITE: ",
                    Long.toString((long) (time * TcpChannel._INVERSE_1000000)), "ms to write ",
                    Integer.toString(bytesWritten), " bytes to ", this.tcpChannel.toString());
        }
    }

    abstract void writeNextFrame(ByteBuffer header, ByteBuffer data) throws Exception;
}

/**
 * Handles frame encoding using a terminator field after each frame.
 *
 * @author Ramon Servadei
 * @see TcpChannelUtils#decodeUsingTerminator
 */
final class TerminatorBasedReaderWriter extends AbstractFrameReaderWriter
{
    TerminatorBasedReaderWriter(TcpChannel tcpChannel)
    {
        super(tcpChannel);
    }

    @Override
    public ByteBuffer[] readFrames(ByteBuffer rxByteBuffer, byte[] rxBytes, ByteBuffer[] frames,
            int[] framesSize)
    {
        return TcpChannelUtils.decodeUsingTerminator(frames, framesSize, rxByteBuffer, rxBytes,
                TcpChannel.TERMINATOR);
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
 * @author Ramon Servadei
 * @see TcpChannelUtils#decode
 */
final class LengthBasedWriter extends AbstractFrameReaderWriter
{
    LengthBasedWriter(TcpChannel tcpChannel)
    {
        super(tcpChannel);
    }

    @Override
    public ByteBuffer[] readFrames(ByteBuffer rxByteBuffer, byte[] rxBytes, ByteBuffer[] frames,
            int[] framesSize)
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
