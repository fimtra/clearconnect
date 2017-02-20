/*
 * Copyright (c) 2016 Ramon Servadei
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
package com.fimtra.ipcchannel;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ConnectException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.util.Log;
import com.fimtra.util.LowGcLinkedList;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.ThreadUtils;

/**
 * Uses {@link MappedByteBuffer} instances to transfer data between two processes. The buffers are
 * each backed by a random access file. The buffers handle the inbound and outbound data between the
 * 2 processes. Essentially, each process writes to its own outbound buffer which is also the
 * inbound buffer of the other process. There is a transmission control protocol within the
 * structure of the buffers to co-ordinate reading and writing between them. The buffer format in
 * ABNF is:
 * 
 * <pre>
 * buffer format = syn hb rseq wseq dlen data 
 * 
 * syn = OCTET; sync flags
 * hb = OCTET; heartbeat signal 
 * rseq = 4 OCTET; read sequence counter
 * wseq = 4 OCTET; write sequence counter
 * dlen = 4 OCTET; data length
 * data = 1*OCTET
 * </pre>
 * 
 * Note: data is max 65536 - 14
 * <p>
 * The channel has an "idle duty cycle" which effectively means that when no data is available for
 * reading or writing the channel will scan for events for a period then sleep for a period. The
 * idle duty cycle is set at 500us on, 500us off. This exists because there is no way to receive
 * asynchronous notifications when data is available for reading/writing to a
 * {@link MappedByteBuffer} (it can't be registered with a {@link Selector}).
 * 
 * @author Ramon Servadei
 */
public final class IpcChannel implements ITransportChannel
{
    /** The period (nanos) for the duty cycle off (when the processing sleeps) */
    // 500us
    private static final long DUTY_CYCLE_OFF_NANOS = 500 * 1000;
    // todo make the duty cycle configurable?
    /** The period (nanos) for the duty cycle on (when the processing is active) */
    private static final long DUTY_CYCLE_ON_NANOS = 1 * DUTY_CYCLE_OFF_NANOS;
    // 1 second at least
    static final long HEARTBEAT_TIMEOUT_NANOS = DUTY_CYCLE_OFF_NANOS * 1000 * 1000000l;

    private static final int CHANNEL_FILE_SIZE = 65536;

    // file segment offsets
    private static final int SYN_INDEX = 0;
    private static final int HB_INDEX = SYN_INDEX + 1;
    private static final int RSEQ_INDEX = HB_INDEX + 1;
    private static final int WSEQ_INDEX = RSEQ_INDEX + 4;
    private static final int DATA_LEN_INDEX = WSEQ_INDEX + 4;
    private static final int DATA_INDEX = DATA_LEN_INDEX + 4;

    // SYNC masks
    private static final byte SYN1_MASK = 1;
    private static final byte SYN2_MASK = 1 << 1;
    private static final byte DESTROY_MASK = SYN1_MASK;
    private static final byte SYN_1_2_MASK = SYN1_MASK | SYN2_MASK;

    static void sleep(int millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
        }
    }

    final byte[] readData;
    final LowGcLinkedList<byte[]> writeData;
    final IReceiver receiver;
    final AtomicBoolean hasRxData;
    final File inFile, outFile;
    final MappedByteBuffer out;
    final Thread ioThread;
    final AtomicBoolean onChannelClosedCalled;

    MappedByteBuffer in;
    boolean connected;
    boolean active;
    int rSeq = 0;
    int wSeq = 0;
    int inWseq = 0;

    public IpcChannel(final String outFileName, final String inFileName, IReceiver receiver) throws IOException
    {
        this.receiver = receiver;
        this.readData = new byte[CHANNEL_FILE_SIZE];
        this.active = true;
        this.onChannelClosedCalled = new AtomicBoolean(false);
        this.writeData = new LowGcLinkedList<byte[]>();
        this.hasRxData = new AtomicBoolean(false);

        this.outFile = new File(outFileName);
        if (!this.outFile.exists())
        {
            if (!this.outFile.createNewFile())
            {
                throw new IOException("Could not create " + this.outFile);
            }
        }

        final RandomAccessFile randomAccessFile = new RandomAccessFile(this.outFile, "rwd");
        final FileChannel channel = randomAccessFile.getChannel();
        this.out = channel.map(MapMode.READ_WRITE, 0, CHANNEL_FILE_SIZE);
        channel.close();
        randomAccessFile.close();

        this.inFile = new File(inFileName);

        this.ioThread = ThreadUtils.newDaemonThread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    // wait for the infile to appear
                    {
                        int i = 0;
                        final int sleepTimeMillis = 5;
                        final int loops = IpcService.SCAN_PERIOD_MILLIS / sleepTimeMillis;
                        while (!IpcChannel.this.inFile.exists() || IpcChannel.this.inFile.length() != CHANNEL_FILE_SIZE)
                        {
                            if (i++ == loops)
                            {
                                throw new ConnectException(IpcChannel.this.inFile.getAbsolutePath() + " exists="
                                    + IpcChannel.this.inFile.exists() + " size=" + IpcChannel.this.inFile.length());
                            }
                            sleep(sleepTimeMillis);
                        }

                        final RandomAccessFile rafInFile = new RandomAccessFile(IpcChannel.this.inFile, "r");
                        final FileChannel inChannel = rafInFile.getChannel();
                        IpcChannel.this.in = inChannel.map(MapMode.READ_ONLY, 0, CHANNEL_FILE_SIZE);
                        rafInFile.close();
                        inChannel.close();
                    }

                    sync();

                    handleIO();
                }
                catch (Exception e)
                {
                    // signal the receiver the channel is closed
                    destroy("Error handling IO", e);
                }
            }
        }, "reader-writer-" + this.inFile.getName() + "<->" + this.outFile.getName());
        this.ioThread.start();
    }

    void sync()
    {
        this.connected = false;
        this.wSeq = 0;
        this.rSeq = 0;

        // setup for sync and wait
        this.out.put(SYN_INDEX, (byte) 0);
        this.out.putInt(WSEQ_INDEX, this.wSeq);
        this.out.putInt(RSEQ_INDEX, this.rSeq);
        while (this.active && !(this.in.getInt(WSEQ_INDEX) == 0 && this.in.getInt(RSEQ_INDEX) == 0))
            sleep(1);
        // wait for sync
        this.out.put(SYN_INDEX, SYN1_MASK);
        while (this.active && (this.in.get(SYN_INDEX) & SYN1_MASK) != SYN1_MASK)
            sleep(1);
        // mark session synced
        this.out.put(SYN_INDEX, SYN_1_2_MASK);
        while (this.active && (this.in.get(SYN_INDEX) & SYN2_MASK) != SYN2_MASK)
            sleep(1);
        // ensure we can't re-sync with this session
        this.out.put(SYN_INDEX, SYN2_MASK);

        if (this.active)
        {
            this.connected = true;
            this.receiver.onChannelConnected(this);
        }
    }

    void handleIO()
    {
        boolean actionThisLoop = false;
        boolean timerStarted = false;
        long timerStart = 0;

        byte outHb = 0;
        byte inHb = 0, inHbPrev = 0;
        boolean hbTimerStarted = false;
        long hbTimerStart = 0;
        byte[] data = null;
        int dataLen = 0;

        while (this.active)
        {
            // check other end is still alive and in sync
            if ((this.in.get(SYN_INDEX) & SYN2_MASK) != SYN2_MASK
                || (hbTimerStarted && (System.nanoTime() - hbTimerStart > HEARTBEAT_TIMEOUT_NANOS)))
            {
                destroy(
                    "disconnected" + ((hbTimerStarted && (System.nanoTime() - hbTimerStart > HEARTBEAT_TIMEOUT_NANOS)
                        ? " (HB failed)" : " (SYN incorrect: " + this.in.get(SYN_INDEX) + ")")));
                return;
            }

            // check for data to write
            if (data == null)
            {
                synchronized (this.writeData)
                {
                    data = this.writeData.poll();
                }
            }

            // write
            if (data != null)
            {
                // NOTE: this means writing "spins" more until reading happens...we are not actually
                // guaranteed to complete this write
                actionThisLoop = true;

                // check if the other side read seq = our write seq
                if (this.in.getInt(RSEQ_INDEX) == this.wSeq)
                {
                    // we can write
                    this.out.putInt(DATA_LEN_INDEX, data.length);
                    this.out.position(DATA_INDEX);
                    this.out.put(data);
                    this.out.putInt(WSEQ_INDEX, ++this.wSeq);
                    data = null;
                }
            }

            // read
            if ((this.inWseq = this.in.getInt(WSEQ_INDEX)) != this.rSeq)
            {
                actionThisLoop = true;

                dataLen = this.in.getInt(DATA_LEN_INDEX);
                byte[] dataRead = new byte[dataLen];
                this.in.position(DATA_INDEX);
                this.in.get(dataRead, 0, dataLen);

                this.hasRxData.set(true);
                try
                {
                    this.receiver.onDataReceived(dataRead, this);
                }
                catch (Exception e)
                {
                    Log.log(this, "Could not handle data from " + ObjectUtils.safeToString(this), e);
                }

                this.rSeq = this.inWseq;
                this.out.putInt(RSEQ_INDEX, this.rSeq);
            }

            // duty cycle check
            if (actionThisLoop == false ||
            // this means we have data but could not send - receiver not ready
                data != null)
            {
                if (!timerStarted)
                {
                    // start the timer
                    timerStarted = true;
                    timerStart = System.nanoTime();
                }

                // duty cycle: Xms on 1ms off
                if (System.nanoTime() - timerStart > DUTY_CYCLE_ON_NANOS)
                {
                    LockSupport.parkNanos(DUTY_CYCLE_OFF_NANOS);
                    timerStarted = false;
                }
            }
            else
            {
                timerStarted = false;
            }

            actionThisLoop = false;

            // heartbeat processing
            this.out.put(HB_INDEX, ++outHb);
            inHbPrev = inHb;
            inHb = this.in.get(HB_INDEX);
            if (inHb == inHbPrev)
            {
                if (!hbTimerStarted)
                {
                    hbTimerStart = System.nanoTime();
                    hbTimerStarted = true;
                }
            }
            else
            {
                hbTimerStarted = false;
            }
        }
    }

    void notifyChannelClosed()
    {
        try
        {
            this.receiver.onChannelClosed(this);
        }
        catch (Exception e)
        {
            Log.log(this, "Could not handle disconnect callback from " + ObjectUtils.safeToString(this) + ", callback="
                + ObjectUtils.safeToString(this.receiver), e);
        }
    }

    @Override
    public boolean sendAsync(byte[] toSend)
    {
        synchronized (this.writeData)
        {
            return this.writeData.add(toSend);
        }
    }

    @Override
    public void destroy(String reason, Exception... e)
    {
        if (this.onChannelClosedCalled.getAndSet(true))
        {
            return;
        }

        this.connected = false;
        this.active = false;

        if (e == null || e.length == 0)
        {
            Log.log(this, reason, ", destroying ", ObjectUtils.safeToString(this));
        }
        else
        {
            Log.log(this, reason + ", destroying " + ObjectUtils.safeToString(this), e[0]);
        }

        this.out.put(SYN_INDEX, DESTROY_MASK);
        notifyChannelClosed();
    }

    @Override
    public boolean isConnected()
    {
        return this.connected;
    }

    @Override
    public String getEndPointDescription()
    {
        return this.inFile.getAbsolutePath();
    }

    @Override
    public String getDescription()
    {
        return this.inFile.getPath() + "<->" + this.outFile.getPath();
    }

    @Override
    public boolean hasRxData()
    {
        return this.hasRxData.getAndSet(false);
    }

    @Override
    public String toString()
    {
        return (this.connected ? "[CONNECTED] " : "[DISCONNECTED] ") + this.getDescription();
    }
}
