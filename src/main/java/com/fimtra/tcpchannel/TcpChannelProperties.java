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

import java.net.Socket;

import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;

/**
 * Defines the properties and property keys used by TcpChannel
 * 
 * @author Ramon Servadei
 */
public abstract class TcpChannelProperties
{
    /**
     * The names of the properties
     * 
     * @author Ramon Servadei
     */
    public static interface Names
    {
        String BASE = "tcpChannel.";
        /**
         * The system property name to define the receive buffer size in bytes.<br>
         * E.g. <code>-DtcpChannel.rxBufferSize=65535</code>
         */
        String PROPERTY_NAME_RX_BUFFER_SIZE = BASE + "rxBufferSize";
        /**
         * The system property name to define the send buffer size in bytes. This MUST always be
         * less than the receive buffer size.<br>
         * E.g. <code>-DtcpChannel.txBufferSize=1024</code>
         */
        String PROPERTY_NAME_TX_BUFFER_SIZE = BASE + "txBufferSize";
        /**
         * The system property name to define the frame encoding. Value is one of the
         * {@link FrameEncodingFormatEnum}s<br>
         * E.g. <code>-DtcpChannel.frameEncoding=TERMINATOR_BASED</code>
         */
        String PROPERTY_NAME_FRAME_ENCODING = BASE + "frameEncoding";
        /**
         * The system property name to define the access control list (ACL) used by any
         * {@link TcpServer} instances in the VM. This is a semi-colon separated list of regular
         * expressions that are matched against incoming TCP/IP remote host IP addresses. If a
         * remote host IP does not match, the connection is terminated.<br>
         * E.g. <code>-DtcpChannel.serverAcl=10.0.0.*;10.1.2.3 </code>
         */
        String PROPERTY_NAME_SERVER_ACL = BASE + "serverAcl";
        /**
         * The system property name to define whether TCP server sockets can re-use an address.<br>
         * E.g. <code>-DtcpChannel.serverSocketReuseAddr=true</code>
         * 
         * @see Socket#setReuseAddress(boolean)
         */
        String SERVER_SOCKET_REUSE_ADDR = BASE + "serverSocketReuseAddr";
        /**
         * The system property name to define the threshold, in nanos, for defining slow RX frame
         * handling (and thus logging a message indicating the RX frame handling was slow).<br>
         * E.g. <code>-DtcpChannel.slowTaskThresholdNanos=50000000</code>
         */
        String SLOW_RX_FRAME_THRESHOLD_NANOS = BASE + "slowRxFrameThresholdNanos";
        /**
         * The system property name to define the minimum alive time, in milliseconds, for a socket
         * before it is classified as a "short-lived" socket and increases the short-lived socket
         * count for the IP.<br>
         * E.g. <code>-DtcpChannel.slsMinSocketAliveTimeMillis=100</code>
         */
        String SLS_MIN_SOCKET_ALIVE_TIME_MILLIS = BASE + "slsMinSocketAliveTimeMillis";
        /**
         * The system property name to define the blacklist timeout, in milliseconds, for an IP
         * after it has exceeded the number of short-lived sockets.<br>
         * E.g. <code>-DtcpChannel.slsBlacklistTimeMillis=300000</code>
         */
        String SLS_BLACKLIST_TIME_MILLIS = BASE + "slsBlacklistTimeMillis";
        /**
         * The system property name to define the maximum short-lived sockets from an IP before it
         * is blacklisted.<br>
         * E.g. <code>-DtcpChannel.slsMaxShortLivedSocketTries=3</code>
         */
        String SLS_MAX_SHORT_LIVED_SOCKET_TRIES = BASE + "slsMaxShortLivedSocketTries";
        /**
         * The system property name to define the send factor (in milliseconds) to wait for a TCP
         * message to be sent in {@link TcpChannel#send(byte[])}.
         * <p>
         * Use a value of 0 for no blocking during send (i.e. send asynchronously).
         * <p>
         * E.g. <code>-DtcpChannel.sendWaitFactorMillis=10</code>
         */
        String SEND_WAIT_FACTOR_MILLIS = BASE + "sendWaitFactorMillis";
        /**
         * The system property name to define writing to the TCP socket using the application
         * thread.
         * <p>
         * E.g. <code>-DtcpChannel.writeToSocketUsingApplicationThread=true</code>
         */
        String WRITE_TO_SOCKET_USING_APPLICATION_THREAD = BASE + "writeToSocketUsingApplicationThread";
        /**
         * The system property name to define the number of threads to use for TCP socket reading.
         * <p>
         * E.g. <code>-DtcpChannel.readerThreadCount=4</code>
         */
        String READER_THREAD_COUNT = BASE + "readerThreadCount";
        /**
         * The system property name to define the number of threads to use for TCP socket sending.
         * <p>
         * E.g. <code>-DtcpChannel.writerThreadCount=4</code>
         */
        String WRITER_THREAD_COUNT = BASE + "writerThreadCount";
    }

    /**
     * The values of the properties described in {@link Names}
     * 
     * @author Ramon Servadei
     */
    public static interface Values
    {
        /**
         * The frame encoding, default is TERMINATOR_BASED.
         * 
         * @see Names#PROPERTY_NAME_FRAME_ENCODING
         */
        TcpChannel.FrameEncodingFormatEnum FRAME_ENCODING =
            TcpChannel.FrameEncodingFormatEnum.valueOf(System.getProperty(Names.PROPERTY_NAME_FRAME_ENCODING,
                TcpChannel.FrameEncodingFormatEnum.TERMINATOR_BASED.toString()));

        /**
         * The receive buffer size, default is 65k.
         * 
         * @see Names#PROPERTY_NAME_RX_BUFFER_SIZE
         */
        int RX_BUFFER_SIZE = Integer.parseInt(System.getProperty(Names.PROPERTY_NAME_RX_BUFFER_SIZE, "65535"));

        /**
         * The send buffer size, default is 1k.
         * 
         * @see Names#PROPERTY_NAME_TX_BUFFER_SIZE
         */
        int TX_SEND_SIZE = Integer.parseInt(System.getProperty(Names.PROPERTY_NAME_TX_BUFFER_SIZE, "1024"));

        /**
         * The default server socket re-use address value, default is <code>false</code>.
         * 
         * @see Names#SERVER_SOCKET_REUSE_ADDR
         */
        boolean SERVER_SOCKET_REUSE_ADDR =
            Boolean.valueOf(System.getProperty(Names.SERVER_SOCKET_REUSE_ADDR, "false")).booleanValue();

        /**
         * The threshold value for logging when RX frame handling is slow, in nanos. This is
         * important to identify potential performance problems for TCP RX handling.
         * <p>
         * Default is: 50000000 (50ms)
         * 
         * @see Names#SLOW_RX_FRAME_THRESHOLD_NANOS
         */
        long SLOW_RX_FRAME_THRESHOLD_NANOS =
            Long.parseLong(System.getProperty(Names.SLOW_RX_FRAME_THRESHOLD_NANOS, "50000000"));

        /**
         * The value for the minimum alive time, in milliseconds, for a socket before it is
         * classified as a "short-lived socket" and increases the short-lived socket count for the
         * IP.
         * <p>
         * Default is: 100
         */
        long SLS_MIN_SOCKET_ALIVE_TIME_MILLIS =
            Long.parseLong(System.getProperty(Names.SLS_MIN_SOCKET_ALIVE_TIME_MILLIS, "100"));

        /**
         * The blacklist timeout, in milliseconds, for an IP after it has exceeded the number of
         * short-lived sockets.<br>
         * <p>
         * Default is: 300000 (5 minutes)
         */
        int SLS_BLACKLIST_TIME_MILLIS = Integer.parseInt(System.getProperty(Names.SLS_BLACKLIST_TIME_MILLIS, "300000"));

        /**
         * The maximum short-lived sockets from an IP before it is blacklisted.
         * <p>
         * Default is: 3
         */
        int SLS_MAX_SHORT_LIVED_SOCKET_TRIES =
            Integer.parseInt(System.getProperty(Names.SLS_BLACKLIST_TIME_MILLIS, "3"));

        /**
         * The send factor (in milliseconds) to wait for a TCP message to be sent in
         * {@link TcpChannel#send(byte[])}. A value of 0 means no waiting, just send asynchronously.
         * <p>
         * Default is: 0
         */
        int SEND_WAIT_FACTOR_MILLIS = Integer.parseInt(System.getProperty(Names.SEND_WAIT_FACTOR_MILLIS, "0"));

        /**
         * Defines whether writing to the socket in the {@link TcpChannel#send(byte[])} is performed
         * using the application thread or the TCP writer thread.
         * <p>
         * Default is: false (use the TCP writer thread)
         */
        boolean WRITE_TO_SOCKET_USING_APPLICATION_THREAD =
            Boolean.getBoolean(Names.WRITE_TO_SOCKET_USING_APPLICATION_THREAD);

        /**
         * The number of threads to use for TCP socket reading.
         * <p>
         * Default is: 4
         */
        int READER_THREAD_COUNT = Integer.parseInt(System.getProperty(Names.READER_THREAD_COUNT, "4"));

        /**
         * The number of threads to use for TCP socket writing.
         * <p>
         * Default is: 4
         */
        int WRITER_THREAD_COUNT = Integer.parseInt(System.getProperty(Names.WRITER_THREAD_COUNT, "4"));
    }

    private TcpChannelProperties()
    {
    }
}
