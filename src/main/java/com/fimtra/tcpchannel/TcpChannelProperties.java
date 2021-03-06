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

import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.SystemUtils;

/**
 * Defines the properties and property keys used by TcpChannel
 *
 * @author Ramon Servadei
 */
public abstract class TcpChannelProperties {
    /**
     * The names of the properties
     *
     * @author Ramon Servadei
     */
    public interface Names {
        String BASE = "tcpChannel.";
        /**
         * The system property name to define the receive buffer size in bytes.<br> E.g.
         * <code>-DtcpChannel.rxBufferSize=65535</code>
         */
        String PROPERTY_NAME_RX_BUFFER_SIZE = BASE + "rxBufferSize";
        /**
         * The system property name to define the send buffer size in bytes. This MUST always be less than the
         * receive buffer size.<br> E.g. <code>-DtcpChannel.txBufferSize=1024</code>
         */
        String PROPERTY_NAME_TX_BUFFER_SIZE = BASE + "txBufferSize";
        /**
         * The system property name to define the frame encoding. Value is one of the {@link
         * FrameEncodingFormatEnum}s<br> E.g. <code>-DtcpChannel.frameEncoding=TERMINATOR_BASED</code>
         */
        String PROPERTY_NAME_FRAME_ENCODING = BASE + "frameEncoding";
        /**
         * The system property name to define the WHITELISTING access control list (ACL) used by any {@link
         * TcpServer} instances in the VM. This is a semi-colon separated list of regular expressions that are
         * matched against incoming TCP/IP remote host IP addresses. If a remote host IP does not match, the
         * connection is terminated.<br> E.g. <code>-DtcpChannel.serverAcl=10.0.0.*;10.1.2.3 </code>
         */
        String PROPERTY_NAME_SERVER_ACL = BASE + "serverAcl";
        /**
         * The system property name to define the BLACKLISTING access control list (ACL) used by any {@link
         * TcpServer} instances in the VM. This is a semi-colon separated list of regular expressions that are
         * matched against incoming TCP/IP remote host IP addresses. If a remote host IP MATCHES, the
         * connection is terminated.
         * <p>
         * <b>This takes precedence over the whitelisting ACL</b><br>
         * E.g. <code>-DtcpChannel.serverBlacklistAcl=10.0.0.*;10.1.2.3 </code>
         */
        String PROPERTY_NAME_SERVER_BLACKLIST_ACL = BASE + "serverBlacklistAcl";
        /**
         * The system property name to define if connections to the {@link TcpServer} instances in the runtime
         * are logged. The logging will track the number of connections attempted from each host and whether
         * the host is blacklisted or blocked.<br> E.g. <code>-DtcpChannel.serverConnectionLogging=true</code>
         */
        String SERVER_CONNECTION_LOGGING = BASE + "serverConnectionLogging";
        /**
         * The system property name to define whether TCP server sockets can re-use an address.<br> E.g.
         * <code>-DtcpChannel.serverSocketReuseAddr=true</code>
         *
         * @see Socket#setReuseAddress(boolean)
         */
        String SERVER_SOCKET_REUSE_ADDR = BASE + "serverSocketReuseAddr";
        /**
         * The system property name to define the threshold, in nanos, for defining slow RX frame handling
         * (and thus logging a message indicating the RX frame handling was slow).<br> E.g.
         * <code>-DtcpChannel.slowRxFrameThresholdNanos=50000000</code>
         */
        String SLOW_RX_FRAME_THRESHOLD_NANOS = BASE + "slowRxFrameThresholdNanos";
        /**
         * The system property name to define the threshold, in nanos, for defining slow TX frame handling
         * (and thus logging a message indicating the TX frame handling was slow).<br> E.g.
         * <code>-DtcpChannel.slowTxFrameThresholdNanos=50000000</code>
         */
        String SLOW_TX_FRAME_THRESHOLD_NANOS = BASE + "slowTxFrameThresholdNanos";
        /**
         * The system property name to define the minimum alive time, in milliseconds, for a socket before it
         * is classified as a "short-lived" socket and increases the short-lived socket count for the IP.<br>
         * E.g. <code>-DtcpChannel.slsMinSocketAliveTimeMillis=100</code>
         */
        String SLS_MIN_SOCKET_ALIVE_TIME_MILLIS = BASE + "slsMinSocketAliveTimeMillis";
        /**
         * The system property name to define the blacklist timeout, in milliseconds, for an IP after it has
         * exceeded the number of short-lived sockets.<br> E.g. <code>-DtcpChannel.slsBlacklistTimeMillis=300000</code>
         */
        String SLS_BLACKLIST_TIME_MILLIS = BASE + "slsBlacklistTimeMillis";
        /**
         * The system property name to define the maximum short-lived sockets from an IP before it is
         * blacklisted.<br> E.g. <code>-DtcpChannel.slsMaxShortLivedSocketTries=3</code>
         */
        String SLS_MAX_SHORT_LIVED_SOCKET_TRIES = BASE + "slsMaxShortLivedSocketTries";
        /**
         * The system property name to define the size of the TX queue that causes the socket to be destroyed
         * from the sender side due to slow consumption on the receiver side.
         * <p>
         * Use a value of 0 to mean no send queue threshold is in use.
         * <p>
         * E.g. <code>-DtcpChannel.sendQueueThreshold=10000</code>
         *
         * @see #SEND_QUEUE_THRESHOLD_BREACH_MILLIS
         */
        String SEND_QUEUE_THRESHOLD = BASE + "sendQueueThreshold";

        /**
         * The system property name to define the time allowed (in millis) for the send queue threshold to be
         * breached before destroying the socket.
         * <p>
         * E.g. <code>-DtcpChannel.sendQueueThresholdBreachMillis=30000</code>
         *
         * @see #SEND_QUEUE_THRESHOLD
         */
        String SEND_QUEUE_THRESHOLD_BREACH_MILLIS = BASE + "sendQueueThresholdBreachMillis";

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

        /**
         * The system property name to define the maximum size of the pool to hold re-usable tx fragment
         * objects.
         * <p>
         * E.g. <code>-DtcpChannel.txFragmentPoolMaxSize=1000</code>
         */
        String TX_FRAGMENT_POOL_MAX_SIZE = BASE + "txFragmentPoolMaxSize";

        /**
         * The system property name to define the maximum size of the pool to hold re-usable rx fragment
         * objects.
         * <p>
         * E.g. <code>-DtcpChannel.rxFragmentPoolMaxSize=32</code>
         */
        String RX_FRAGMENT_POOL_MAX_SIZE = BASE + "rxFragmentPoolMaxSize";

        /**
         * The system property name to define the maximum size of the pool to hold re-usable frame resolver
         * objects.
         * <p>
         * E.g. <code>-DtcpChannel.rxFrameResolverPoolMaxSize=1000</code>
         */
        String RX_FRAME_RESOLVER_POOL_MAX_SIZE = BASE + "rxFrameResolverPoolMaxSize";

        /**
         * The system property name to define the range start for ephemeral ports.
         * <p>
         * E.g. <code>-DtcpChannel.ephemeralPortRangeStart=22222</code>
         */
        String EPHEMERAL_PORT_RANGE_START = BASE + "ephemeralPortRangeStart";

        /**
         * The system property name to define the range end for ephemeral ports.
         * <p>
         * E.g. <code>-DtcpChannel.ephemeralPortRangeEnd=33333</code>
         */
        String EPHEMERAL_PORT_RANGE_END = BASE + "ephemeralPortRangeEnd";
    }

    /**
     * The values of the properties described in {@link Names}
     *
     * @author Ramon Servadei
     */
    public interface Values {
        /**
         * The frame encoding, default is TERMINATOR_BASED.
         *
         * @see Names#PROPERTY_NAME_FRAME_ENCODING
         */
        TcpChannel.FrameEncodingFormatEnum FRAME_ENCODING = TcpChannel.FrameEncodingFormatEnum.valueOf(
                System.getProperty(Names.PROPERTY_NAME_FRAME_ENCODING,
                        TcpChannel.FrameEncodingFormatEnum.TERMINATOR_BASED.toString()));

        /**
         * The receive buffer size, default is 65k.
         *
         * @see Names#PROPERTY_NAME_RX_BUFFER_SIZE
         */
        int RX_BUFFER_SIZE = SystemUtils.getPropertyAsInt(Names.PROPERTY_NAME_RX_BUFFER_SIZE, 65535);

        /**
         * The send buffer size, default is 1k.
         *
         * @see Names#PROPERTY_NAME_TX_BUFFER_SIZE
         */
        int TX_SEND_SIZE = SystemUtils.getPropertyAsInt(Names.PROPERTY_NAME_TX_BUFFER_SIZE, 1024);

        /**
         * The default server socket re-use address value, default is <code>true</code>.
         *
         * @see Names#SERVER_SOCKET_REUSE_ADDR
         */
        boolean SERVER_SOCKET_REUSE_ADDR = SystemUtils.getProperty(Names.SERVER_SOCKET_REUSE_ADDR, true);

        /**
         * The default for server connection logging, default is <code>true</code>.
         *
         * @see Names#SERVER_CONNECTION_LOGGING
         */
        boolean SERVER_CONNECTION_LOGGING = SystemUtils.getProperty(Names.SERVER_CONNECTION_LOGGING, true);

        /**
         * The threshold value for logging when RX frame handling is slow, in nanos. This is important to
         * identify potential performance problems for TCP RX handling.
         * <p>
         * Default is: 10000000 (10ms)
         *
         * @see Names#SLOW_RX_FRAME_THRESHOLD_NANOS
         */
        long SLOW_RX_FRAME_THRESHOLD_NANOS =
                SystemUtils.getPropertyAsLong(Names.SLOW_RX_FRAME_THRESHOLD_NANOS, 10_000_000);

        /**
         * The threshold value for logging when TX frame handling is slow, in nanos. This is important to
         * identify potential performance problems for TCP TX handling.
         * <p>
         * Default is: 10000000 (10ms)
         *
         * @see Names#SLOW_TX_FRAME_THRESHOLD_NANOS
         */
        long SLOW_TX_FRAME_THRESHOLD_NANOS =
                SystemUtils.getPropertyAsLong(Names.SLOW_TX_FRAME_THRESHOLD_NANOS, 10_000_000);

        /**
         * The value for the minimum alive time, in milliseconds, for a socket before it is classified as a
         * "short-lived socket" and increases the short-lived socket count for the IP.
         * <p>
         * Default is: 100
         */
        long SLS_MIN_SOCKET_ALIVE_TIME_MILLIS =
                SystemUtils.getPropertyAsLong(Names.SLS_MIN_SOCKET_ALIVE_TIME_MILLIS, 100);

        /**
         * The blacklist timeout, in milliseconds, for an IP after it has exceeded the number of short-lived
         * sockets.<br>
         * <p>
         * Default is: 300000 (5 minutes)
         */
        int SLS_BLACKLIST_TIME_MILLIS =
                SystemUtils.getPropertyAsInt(Names.SLS_BLACKLIST_TIME_MILLIS, 300_000);

        /**
         * The maximum short-lived sockets from an IP before it is blacklisted.
         * <p>
         * Default is: 3
         */
        int SLS_MAX_SHORT_LIVED_SOCKET_TRIES =
                SystemUtils.getPropertyAsInt(Names.SLS_MAX_SHORT_LIVED_SOCKET_TRIES, 3);

        /**
         * The size of the TX queue that causes the socket to be destroyed from the sender side due to slow
         * consumption on the receiver side. A value of 0 means no send queue threshold is in use.
         * <p>
         * Default is: 1000
         */
        int SEND_QUEUE_THRESHOLD = SystemUtils.getPropertyAsInt(Names.SEND_QUEUE_THRESHOLD, 1000);

        /**
         * The time allowed (in millis) for the send queue threshold to be breached before destroying the
         * socket.
         * <p>
         * Default is 10000 (10 secs)
         */
        long SEND_QUEUE_THRESHOLD_BREACH_MILLIS =
                SystemUtils.getPropertyAsLong(Names.SEND_QUEUE_THRESHOLD_BREACH_MILLIS, 10_000);

        /**
         * The number of threads to use for TCP socket reading.
         * <p>
         * Default is: {@link DataFissionProperties.Values#CORE_THREAD_COUNT}
         */
        int READER_THREAD_COUNT = SystemUtils.getPropertyAsInt(Names.READER_THREAD_COUNT,
                DataFissionProperties.Values.CORE_THREAD_COUNT);

        /**
         * The number of threads to use for TCP socket writing.
         * <p>
         * Default is:  {@link DataFissionProperties.Values#CORE_THREAD_COUNT}
         */
        int WRITER_THREAD_COUNT = SystemUtils.getPropertyAsInt(Names.WRITER_THREAD_COUNT,
                DataFissionProperties.Values.CORE_THREAD_COUNT);

        /**
         * The maximum size of the pool to hold re-usable tx fragment objects.
         * <p>
         * Default is: 1000
         */
        int TX_FRAGMENT_POOL_MAX_SIZE = SystemUtils.getPropertyAsInt(Names.TX_FRAGMENT_POOL_MAX_SIZE, 1000);

        /**
         * The maximum size of the pool to hold re-usable rx fragment objects.
         * <p>
         * Default is: 1000
         */
        int RX_FRAGMENT_POOL_MAX_SIZE = SystemUtils.getPropertyAsInt(Names.RX_FRAGMENT_POOL_MAX_SIZE, 1000);

        /**
         * The maximum size of the pool to hold re-usable rx frame resolver objects.
         * <p>
         * Default is: 1000
         */
        int RX_FRAME_RESOLVER_POOL_MAX_SIZE =
                SystemUtils.getPropertyAsInt(Names.RX_FRAME_RESOLVER_POOL_MAX_SIZE, 1000);

        /**
         * The system property name to define the range start for ephemeral ports.
         * <p>
         * Default is: -1 (no range)
         */
        int EPHEMERAL_PORT_RANGE_START = SystemUtils.getPropertyAsInt(Names.EPHEMERAL_PORT_RANGE_START, -1);

        /**
         * The system property name to define the range end for ephemeral ports.
         * <p>
         * Default is: -1 (no range)
         */
        int EPHEMERAL_PORT_RANGE_END = SystemUtils.getPropertyAsInt(Names.EPHEMERAL_PORT_RANGE_END, -1);
    }

    private TcpChannelProperties()
    {
    }
}
