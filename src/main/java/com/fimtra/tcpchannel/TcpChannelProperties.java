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
    }

    private TcpChannelProperties()
    {
    }
}
