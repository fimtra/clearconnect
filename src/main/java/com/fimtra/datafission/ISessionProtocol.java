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
package com.fimtra.datafission;

import com.fimtra.datafission.core.session.ISessionListener;

/**
 * Handles operations required to synchronise a session between a proxy and a publisher. The
 * protocol to synchronise a session is implementation specific and consists of an initial sync
 * message followed by N repetitions of sync responses between a proxy and publisher:
 * 
 * <pre>
 * [proxy(client)]        [publisher(server)]
 *        |                        |
 *        |----(INITIAL SYNC)----->|
 *        |                        |
 *        |<---(SYNC RESPONSE)-----|
 *        |                        |
 *        |----(SYNC RESPONSE)---->|
 *        ...
 * </pre>
 * 
 * Once synchronised, sending data using the session is done via the {@link #encode(byte[])} and
 * {@link #decode(byte[])} methods and can provide session specific data transfer (e.g. encryption).
 * 
 * @author Ramon Servadei
 */
public interface ISessionProtocol
{

    /**
     * Encapsulates responses to received sync data
     * 
     * @see ICodec#handleSessionSyncData(byte[])
     * @author Ramon Servadei
     */
    class SyncResponse
    {
        public final boolean syncFailed;
        public final boolean syncComplete;
        public final byte[] syncDataResponse;

        SyncResponse(boolean syncFailed, boolean syncComplete, byte[] syncDataResponse)
        {
            super();
            this.syncFailed = syncFailed;
            this.syncComplete = syncComplete;
            this.syncDataResponse = syncDataResponse;
        }

        public SyncResponse(byte[] syncDataResponse)
        {
            this(false, false, syncDataResponse);
        }
    }

    final class SyncFailed extends SyncResponse
    {
        public SyncFailed(byte[] syncDataResponse)
        {
            super(true, false, syncDataResponse);
        }
    }

    final class SyncComplete extends SyncResponse
    {
        public SyncComplete(byte[] syncDataResponse)
        {
            super(false, true, syncDataResponse);
        }
    }

    /**
     * Get the starting message to send to begin synchronising a session.
     * 
     * @param sessionContext
     *            the session context for the codec sync
     * @return the byte[] for the initial codec sync message
     */
    byte[] getSessionSyncStartMessage(String sessionContext);

    /**
     * Handle a sync message (initial or response) from the other end and return a response.
     * 
     * @param data
     *            the data from the other end
     * @return a response
     */
    SyncResponse handleSessionSyncData(byte[] data);

    /**
     * Destroy this instance
     */
    void destroy();

    /**
     * Encode the data for the session
     * 
     * @param toSend
     *            the data to send
     * @return the data encoded for the session
     */
    byte[] encode(byte[] toSend);

    /**
     * Decode received data that was encoded via {@link #encode(byte[])} for this session
     * 
     * @param received
     *            the data to decode
     * @return the decoded data for the session
     */
    byte[] decode(byte[] received);

    /**
     * Set the listener to receive callbacks about the session status
     * 
     * @param sessionListener
     *            the listener
     */
    void setSessionListener(ISessionListener sessionListener);
}