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
package com.fimtra.datafission.core.session;

import java.io.Serializable;
import java.security.Key;

import com.fimtra.util.AsymmetricCipher;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SerializationUtils;

/**
 * Synchronises a session using encrypted messages but has no encryption for the data
 * 
 * <pre>
* [proxy(client)]              [publisher(server)]
*        |                                |
*        |----(public key)--------------->|
*        |                                |
*        |<---(public key)----------------|
*        |                                |
*        |----(encoded session attrs)---->|
*        |                                |
*        |<---(encoded session ID)--------|
 * </pre>
 * 
 * @author Ramon Servadei
 */
public class EncryptedSessionSyncProtocol extends SimpleSessionProtocol
{
    static class FromProxy extends com.fimtra.datafission.core.session.SimpleSessionProtocol.FromProxy
    {
        private static final long serialVersionUID = 1L;
        Key key;
        byte[] extra;
    }

    static class FromPublisher extends com.fimtra.datafission.core.session.SimpleSessionProtocol.FromPublisher
    {
        private static final long serialVersionUID = 1L;
        Key key;
        byte[] extra;
    }

    final AsymmetricCipher handshakeCipher;

    public EncryptedSessionSyncProtocol()
    {
        try
        {
            this.handshakeCipher = new AsymmetricCipher();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final byte[] getSessionSyncStartMessage(String sessionContext)
    {
        this.sessionContext = sessionContext;

        try
        {
            FromProxy initSync = new FromProxy();
            initSync.key = this.handshakeCipher.getPubKey();
            return SerializationUtils.toByteArray(initSync);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final SyncResponse handleSessionSyncData(byte[] data)
    {
        try
        {
            final Serializable fromByteArray = SerializationUtils.fromByteArray(data);

            if (fromByteArray instanceof FromProxy)
            {
                // the publisher handles these

                final FromProxy fromProxy = (FromProxy) fromByteArray;

                if (fromProxy.key != null)
                {
                    this.handshakeCipher.setEncryptionKey(fromProxy.key);

                    final FromPublisher response = new FromPublisher();
                    response.key = this.handshakeCipher.getPubKey();
                    return new SyncResponse(SerializationUtils.toByteArray(response));
                }
                else
                {
                    this.sessionContext = fromProxy.sessionContext;
                    this.sessionId = SessionContexts.getSessionManager(fromProxy.sessionContext).createSession(
                        (String[]) SerializationUtils.fromByteArray(
                            this.handshakeCipher.decrypt(fromProxy.sessionAttrs)));

                    final FromPublisher response = new FromPublisher();

                    handleExtra(response, fromProxy);

                    final boolean syncFailed = (this.sessionId == null);
                    if (syncFailed)
                    {
                        return new SyncFailed();
                    }
                    else
                    {
                        response.session = this.handshakeCipher.encrypt(this.sessionId.getBytes());
                        return new SyncComplete(SerializationUtils.toByteArray(response));
                    }
                }
            }
            else
            {
                if (fromByteArray instanceof FromPublisher)
                {
                    // the proxy handles these

                    final FromPublisher fromPublisher = (FromPublisher) fromByteArray;
                    if (fromPublisher.key != null)
                    {
                        this.handshakeCipher.setEncryptionKey(fromPublisher.key);

                        FromProxy response = new FromProxy();
                        response.sessionContext = this.sessionContext;
                        response.sessionAttrs = this.handshakeCipher.encrypt(
                            SerializationUtils.toByteArray(SessionContexts.getSessionAttributes(this.sessionContext)));

                        prepareExtra(response);

                        return new SyncResponse(SerializationUtils.toByteArray(response));
                    }
                    else
                    {
                        handleExtra(fromPublisher);

                        this.sessionId = new String(this.handshakeCipher.decrypt(fromPublisher.session));
                        try
                        {
                            if (this.sessionListener != null)
                            {
                                this.sessionListener.onSessionOpen(this.sessionContext, this.sessionId);
                            }
                        }
                        catch (Exception e)
                        {
                            Log.log(this,
                                "Could not notify session listener for session context:" + this.sessionContext, e);
                        }

                        return new SyncComplete(null);
                    }
                }
                else
                {
                    Log.log(this, "Incorrect session sync data: " + ObjectUtils.safeToString(fromByteArray),
                        new Exception());
                    return new SyncFailed();
                }
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Error synchronising session", e);
            return new SyncFailed();
        }
    }

    @SuppressWarnings("unused")
    void prepareExtra(FromProxy response)
    {
        // noop
    }

    @SuppressWarnings("unused")
    void handleExtra(FromPublisher fromPublisher)
    {
        // noop
    }

    @SuppressWarnings("unused")
    void handleExtra(FromPublisher response, FromProxy fromProxy)
    {
        // noop
    }
}
