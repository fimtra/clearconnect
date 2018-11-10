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
import java.nio.ByteBuffer;
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
    static class FromProxy extends SimpleSessionProtocol.FromProxy
    {
        private static final long serialVersionUID = 1L;
        Key key;
        byte[] extra;
        int keySize;
        String transformation;
        String dataTransformation;
    }

    static class FromPublisher extends SimpleSessionProtocol.FromPublisher
    {
        private static final long serialVersionUID = 1L;
        Key key;
        byte[] extra;
        String dataTransformation;
    }

    AsymmetricCipher handshakeCipher;

    public EncryptedSessionSyncProtocol()
    {
    }

    @Override
    public final byte[] getSessionSyncStartMessage(String sessionContext)
    {
        this.sessionContext = sessionContext;

        // the proxy starts the session sync procedure and reacts to
        // which ever key the publisher uses.
        try
        {
            this.handshakeCipher = new AsymmetricCipher(AsymmetricCipher.ALGORITHM_RSA, 2048);

            final FromProxy initSync = new FromProxy();
            initSync.key = this.handshakeCipher.getPubKey();
            initSync.transformation = AsymmetricCipher.TRANSFORMATION;
            initSync.keySize = this.handshakeCipher.getKeySize();
            return SerializationUtils.toByteArray(initSync);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final SyncResponse handleSessionSyncData(ByteBuffer data)
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
                    final FromPublisher response = new FromPublisher();
                    // todo check version >= than 3.15.5?
                    if (fromProxy.version != null)
                    {
                        this.handshakeCipher = new AsymmetricCipher(fromProxy.key.getAlgorithm(), fromProxy.keySize);
                        this.handshakeCipher.setTransformation(fromProxy.transformation);
                        this.handshakeCipher.setEncryptionKey(fromProxy.key);
                        response.key = this.handshakeCipher.getPubKey();
                    }
                    else
                    {
                        // pre 3.15.5 code support
                        Log.log(this, "pre 3.15.5 proxy connection");
                        this.handshakeCipher = new AsymmetricCipher(AsymmetricCipher.ALGORITHM_RSA, 2048);
                        // yes, this is wrong for the transformation but this is the pre 3.15.5 code
                        this.handshakeCipher.setTransformation("RSA");
                        this.handshakeCipher.setEncryptionKey(fromProxy.key);
                        response.key = this.handshakeCipher.getPubKey();
                    }

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
                        return new SyncFailed(SerializationUtils.toByteArray(response));
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
                        if (fromPublisher.version != null)
                        {
                            this.handshakeCipher.setTransformation(AsymmetricCipher.TRANSFORMATION);
                            this.handshakeCipher.setEncryptionKey(fromPublisher.key);
                        }
                        else
                        {
                            // pre 3.15.5 code support
                            Log.log(this, "pre 3.15.5 publisher connection");
                            // yes, this is wrong for the transformation but this is the pre 3.15.5 code
                            this.handshakeCipher.setTransformation("RSA");
                            this.handshakeCipher.setEncryptionKey(fromPublisher.key);
                        }

                        final FromProxy response = new FromProxy();
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
                    return new SyncFailed(null);
                }
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Error synchronising session", e);
            return new SyncFailed(null);
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
