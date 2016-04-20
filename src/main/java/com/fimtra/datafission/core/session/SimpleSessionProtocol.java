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

import com.fimtra.datafission.ISessionProtocol;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SerializationUtils;

/**
 * Synchronises a session using unencrypted messages and provides no data encryption.
 * 
 * <pre>
* [proxy(client)]      [publisher(server)]
*        |                        |
*        |----(session attrs)---->|
*        |                        |
*        |<---(session ID)--------|
 * </pre>
 * 
 * @author Ramon Servadei
 */
public class SimpleSessionProtocol implements ISessionProtocol
{
    static class FromProxy implements Serializable
    {
        private static final long serialVersionUID = 1L;
        String sessionContext;
        byte[] sessionAttrs;
    }

    static class FromPublisher implements Serializable
    {
        private static final long serialVersionUID = 1L;
        Key key;
        byte[] session;
    }

    String sessionId;
    String sessionContext;
    ISessionListener sessionListener;

    public SimpleSessionProtocol()
    {
    }

    @Override
    public byte[] getSessionSyncStartMessage(String sessionContext)
    {
        try
        {
            this.sessionContext = sessionContext;

            final FromProxy startMessage = new FromProxy();
            startMessage.sessionContext = this.sessionContext;
            startMessage.sessionAttrs =
                SerializationUtils.toByteArray(SessionContexts.getSessionAttributes(this.sessionContext));
            return SerializationUtils.toByteArray(startMessage);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not initiate session for session context:" + this.sessionContext, e);
        }
    }

    @Override
    public SyncResponse handleSessionSyncData(byte[] data)
    {
        try
        {
            final Serializable fromByteArray = SerializationUtils.fromByteArray(data);

            if (fromByteArray instanceof FromProxy)
            {
                // the publisher handles these

                final FromProxy fromProxy = (FromProxy) fromByteArray;

                this.sessionContext = fromProxy.sessionContext;
                try
                {
                    this.sessionId = SessionContexts.getSessionManager(fromProxy.sessionContext).createSession(
                        (String[]) SerializationUtils.fromByteArray(((FromProxy) fromByteArray).sessionAttrs));
                }
                catch (Exception e)
                {
                    Log.log(this, "Could not create session listener for session context:" + this.sessionContext, e);
                    this.sessionId = null;
                }

                final FromPublisher response = new FromPublisher();
                
                final boolean syncFailed = (this.sessionId == null);
                if (syncFailed)
                {
                    return new SyncFailed(SerializationUtils.toByteArray(response));
                }
                else
                {
                    response.session = this.sessionId.getBytes();
                    return new SyncComplete(SerializationUtils.toByteArray(response));
                }
            }
            else
            {
                if (fromByteArray instanceof FromPublisher)
                {
                    // proxy handles these

                    final FromPublisher fromPublisher = (FromPublisher) fromByteArray;

                    if (fromPublisher.session != null)
                    {
                        this.sessionId = new String(fromPublisher.session);

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

                    return new SyncFailed(null);
                }
                else
                {
                    Log.log(this, "Incorrect session sync data: " + fromByteArray, new Exception());
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

    @Override
    public final void destroy()
    {
        final ISessionManager sessionManager = SessionContexts.getSessionManager(this.sessionContext);
        try
        {
            sessionManager.sessionEnded(this.sessionId);
        }
        catch (Exception e)
        {
            Log.log(this, "Could not notify " + ObjectUtils.safeToString(sessionManager), e);
        }
        try
        {
            if (this.sessionListener != null)
            {
                this.sessionListener.onSessionClosed(this.sessionContext, this.sessionId);
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Could not notify " + ObjectUtils.safeToString(this.sessionListener), e);
        }
    }

    @Override
    public byte[] encode(byte[] toSend)
    {
        return toSend;
    }

    @Override
    public byte[] decode(byte[] received)
    {
        return received;
    }

    @Override
    public void setSessionListener(ISessionListener sessionListener)
    {
        this.sessionListener = sessionListener;
    }
}
