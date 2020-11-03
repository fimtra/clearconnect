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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.fimtra.datafission.ISessionProtocol;
import com.fimtra.datafission.ISessionProtocol.SyncResponse;
import com.fimtra.datafission.core.session.EncryptedSessionSyncProtocol.FromProxy;
import com.fimtra.datafission.core.session.EncryptedSessionSyncProtocol.FromPublisher;
import com.fimtra.util.SerializationUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests for the {@link EncryptedSessionSyncProtocol}
 * 
 * @author Ramon Servadei
 */
public class EncryptedSessionSyncProtocolTest
{
    @Rule
    public TestName name = new TestName();

    ISessionProtocol proxyEnd;
    ISessionProtocol publisherEnd;

    @Before
    public void setUp() throws Exception
    {
        this.proxyEnd = new EncryptedSessionSyncProtocol();
        this.publisherEnd = new EncryptedSessionSyncProtocol();
    }

    @Test
    public void testSyncSessionPre3_15_5_client() throws IOException, ClassNotFoundException
    {
        // prepare mocks
        ISessionAttributesProvider provider = mock(ISessionAttributesProvider.class);
        final String[] attrs = new String[] { "a1", "a2", "a3" };
        when(provider.getSessionAttributes()).thenReturn(attrs);

        ISessionManager manager = mock(ISessionManager.class);
        final String sessionId = "sesh-" + this.name.getMethodName();
        when(manager.createSession(eq(attrs))).thenReturn(sessionId);

        ISessionListener listener = mock(ISessionListener.class);

        // register session collaborators
        SessionContexts.registerSessionProvider(this.name.getMethodName(), provider);
        SessionContexts.registerSessionManager(this.name.getMethodName(), manager);
        this.proxyEnd.setSessionListener(listener);

        // start the session sync
        byte[] syncInit = this.proxyEnd.getSessionSyncStartMessage(this.name.getMethodName());
        
        // simulate that its a pre 3.15.5 client
        FromProxy fromProxy = SerializationUtils.fromByteArray(syncInit);
        fromProxy.version = null;
        fromProxy.dataTransformation = null;
        fromProxy.keySize = 0;
        fromProxy.transformation = null;
        syncInit = SerializationUtils.toByteArray(fromProxy);

        // NOTE: 4-way session sync for encrypted
        SyncResponse response = this.publisherEnd.handleSessionSyncData(ByteBuffer.wrap(syncInit));
        assertFalse(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNotNull(response.syncDataResponse);
        
        // we need to set the FromPublisher.version to null...
        // in a pre 3.15.5 runtime, the version will be ignored from the serialised response
        FromPublisher fromPublisher = SerializationUtils.fromByteArray(response.syncDataResponse);
        fromPublisher.version = null;
        
        response = this.proxyEnd.handleSessionSyncData(ByteBuffer.wrap(SerializationUtils.toByteArray(fromPublisher)));
        assertFalse(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNotNull(response.syncDataResponse);
        
        // simulate that its a pre 3.15.5 client
        fromProxy = SerializationUtils.fromByteArray(response.syncDataResponse);
        fromProxy.version = null;
        fromProxy.dataTransformation = null;
        fromProxy.keySize = 0;
        fromProxy.transformation = null;

        response = this.publisherEnd.handleSessionSyncData(ByteBuffer.wrap(SerializationUtils.toByteArray(fromProxy)));
        assertTrue(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNotNull(response.syncDataResponse);
        
        response = this.proxyEnd.handleSessionSyncData(ByteBuffer.wrap(response.syncDataResponse));
        assertTrue(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNull(response.syncDataResponse);

        verify(listener).onSessionOpen(eq(this.name.getMethodName()), eq(sessionId));
    }

    @Test
    public void testSyncSession()
    {
        // prepare mocks
        ISessionAttributesProvider provider = mock(ISessionAttributesProvider.class);
        final String[] attrs = new String[] { "a1", "a2", "a3" };
        when(provider.getSessionAttributes()).thenReturn(attrs);

        ISessionManager manager = mock(ISessionManager.class);
        final String sessionId = "sesh-" + this.name.getMethodName();
        when(manager.createSession(eq(attrs))).thenReturn(sessionId);

        ISessionListener listener = mock(ISessionListener.class);

        // register session collaborators
        SessionContexts.registerSessionProvider(this.name.getMethodName(), provider);
        SessionContexts.registerSessionManager(this.name.getMethodName(), manager);
        this.proxyEnd.setSessionListener(listener);

        // start the session sync
        final byte[] syncInit = this.proxyEnd.getSessionSyncStartMessage(this.name.getMethodName());

        // NOTE: 4-way session sync for encrypted
        SyncResponse response = this.publisherEnd.handleSessionSyncData(ByteBuffer.wrap(syncInit));
        assertFalse(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNotNull(response.syncDataResponse);

        response = this.proxyEnd.handleSessionSyncData(ByteBuffer.wrap(response.syncDataResponse));
        assertFalse(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNotNull(response.syncDataResponse);

        response = this.publisherEnd.handleSessionSyncData(ByteBuffer.wrap(response.syncDataResponse));
        assertTrue(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNotNull(response.syncDataResponse);

        response = this.proxyEnd.handleSessionSyncData(ByteBuffer.wrap(response.syncDataResponse));
        assertTrue(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNull(response.syncDataResponse);

        verify(listener).onSessionOpen(eq(this.name.getMethodName()), eq(sessionId));
    }

    @Test
    public void testSyncFail()
    {
        // simulate failed sync
        ISessionManager manager = mock(ISessionManager.class);
        when(manager.createSession((String[]) any())).thenReturn(null);

        SessionContexts.registerSessionManager(this.name.getMethodName(), manager);

        final byte[] syncInit = this.proxyEnd.getSessionSyncStartMessage(this.name.getMethodName());

        SyncResponse response = this.publisherEnd.handleSessionSyncData(ByteBuffer.wrap(syncInit));
        assertFalse(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNotNull(response.syncDataResponse);

        response = this.proxyEnd.handleSessionSyncData(ByteBuffer.wrap(response.syncDataResponse));
        assertFalse(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNotNull(response.syncDataResponse);

        response = this.publisherEnd.handleSessionSyncData(ByteBuffer.wrap(response.syncDataResponse));
        assertFalse(response.syncComplete);
        assertTrue(response.syncFailed);
        assertNotNull(response.syncDataResponse);
    }

}
