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

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.fimtra.datafission.ISessionProtocol;
import com.fimtra.datafission.ISessionProtocol.SyncResponse;

/**
 * Tests for the {@link SimpleSessionProtocol}
 * 
 * @author Ramon Servadei
 */
public class SimpleSessionProtocolTest
{
    @Rule
    public TestName name = new TestName();

    ISessionProtocol proxyEnd;
    ISessionProtocol publisherEnd;

    @Before
    public void setUp() throws Exception
    {
        this.proxyEnd = new SimpleSessionProtocol();
        this.publisherEnd = new SimpleSessionProtocol();
    }

    @Test
    public void testSyncSession()
    {
        // prepare mocks
        ISessionAttributesProvider provider = mock(ISessionAttributesProvider.class);
        final String[] attrs = new String[] { "a1", "a2", "a3" };
        when(provider.getSessionAttributes()).thenReturn(attrs);

        ISessionManager manager = mock(ISessionManager.class);
        final String sessionId = "sesh-" + name.getMethodName();
        when(manager.createSession(eq(attrs))).thenReturn(sessionId);

        ISessionListener listener = mock(ISessionListener.class);

        // register session collaborators
        SessionContexts.registerSessionProvider(name.getMethodName(), provider);
        SessionContexts.registerSessionManager(name.getMethodName(), manager);
        proxyEnd.setSessionListener(listener);

        // start the session sync
        final byte[] syncInit = this.proxyEnd.getSessionSyncStartMessage(name.getMethodName());

        SyncResponse response = this.publisherEnd.handleSessionSyncData(syncInit);
        assertTrue(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNotNull(response.syncDataResponse);

        response = this.proxyEnd.handleSessionSyncData(response.syncDataResponse);
        assertTrue(response.syncComplete);
        assertFalse(response.syncFailed);
        assertNull(response.syncDataResponse);

        verify(listener).onSessionOpen(eq(name.getMethodName()), eq(sessionId));
    }

    @Test
    public void testSyncFail()
    {
        // simulate failed sync
        ISessionManager manager = mock(ISessionManager.class);
        when(manager.createSession((String[]) any())).thenReturn(null);

        SessionContexts.registerSessionManager(name.getMethodName(), manager);

        final byte[] syncInit = this.proxyEnd.getSessionSyncStartMessage(name.getMethodName());

        SyncResponse response = this.publisherEnd.handleSessionSyncData(syncInit);
        assertFalse(response.syncComplete);
        assertTrue(response.syncFailed);
        assertNotNull(response.syncDataResponse);
    }

}
