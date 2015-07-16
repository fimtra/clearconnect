/*
 * Copyright (c) 2015 Ramon Servadei 
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
package com.fimtra.clearconnect.core;

import static org.mockito.Mockito.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.IPlatformRegistryAgent.RegistryNotAvailableException;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.tcpchannel.TcpChannelUtils;

/**
 * Tests for the {@link ShadowRegistry}
 * 
 * @author Ramon Servadei
 */
public class ShadowRegistryTest
{
    private static final int BACKUP_PORT = 33333;
    private static final int PRIMARY_PORT = 32222;
    ShadowRegistry candidate;
    PlatformRegistry primary;

    @Before
    public void setUp() throws Exception
    {
        this.primary = new PlatformRegistry("Test", TcpChannelUtils.LOOPBACK, PRIMARY_PORT);
        this.candidate =
            new ShadowRegistry("Test", new EndPointAddress(TcpChannelUtils.LOOPBACK, PRIMARY_PORT),
                new EndPointAddress(TcpChannelUtils.LOOPBACK, 33333));
    }

    @After
    public void tearDown() throws Exception
    {
        this.primary.destroy();
        this.candidate.destroy();
    }

    @Test
    public void test() throws RegistryNotAvailableException
    {
        IPlatformRegistryAgent agent =
            new PlatformRegistryAgent("ShadowKernelTest", new EndPointAddress(TcpChannelUtils.LOOPBACK, PRIMARY_PORT),
                new EndPointAddress(TcpChannelUtils.LOOPBACK, BACKUP_PORT));
        agent.setRegistryReconnectPeriodMillis(1000);

        IRegistryAvailableListener listener = mock(IRegistryAvailableListener.class);
        agent.addRegistryAvailableListener(listener);
        final int millis = 5000;
        verify(listener, timeout(millis)).onRegistryConnected();

        this.primary.destroy();

        verify(listener, timeout(millis)).onRegistryDisconnected();
        verify(listener, timeout(millis).times(2)).onRegistryConnected();

        this.primary = new PlatformRegistry("Test", TcpChannelUtils.LOOPBACK, PRIMARY_PORT);

        verify(listener, timeout(millis).times(2)).onRegistryDisconnected();
        verify(listener, timeout(millis).times(3)).onRegistryConnected();

        verifyNoMoreInteractions(listener);
    }
}
