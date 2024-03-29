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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.event.EventListenerUtils;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.tcpchannel.TcpChannelUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link PlatformRegistryAgent}
 * 
 * @author Ramon Servadei
 */
public class PlatformRegistryAgentTest
{
    PlatformRegistryAgent candidate;
    PlatformRegistry registry;

    @Before
    public void setUp() throws Exception
    {
        this.registry = new PlatformRegistry("PRA-Test", "localhost", 54321);
    }

    @After
    public void tearDown() throws Exception
    {
        this.registry.destroy();
        this.candidate.destroy();
    }

    @Test
    public void testRetryForReRegistering() throws IOException, InterruptedException
    {
        this.candidate = new PlatformRegistryAgent("test", new EndPointAddress("localhost", 54322),
            new EndPointAddress("localhost", 54321));
        assertTrue(this.candidate.registryProxy.isConnected());
        
        final String serviceFamily = "family";
        final String serviceMember = "member";
        this.candidate.createPlatformServiceInstance(serviceFamily, serviceMember, TcpChannelUtils.LOCALHOST_IP,
            ChannelUtils.getNextAvailableServicePort(), WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT);
        final IPlatformServiceInstance platformServiceInstance1 =
            this.candidate.getPlatformServiceInstance(serviceFamily, serviceMember);

        Thread.sleep(1000);

        // restart the registry
        this.registry.destroy();
        this.registry = new PlatformRegistry("PRA-Test", "localhost", 54322);

        final CountDownLatch connected = new CountDownLatch(1);
        this.candidate.addRegistryAvailableListener(EventListenerUtils.synchronizedListener(new IRegistryAvailableListener()
        {
            @Override
            public void onRegistryDisconnected()
            {

            }

            @Override
            public void onRegistryConnected()
            {
                connected.countDown();
            }
        }));

        assertTrue("Not re-connected", connected.await(10, TimeUnit.SECONDS));
        assertTrue(platformServiceInstance1.isActive());
    }

}
