/*
 * Copyright (c) 2015 Ramon Servadei, Fimtra
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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.tcpchannel.TcpChannelUtils;

/**
 * Tests for the {@link PlatformRegistry}
 * 
 * @author Ramon Servadei
 */
public class PlatformRegistryTest
{
    PlatformRegistry candidate;
    static int regPort = 21212;
    static int port = regPort + 1;

    @Before
    public void setup()
    {
        this.candidate = new PlatformRegistry("PlatformRegistryTest", TcpChannelUtils.LOCALHOST_IP, regPort);
        this.candidate.publisher.publishContextConnectionsRecordAtPeriod(100);
    }

    @After
    public void teardown()
    {
        this.candidate.destroy();
    }

    @Test
    public void testNoLeakage() throws IOException, InterruptedException
    {
        checkEmpty();

        final int MAX = 2;
        PlatformRegistryAgent agents[] = new PlatformRegistryAgent[MAX];
        for (int i = 0; i < MAX; i++)
        {
            final String suffix = i + "-" + System.nanoTime();
            agents[i] = new PlatformRegistryAgent("Test-Agent-" + suffix, TcpChannelUtils.LOCALHOST_IP, regPort);

            agents[i].createPlatformServiceInstance("Test-FTservice-" + i, suffix, TcpChannelUtils.LOCALHOST_IP,
                port++, WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT);
            publishRecordAndRpc(suffix, agents[i].getPlatformServiceInstance("Test-FTservice-" + i, suffix));

            // create a load balanced service
            agents[i].createPlatformServiceInstance("Test-LBservice-" + i, suffix, TcpChannelUtils.LOCALHOST_IP,
                port++, WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT);
            publishRecordAndRpc(suffix, agents[i].getPlatformServiceInstance("Test-LBservice-" + i, suffix));
        }

        final CountDownLatch allConnections = new CountDownLatch(1);
        final CountDownLatch noConnections = new CountDownLatch(1);
        this.candidate.context.addObserver(new IRecordListener()
        {
            boolean connected;
            
            @Override
            public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
            {
                if (imageValidInCallingThreadOnly.getSubMapKeys().size() == MAX * 3)
                {
                    allConnections.countDown();
                    connected = true;
                }
                
                // this is for when we destroy the agents
                if (connected && imageValidInCallingThreadOnly.getSubMapKeys().size() == 0)
                {
                    noConnections.countDown();
                }
            }
        }, IRegistryRecordNames.PLATFORM_CONNECTIONS);

        assertTrue(allConnections.await(10, TimeUnit.SECONDS));

        for (int i = 0; i < MAX; i++)
        {
            agents[i].destroy();
        }

        assertTrue(noConnections.await(10, TimeUnit.SECONDS));

        checkEmpty();
    }

    void checkEmpty() throws InterruptedException
    {
        Thread.sleep(1000);
        
        checkZeroSize(this.candidate.platformConnections);
        checkZeroSize(this.candidate.serviceInstancesPerAgent);
        // the platform registry adds itself as a service instance
        checkSize(0, 1, this.candidate.serviceInstancesPerServiceFamily);
        checkZeroSize(this.candidate.serviceInstanceStats);
        // the platform registry adds itself as a service
        checkSize(1, 0, this.candidate.services);
        checkZeroSize(this.candidate.monitoredServiceInstances);
        checkZeroSize(this.candidate.pendingMasterInstancePerFtService);
        checkZeroSize(this.candidate.confirmedMasterInstancePerFtService);
        checkZeroSize(this.candidate.pendingPlatformServices);
        checkZeroSize(this.candidate.connectionMonitors);

        // the platform registry adds its records as a service instance
        checkSize(0, 1, this.candidate.recordsPerServiceInstance);
        checkZeroSize(this.candidate.rpcsPerServiceInstance);
        // the platform registry adds its records as a service
        checkSize(0, 1, this.candidate.recordsPerServiceFamily);
        checkZeroSize(this.candidate.rpcsPerServiceFamily);

        // need to wait for connections to be destroyed?
        checkZeroSize(this.candidate.runtimeStatus);
    }

    void publishRecordAndRpc(final String suffix, final IPlatformServiceInstance service)
    {
        ((PlatformServiceInstance)service).publisher.publishContextConnectionsRecordAtPeriod(100);
        final IRecord record = service.getOrCreateRecord("record-" + System.currentTimeMillis() + "-" + suffix);
        record.put("field", System.currentTimeMillis());
        service.publishRecord(record);
        service.publishRPC(new RpcInstance(TypeEnum.DOUBLE, "rpc-" + System.currentTimeMillis() + "-" + suffix));
    }

    private static void checkSize(int expectedRecordFieldCount, int expectedSubMapSize, IRecord record)
    {
        assertEquals("Got:" + record.keySet(), expectedRecordFieldCount, record.size());
        assertEquals("(Submaps) Got keys:" + record.getSubMapKeys() + ", record=" + record, expectedSubMapSize,
            record.getSubMapKeys().size());
    }

    private static void checkZeroSize(Map<?, ?> map)
    {
        assertEquals(0, map.size());
    }

    private static void checkZeroSize(IRecord record)
    {
        checkSize(0, 0, record);
    }
}
