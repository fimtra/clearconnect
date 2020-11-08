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
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames;
import com.fimtra.clearconnect.event.EventListenerUtils;
import com.fimtra.clearconnect.event.IFtStatusListener;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.Log;
import com.fimtra.util.TestUtils;
import com.fimtra.util.TestUtils.EventCheckerWithFailureReason;
import com.fimtra.util.TestUtils.EventFailedException;
import com.fimtra.util.ThreadUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        regPort++;
        port++;
        this.candidate = new PlatformRegistry("PlatformRegistryTest", TcpChannelUtils.LOCALHOST_IP, regPort);
        this.candidate.publisher.publishContextConnectionsRecordAtPeriod(100);
    }

    @After
    public void teardown()
    {
        this.candidate.destroy();
    }

    @Test
    public void testMultipleConnectionsRestart() throws IOException, InterruptedException
    {
        final int MAX = 150;
        final AtomicReference<CountDownLatch> connectedLatch = new AtomicReference<CountDownLatch>();
        connectedLatch.set(new CountDownLatch(MAX));
        final AtomicReference<CountDownLatch> disconnectedLatch = new AtomicReference<CountDownLatch>();
        disconnectedLatch.set(new CountDownLatch(MAX));
        final String serviceFamily = "testMultipleConnectionsRestart";
        PlatformRegistryAgent agents[] = new PlatformRegistryAgent[MAX];
        System.err.println("Constructing "+ MAX + " agents with 1 service each...");
        for (int i = 0; i < MAX; i++)
        {
            long time = System.nanoTime();
            final String suffix = i + "-" + System.nanoTime();
            agents[i] = new PlatformRegistryAgent("Test-Agent-" + suffix, TcpChannelUtils.LOCALHOST_IP, regPort);
            agents[i].setRegistryReconnectPeriodMillis(500);
            agents[i].addRegistryAvailableListener(
                EventListenerUtils.synchronizedListener(new IRegistryAvailableListener()
                {
                    @Override
                    public void onRegistryDisconnected()
                    {
                        disconnectedLatch.get().countDown();
                    }

                    @Override
                    public void onRegistryConnected()
                    {
                        connectedLatch.get().countDown();
                    }
                }));
            System.err.println(i + "(" + ((System.nanoTime() - time) / 1_000_000) + "ms)...");

        }

        // create a service too (async)
        Arrays.stream(agents).forEach((a) ->{
            ThreadUtils.newThread(() ->{
                assertTrue(
                        a.createPlatformServiceInstance(serviceFamily, "instance-" + a.getAgentName(),
                                TcpChannelUtils.LOCALHOST_IP, WireProtocolEnum.GZIP,
                                RedundancyModeEnum.FAULT_TOLERANT));
            }, "service-create-" + a.getAgentName()).start();
        });

        try
        {
            System.err.println("Waiting for all agents to be connected...");
            final int timeout = 60;
            assertTrue(connectedLatch.get().await(timeout, TimeUnit.SECONDS));

            System.err.println("Waiting for all services to be registered...");
            // check registration of services
            final AtomicReference<CountDownLatch> servicesLatch = new AtomicReference<CountDownLatch>();
            listenForServices(MAX, serviceFamily, servicesLatch);
            assertTrue("Only got: " + (MAX - servicesLatch.get().getCount()),
                    servicesLatch.get().await(timeout, TimeUnit.SECONDS));

            int STABILISE_PAUSE = 1000;
            System.err.println("Sleeping " + STABILISE_PAUSE + "ms before destroying registry...");
            Log.banner(this,"SLEEPING " + STABILISE_PAUSE + "ms BEFORE DESTROYING REGISTRY");
            Thread.sleep(STABILISE_PAUSE);
            System.err.println("continuing...");

            Log.banner(this,"DESTROYING REGISTRY...");
            this.candidate.destroy();
            Log.banner(this,"DESTROYED REGISTRY...");
            assertTrue(disconnectedLatch.get().await(timeout, TimeUnit.SECONDS));

            System.err.println("Sleeping " + STABILISE_PAUSE + "ms before re-creating registry...");
            Log.banner(this,"SLEEPING " + STABILISE_PAUSE + "ms BEFORE RE-CREATING REGISTRY");
            Thread.sleep(STABILISE_PAUSE);
            System.err.println("continuing...");

            connectedLatch.set(new CountDownLatch(MAX));

            this.candidate = null;
            int i = 0;
            while (this.candidate == null && i++ < 60)
            {
                try
                {
                    this.candidate =
                        new PlatformRegistry("PlatformRegistryTest", TcpChannelUtils.LOCALHOST_IP, regPort);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    Thread.sleep(1000);
                }
            }

            listenForServices(MAX, serviceFamily, servicesLatch);

            System.err.println("Waiting for all agents to re-connect...");
            assertTrue("Only got: " + (MAX - connectedLatch.get().getCount()),
                    connectedLatch.get().await(timeout, TimeUnit.SECONDS));

            System.err.println("Waiting for all services to be re-registered...");
            assertTrue("Only got: " + (MAX - servicesLatch.get().getCount()),
                    servicesLatch.get().await(timeout, TimeUnit.SECONDS));
        }
        finally
        {
            Log.banner(this,"SHUTTING DOWN AGENTS");
            for (PlatformRegistryAgent agent : agents)
            {
                agent.destroy();
            }
        }
    }

    private void listenForServices(int MAX, String serviceFamily,
            AtomicReference<CountDownLatch> servicesLatch)
    {
        servicesLatch.set(new CountDownLatch(1));
        candidate.context.addObserver((image, atomicChange) -> {
            if (image.getSubMapKeys().contains(serviceFamily))
            {
                if (image.getOrCreateSubMap(serviceFamily).values().size() == MAX)
                {
                    servicesLatch.get().countDown();
                }
            }
        }, IRegistryRecordNames.SERVICE_INSTANCES_PER_SERVICE_FAMILY);
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

            agents[i].createPlatformServiceInstance("Test-FTservice", suffix, TcpChannelUtils.LOCALHOST_IP, port++,
                WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT);
            publishRecordAndRpc(suffix, agents[i].getPlatformServiceInstance("Test-FTservice" , suffix));

            // create a load balanced service
            agents[i].createPlatformServiceInstance("Test-LBservice", suffix, TcpChannelUtils.LOCALHOST_IP, port++,
                WireProtocolEnum.STRING, RedundancyModeEnum.LOAD_BALANCED);
            publishRecordAndRpc(suffix, agents[i].getPlatformServiceInstance("Test-LBservice" , suffix));
        }

        final CountDownLatch allConnections = new CountDownLatch(1);
        final CountDownLatch noConnections = new CountDownLatch(1);
        this.candidate.context.addObserver(new IRecordListener()
        {
            boolean connected;

            @Override
            public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
            {
                if (imageValidInCallingThreadOnly.get(
                        PlatformRegistry.IPlatformSummaryRecordFields.SERVICE_INSTANCES).longValue()
                        == (MAX * 2) + 1)
                {
                    this.connected = true;
                    allConnections.countDown();
                }

                // this is for when we destroy the agents
                if (this.connected && imageValidInCallingThreadOnly.get(
                        PlatformRegistry.IPlatformSummaryRecordFields.SERVICE_INSTANCES).longValue() == 1)
                {
                    noConnections.countDown();
                }
            }
        }, IRegistryRecordNames.PLATFORM_SUMMARY);

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
        Thread.sleep(2000);

        // the platform registry adds itself as a service instance
        checkSize(0, 1, this.candidate.serviceInstancesPerServiceFamily);
        checkZeroSize(this.candidate.serviceInstanceStats);
        // the platform registry adds itself as a service
        checkSize(1, 0, this.candidate.services);
        checkZeroSize(this.candidate.eventHandler.monitoredServiceInstances);
        checkZeroSize(this.candidate.eventHandler.pendingMasterInstancePerFtService);
        checkZeroSize(this.candidate.eventHandler.confirmedMasterInstancePerFtService);
        checkZeroSize(this.candidate.eventHandler.pendingPlatformServices); // todo failing here
        checkZeroSize(this.candidate.eventHandler.connectionMonitors);

        // need to wait for connections to be destroyed?
        checkZeroSize(this.candidate.runtimeStatus);
    }

    void publishRecordAndRpc(final String suffix, final IPlatformServiceInstance service)
    {
        ((PlatformServiceInstance) service).publisher.publishContextConnectionsRecordAtPeriod(100);
        final IRecord record = service.getOrCreateRecord("record-" + System.currentTimeMillis() + "-" + suffix);
        record.put("field", System.currentTimeMillis());
        service.publishRecord(record);
        service.publishRPC(new RpcInstance(TypeEnum.DOUBLE, "rpc-" + System.currentTimeMillis() + "-" + suffix));
    }

    private static void checkSize(final int expectedRecordFieldCount, final int expectedSubMapSize,
        final IRecord record) throws EventFailedException, InterruptedException
    {
        TestUtils.waitForEvent(new EventCheckerWithFailureReason()
        {
            @Override
            public Object got()
            {
                return record.size();
            }

            @Override
            public Object expect()
            {
                return expectedRecordFieldCount;
            }

            @Override
            public String getFailureReason()
            {
                return "Got:" + record.keySet();
            }
        }, 10000);

        TestUtils.waitForEvent(new EventCheckerWithFailureReason()
        {
            @Override
            public Object got()
            {
                return record.getSubMapKeys().size();
            }

            @Override
            public Object expect()
            {
                return expectedSubMapSize;
            }

            @Override
            public String getFailureReason()
            {
                return "(Submaps) Got keys:" + record.getSubMapKeys() + ", record=" + record;
            }
        }, 10000);

    }

    private static void checkZeroSize(Map<?, ?> map)
    {
        assertEquals(0, map.size());
    }

    private static void checkZeroSize(IRecord record) throws EventFailedException, InterruptedException
    {
        checkSize(0, 0, record);
    }
}
