/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
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

import static com.fimtra.util.TestUtils.waitForEvent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.EndPointAddress;
import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.IPlatformServiceProxy;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.event.EventListenerUtils;
import com.fimtra.clearconnect.event.IFtStatusListener;
import com.fimtra.clearconnect.event.IProxyConnectionListener;
import com.fimtra.clearconnect.event.IRecordSubscriptionListener;
import com.fimtra.clearconnect.event.IRecordSubscriptionListener.SubscriptionInfo;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.clearconnect.event.IServiceAvailableListener;
import com.fimtra.clearconnect.event.IServiceConnectionStatusListener;
import com.fimtra.clearconnect.event.IServiceInstanceAvailableListener;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.core.ImmutableSnapshotRecord;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.Log;
import com.fimtra.util.TestUtils;
import com.fimtra.util.TestUtils.EventChecker;
import com.fimtra.util.TestUtils.EventCheckerWithFailureReason;
import com.fimtra.util.TestUtils.EventFailedException;
import com.fimtra.util.ThreadUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests for the {@link PlatformRegistry} and {@link PlatformRegistryAgent}
 * <p>
 * Its big, its ugly....
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings({ "boxing", "unused" })
public class PlatformTest
{
    @Rule
    public TestName name = new TestName();

    private String logStart()
    {
        System.err.println(this.name.getMethodName());
        return this.name.getMethodName();
    }

    static class TestServiceAvailableListener implements IServiceAvailableListener
    {
        final List<String> available = new ArrayList<>();
        final List<String> unavailable = new ArrayList<>();
        boolean debug = false;

        @Override
        public String toString()
        {
            return " [available=" + this.available + ", unavailable=" + this.unavailable + "]";
        }

        @Override
        public synchronized void onServiceAvailable(String serviceName)
        {
            if (this.debug)
            {
                Log.log(this, ">>>>onServiceAvailable: ", serviceName);
            }
            if ("PlatformRegistry".equals(serviceName))
            {
                return;
            }
            this.available.add(serviceName);
            this.notifyAll();
        }

        @Override
        public synchronized void onServiceUnavailable(String serviceName)
        {
            if (this.debug)
            {
                Log.log(this, ">>>>onServiceUnavailable: ", serviceName);
            }
            if ("PlatformRegistry".equals(serviceName))
            {
                return;
            }
            this.unavailable.add(serviceName);
            this.notifyAll();
        }

        synchronized void verifyOnServiceAvailableCalled(long timeout, String... order)
        {
            checkContains(timeout, this.available, order);
            if (this.debug)
            {
                Log.log(this, ">>>>on after verifyOnServiceAvailableCalled: ", this.available.toString());
            }
        }

        synchronized void verifyOnServiceUnavailableCalled(long timeout, String... order)
        {
            checkContains(timeout, this.unavailable, order);
            if (this.debug)
            {
                Log.log(this, ">>>>on after verifyOnServiceUnavailableCalled: ", this.unavailable.toString());
            }
        }

        synchronized void verifyNoMoreInteractions()
        {
            assertTrue("Got: " + this, this.unavailable.size() == 0 && this.available.size() == 0);
        }

        private synchronized void checkContains(long timeout, List<String> list, String... availableOrder)
        {
            try
            {
                long remains = timeout;
                long start = System.currentTimeMillis();
                final List<String> expected = Arrays.asList(availableOrder);
                boolean containsAll = list.containsAll(expected);

                while (!containsAll)
                {
                    try
                    {
                        wait(remains);
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                    remains = timeout - (System.currentTimeMillis() - start);
                    containsAll = list.containsAll(expected);
                    if (remains <= 0)
                    {
                        break;
                    }
                }
                assertTrue("Got: " + this, containsAll);
            }
            finally
            {
                list.clear();
            }
        }

    }

    static class TestServiceInstanceAvailableListener implements IServiceInstanceAvailableListener
    {
        List<String> available = new CopyOnWriteArrayList<>();
        List<String> unavailable = new CopyOnWriteArrayList<>();

        @Override
        public String toString()
        {
            return " [available=" + this.available + ", unavailable=" + this.unavailable + "]";
        }

        @Override
        public synchronized void onServiceInstanceAvailable(String serviceInstanceId)
        {
            this.available.add(serviceInstanceId);
            this.notify();
        }

        @Override
        public synchronized void onServiceInstanceUnavailable(String serviceInstanceId)
        {
            this.unavailable.add(serviceInstanceId);
            this.notify();
        }

        void verifyOnServiceInstanceAvailableCalled(long timeout, String... order)
        {
            checkContains(timeout, this.available, order);
        }

        void verifyOnServiceInstanceUnavailableCalled(long timeout, String... order)
        {
            checkContains(timeout, this.unavailable, order);
        }

        void verifyNoMoreInteractions()
        {
            assertTrue("Got: " + this, this.unavailable.size() == 0 && this.available.size() == 0);
        }

        private synchronized void checkContains(long timeout, List<String> list, String... availableOrder)
        {
            long remains = timeout;
            long start = System.currentTimeMillis();
            final List<String> expected = Arrays.asList(availableOrder);
            boolean containsAll = list.containsAll(expected);

            while (!containsAll)
            {
                try
                {
                    wait(remains);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                remains = timeout - (System.currentTimeMillis() - start);
                containsAll = list.containsAll(expected);
                if (remains <= 0)
                {
                    break;
                }
            }
            assertTrue("Got: " + this, containsAll);
            list.clear();
        }

    }

    private static final int VERIFY_TIMEOUT = 5000;
    private static final int STD_TIMEOUT = 30000;
    private static final int RECONNECT_PERIOD = 1000;
    private static final String TEST_PLATFORM = "PlatformTestJUnit-";

    String registryHost = TcpChannelUtils.LOCALHOST_IP;
    String agentHost = TcpChannelUtils.LOCALHOST_IP;
    String primary = "PRIMARY";
    String secondary = "SECONDARY";

    // todo consider using full ephemeral ports
    static int servicePort3 = 34001;
    static int servicePort2 = 33001;
    static int servicePort = 32001;
    static int registryPort = 31001;

    PlatformRegistry registry;
    PlatformRegistryAgent agent, agent008;

    @Before
    public void setup()
    {
        Log.log(this, "============== START " + this.name.getMethodName() + " =============================");

        ChannelUtils.WATCHDOG.configure(RECONNECT_PERIOD, 10);

        registryPort += 1;
        servicePort += 1;
        servicePort2 += 1;
        servicePort3 += 1;

        this.registry = new PlatformRegistry(getPlatformName(), this.registryHost, registryPort);
        this.registry.setReconnectPeriodMillis(RECONNECT_PERIOD / 2);
        this.registry.publisher.publishContextConnectionsRecordAtPeriod(RECONNECT_PERIOD / 2);
    }

    void createAgent() throws IOException
    {
        this.agent = new PlatformRegistryAgent(getAgentName(), this.registryHost, registryPort);
        this.agent.setRegistryReconnectPeriodMillis(RECONNECT_PERIOD);
    }

    void createAgent008() throws IOException
    {
        this.agent008 = new PlatformRegistryAgent(getAgentName() + "_008", this.registryHost, registryPort);
        this.agent008.setRegistryReconnectPeriodMillis(RECONNECT_PERIOD);
    }

    @After
    public void teardown() throws InterruptedException
    {
        Log.log(this, "============== START TEAR DOWN " + this.name.getMethodName() + " =============================");

        ThreadUtils.newThread(new Runnable()
        {
            @Override
            public void run()
            {
                PlatformTest.this.registry.destroy();
                if (PlatformTest.this.agent != null)
                {
                    PlatformTest.this.agent.destroy();
                }
                if (PlatformTest.this.agent008 != null)
                {
                    PlatformTest.this.agent008.destroy();
                }

                Log.log(PlatformTest.this, "============== END TEAR DOWN " + PlatformTest.this.name.getMethodName()
                    + " =============================");
            }
        }, "tearDown-" + this.name.getMethodName()).start();

        ChannelUtils.WATCHDOG.configure(5000);
    }

    @Test
    public void testBounceRegistry() throws Exception
    {
        final String SERVICE1 = logStart();
        createAgent();
        createAgent008();
        this.agent008.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT);

        final TestServiceAvailableListener serviceListener = new TestServiceAvailableListener();
        this.agent.addServiceAvailableListener(serviceListener);
        TestServiceInstanceAvailableListener serviceInstanceListener = new TestServiceInstanceAvailableListener();
        this.agent.addServiceInstanceAvailableListener(serviceInstanceListener);

        serviceListener.verifyOnServiceAvailableCalled(STD_TIMEOUT, SERVICE1);
        serviceInstanceListener.verifyOnServiceInstanceAvailableCalled(STD_TIMEOUT,
            PlatformUtils.composePlatformServiceInstanceID(SERVICE1, this.primary));

        // stop the registry
        this.registry.destroy();

        serviceListener.verifyOnServiceUnavailableCalled(STD_TIMEOUT, SERVICE1);
        serviceInstanceListener.verifyOnServiceInstanceUnavailableCalled(STD_TIMEOUT,
            PlatformUtils.composePlatformServiceInstanceID(SERVICE1, this.primary));

        // restart the registry, then check we get our services back
        this.registry = null;
        int i = 0;
        while (this.registry == null && i++ < 60)
        {
            try
            {
                this.registry = new PlatformRegistry(getPlatformName(), this.registryHost, registryPort);
            }
            catch (Exception e)
            {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        this.registry.setReconnectPeriodMillis(RECONNECT_PERIOD / 2);
        this.registry.publisher.publishContextConnectionsRecordAtPeriod(RECONNECT_PERIOD / 2);

        serviceListener.verifyOnServiceAvailableCalled(STD_TIMEOUT, SERVICE1);
        serviceInstanceListener.verifyOnServiceInstanceAvailableCalled(STD_TIMEOUT,
            PlatformUtils.composePlatformServiceInstanceID(SERVICE1, this.primary));
    }

    @Test(timeout = 30000L)
    public void testWaitForServices() throws IOException, InterruptedException
    {
        final String SERVICE1 = logStart();
        createAgent();
        createAgent008();
        this.agent008.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT);
        this.agent.waitForPlatformService(SERVICE1);
    }

    @Test(timeout = 30000L)
    public void testWaitForServicesNull() throws IOException, InterruptedException
    {
        final String SERVICE1 = logStart();
        createAgent();
        createAgent008();
        this.agent008.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT);
        this.agent.waitForPlatformService(null);
    }

    @Test
    public void testGetPlatformName() throws IOException, EventFailedException, InterruptedException
    {
        final String SERVICE1 = logStart();
        createAgent();
        verifyPlatformName(this.agent);
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        assertEquals(getPlatformName(),
            this.agent.getPlatformServiceInstance(SERVICE1, this.primary).getPlatformName());
    }

    @Test
    public void testCreateMultipleServiceInstances() throws IOException
    {
        final String SERVICE1 = logStart();
        createAgent();

        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.secondary, this.agentHost, servicePort += 1,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

        assertNotSame("Same service instances!", this.agent.getPlatformServiceInstance(SERVICE1, this.primary),
            this.agent.getPlatformServiceInstance(SERVICE1, this.secondary));
    }

    @Test
    public void testCannotMixRedundancyModeServices() throws IOException
    {
        final String SERVICE1 = logStart();
        createAgent();
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        // todo fails here 2x
        assertFalse(this.agent.createPlatformServiceInstance(SERVICE1, this.secondary, this.agentHost, servicePort += 1,
            WireProtocolEnum.STRING, RedundancyModeEnum.LOAD_BALANCED));
    }

    @Test
    public void testLoadBalancedServices() throws InterruptedException, IOException
    {
        final String SERVICE1 = logStart();
        createAgent();
        createAgent008();

        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort += 1,
            WireProtocolEnum.STRING, RedundancyModeEnum.LOAD_BALANCED));
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.secondary, this.agentHost, servicePort += 1,
            WireProtocolEnum.STRING, RedundancyModeEnum.LOAD_BALANCED));

        // wait for both services to be registered
        waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                final IRecord record = PlatformTest.this.registry.context.getRecord(
                    PlatformRegistry.IRegistryRecordNames.SERVICE_INSTANCES_PER_SERVICE_FAMILY);
                if (record == null)
                {
                    return null;
                }
                return record.getOrCreateSubMap(SERVICE1).size();
            }

            @Override
            public Object expect()
            {
                return 2;
            }
        });

        IPlatformServiceInstance s1 = this.agent.getPlatformServiceInstance(SERVICE1, this.primary);
        IPlatformServiceInstance s2 = this.agent.getPlatformServiceInstance(SERVICE1, this.secondary);

        final SubscriptionInfo expectedSubscriptionInfo = new SubscriptionInfo("record1", 1, 0);

        final CountDownLatch s1latch = new CountDownLatch(1);
        IRecordSubscriptionListener s1recordListener =
            EventListenerUtils.synchronizedListener((new IRecordSubscriptionListener()
            {
                @Override
                public void onRecordSubscriptionChange(SubscriptionInfo subscriptionInfo)
                {
                    if (subscriptionInfo.equals(expectedSubscriptionInfo))
                    {
                        s1latch.countDown();
                    }
                }
            }));
        final CountDownLatch s2latch = new CountDownLatch(1);
        IRecordSubscriptionListener s2recordListener =
            EventListenerUtils.synchronizedListener(new IRecordSubscriptionListener()
            {
                @Override
                public void onRecordSubscriptionChange(SubscriptionInfo subscriptionInfo)
                {
                    if (subscriptionInfo.equals(expectedSubscriptionInfo))
                    {
                        s2latch.countDown();
                    }
                }
            });

        s1.addRecordSubscriptionListener(s1recordListener);
        s2.addRecordSubscriptionListener(s2recordListener);

        IRecordListener listener = mock(IRecordListener.class);

        this.agent.waitForPlatformService(SERVICE1);
        this.agent008.waitForPlatformService(SERVICE1);
        IPlatformServiceProxy p1 = this.agent.getPlatformServiceProxy(SERVICE1);
        IPlatformServiceProxy p2 = this.agent008.getPlatformServiceProxy(SERVICE1);

        p1.addRecordListener(listener, "record1");
        p2.addRecordListener(listener, "record1");

        assertTrue("Did not get notified for record1", s1latch.await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
        assertTrue("Did not get notified for record1", s2latch.await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testConnectionsToFaultToleranceService() throws InterruptedException, IOException
    {
        final String SERVICE1 = logStart();
        createAgent();
        createAgent008();
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort += 1,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

        final CountDownLatch latch = new CountDownLatch(1);

        final PlatformServiceInstance platformServiceInstance =
            (PlatformServiceInstance) this.agent.getPlatformServiceInstance(SERVICE1, this.primary);
        platformServiceInstance.addRecordListener((image, change) -> {
            for (String subMapKey : change.getSubMapKeys())
            {
                // the submap has been removed due to the change
                if (!image.containsKey(subMapKey))
                {
                    latch.countDown();
                }
            }
        }, ISystemRecordNames.CONTEXT_CONNECTIONS);

        this.agent008.waitForPlatformService(SERVICE1);
        final IPlatformServiceProxy proxy = this.agent008.getPlatformServiceProxy(SERVICE1);

        IServiceConnectionStatusListener listener = mock(IServiceConnectionStatusListener.class);
        proxy.addServiceConnectionStatusListener(listener);
        verify(listener, timeout(2000).times(1)).onConnected(eq(SERVICE1), anyInt());

        // simulate changing FT master state
        platformServiceInstance.doSetFtState(Boolean.FALSE);

        verify(listener, timeout(1000).times(1)).onDisconnected(eq(SERVICE1), anyInt());
        verify(listener, timeout(2000).times(2)).onConnected(eq(SERVICE1), anyInt());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testFaultToleranceServiceInstanceChangesOverThenDestroyLastService()
        throws InterruptedException, IOException
    {
        final String SERVICE1 = logStart();
        Log.log(this, ">>>>>> START testServiceInstanceChangesOverThenDestroyLastService");
        createAgent();
        try
        {
            int activateTimeout = 5000;
            IFtStatusListener ftStatusListener1 = mock(IFtStatusListener.class);
            IFtStatusListener ftStatusListener2 = mock(IFtStatusListener.class);

            assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost,
                servicePort += 1, WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

            this.agent.getPlatformServiceInstance(SERVICE1, this.primary).addFtStatusListener(ftStatusListener1);

            // check primary is active
            verify(ftStatusListener1, timeout(activateTimeout)).onActive(eq(SERVICE1), eq(this.primary));

            verify(ftStatusListener1, atMost(1)).onStandby(eq(SERVICE1), eq(this.primary));

            // create secondary after primary is confirmed (so we know that secondary is standby for
            // the test)
            assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.secondary, this.agentHost,
                servicePort += 1, WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

            // wait for both instances to be registered - otherwise we can get interleaving
            // available-unavailable-available signals in the test which causes false failures
            TestServiceInstanceAvailableListener serviceInstanceListener = new TestServiceInstanceAvailableListener();
            this.agent.addServiceInstanceAvailableListener(serviceInstanceListener);
            final String serviceInstance1 = PlatformUtils.composePlatformServiceInstanceID(SERVICE1, this.primary);
            final String serviceInstance2 = PlatformUtils.composePlatformServiceInstanceID(SERVICE1, this.secondary);
            serviceInstanceListener.verifyOnServiceInstanceAvailableCalled(STD_TIMEOUT, serviceInstance1,
                serviceInstance2);

            TestServiceAvailableListener serviceListener = new TestServiceAvailableListener();
            this.agent.addServiceAvailableListener(serviceListener);
            serviceListener.verifyOnServiceAvailableCalled(STD_TIMEOUT, SERVICE1);
            serviceListener.verifyNoMoreInteractions();

            this.agent.getPlatformServiceInstance(SERVICE1, this.secondary).addFtStatusListener(ftStatusListener2);
            verify(ftStatusListener2, timeout(activateTimeout).times(1)).onStandby(eq(SERVICE1), eq(this.secondary));

            Log.log(this, ">>>>> destroying SERVICE1 PRIMARY");
            this.agent.destroyPlatformServiceInstance(SERVICE1, this.primary);

            // check secondary is now active
            verify(ftStatusListener2, timeout(activateTimeout)).onActive(eq(SERVICE1), eq(this.secondary));

            this.agent.destroyPlatformServiceInstance(SERVICE1, this.secondary);

            serviceListener.verifyOnServiceUnavailableCalled(STD_TIMEOUT, SERVICE1);
            serviceListener.verifyNoMoreInteractions();

            Log.log(this, ">>>>> recreating SERVICE1 PRIMARY");
            // recreate the first service instance again
            assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost,
                servicePort += 1, WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

            IFtStatusListener ftStatusListener3 = mock(IFtStatusListener.class);
            this.agent.getPlatformServiceInstance(SERVICE1, this.primary).addFtStatusListener(ftStatusListener3);

            serviceListener.verifyOnServiceAvailableCalled(STD_TIMEOUT, SERVICE1);
            serviceListener.verifyNoMoreInteractions();

            verify(ftStatusListener3, timeout(activateTimeout)).onActive(eq(SERVICE1), eq(this.primary));
            verify(ftStatusListener3, atMost(1)).onStandby(eq(SERVICE1), eq(this.primary));

            Log.log(this, ">>>>> destroying SERVICE1 PRIMARY (AGAIN)");
            // destroy it (again!)
            this.agent.destroyPlatformServiceInstance(SERVICE1, this.primary);

            serviceListener.verifyOnServiceUnavailableCalled(STD_TIMEOUT, SERVICE1);
            serviceListener.verifyNoMoreInteractions();

            verifyNoMoreInteractions(ftStatusListener1);
            verifyNoMoreInteractions(ftStatusListener2);
            verifyNoMoreInteractions(ftStatusListener3);
        }
        finally
        {
            Log.log(this, ">>>>>> END testServiceInstanceChangesOverThenDestroyLastService");
        }
    }

    @Test
    public void testDestroyRegistry() throws IOException
    {
        final String SERVICE1 = logStart();
        createAgent();
        TestServiceAvailableListener listener = new TestServiceAvailableListener();
        assertTrue(this.agent.addServiceAvailableListener(listener));
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

        listener.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1);

        this.registry.destroy();

        IPlatformServiceProxy agent_service1Proxy = this.agent.getPlatformServiceProxy(SERVICE1);
        assertNull(agent_service1Proxy);
    }

    @Test
    public void testDestroyProxy() throws IOException
    {
        final String SERVICE1 = logStart();
        createAgent();
        TestServiceAvailableListener listener = new TestServiceAvailableListener();
        assertTrue(this.agent.addServiceAvailableListener(listener));

        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

        listener.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1);

        assertEquals(0, this.agent.getActiveProxies().size());
        IPlatformServiceProxy platformServiceProxy = this.agent.getPlatformServiceProxy(SERVICE1);
        assertNotNull(platformServiceProxy);
        assertEquals(1, this.agent.getActiveProxies().size());
        assertEquals(platformServiceProxy, this.agent.getActiveProxies().get(SERVICE1));

        assertFalse(this.agent.destroyPlatformServiceProxy("no service"));
        assertTrue(this.agent.destroyPlatformServiceProxy(SERVICE1));
        assertFalse(this.agent.destroyPlatformServiceProxy(SERVICE1));
        assertFalse(platformServiceProxy.isActive());
        assertEquals(0, this.agent.getActiveProxies().size());

        IPlatformServiceProxy platformServiceProxy2 = this.agent.getPlatformServiceProxy(SERVICE1);
        assertTrue(platformServiceProxy2.isActive());
        assertNotSame(platformServiceProxy, platformServiceProxy2);
    }

    @Test
    public void testLocalServiceAddRemove() throws IOException
    {
        final String SERVICE1 = logStart();
        final String SERVICE2 = this.name.getMethodName() + "2";
        final String SERVICE3 = this.name.getMethodName() + "3";
        createAgent();
        boolean platformService = this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost,
            servicePort, WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT);
        assertTrue(platformService);
        TestServiceAvailableListener listener = new TestServiceAvailableListener();
        assertTrue(this.agent.addServiceAvailableListener(listener));

        listener.verifyOnServiceAvailableCalled(STD_TIMEOUT, SERVICE1);

        assertTrue(this.agent.createPlatformServiceInstance(SERVICE2, this.primary, this.agentHost, servicePort2,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        listener.verifyOnServiceAvailableCalled(STD_TIMEOUT, SERVICE2);

        boolean destroyPlatformService = this.agent.destroyPlatformServiceInstance(SERVICE1, this.primary);
        assertTrue(destroyPlatformService);

        listener.verifyOnServiceUnavailableCalled(STD_TIMEOUT, SERVICE1);

        assertFalse(this.agent.destroyPlatformServiceInstance(SERVICE1, this.primary));
        assertFalse(this.agent.destroyPlatformServiceInstance(SERVICE3, this.primary));

        listener.verifyNoMoreInteractions();
    }

    @Test
    public void testAddProxyConnectionAvailableListener() throws IOException, InterruptedException
    {
        final String SERVICE1 = logStart();
        createAgent008();
        createAgent();
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

        final PlatformServiceInstance service =
            (PlatformServiceInstance) this.agent.getPlatformServiceInstance(SERVICE1, this.primary);
        IProxyConnectionListener listener = mock(IProxyConnectionListener.class);
        service.addProxyConnectionListener(listener);

        // this is the registry connection
        // testAddProxyConnectionAvailableListener[PRIMARY]->PlatformRegistry[PlatformTestJUnit]@169.254.12.201
        final int timeout = 2000;
        verify(listener, timeout(timeout)).onConnected(anyString());

        // wait for the service to be published
        final IServiceAvailableListener serviceAvailableListener = mock(IServiceAvailableListener.class);
        this.agent.addServiceAvailableListener(serviceAvailableListener);
        verify(serviceAvailableListener, timeout(timeout)).onServiceAvailable(eq("PlatformRegistry"));
        verify(serviceAvailableListener, timeout(timeout)).onServiceAvailable(eq(SERVICE1));

        assertNotNull(this.agent.getPlatformServiceProxy(SERVICE1));
        verify(listener, timeout(timeout)).onConnected(
            eq(PlatformUtils.composeProxyName(SERVICE1, this.agent.getAgentName())));

        assertNotNull(this.agent008.getPlatformServiceProxy(SERVICE1));
        verify(listener, timeout(timeout)).onConnected(
            eq(PlatformUtils.composeProxyName(SERVICE1, this.agent008.getAgentName())));

        assertTrue(this.agent008.destroyPlatformServiceProxy(SERVICE1));
        verify(listener, timeout(timeout)).onDisconnected(
            eq(PlatformUtils.composeProxyName(SERVICE1, this.agent008.getAgentName())));

        // test disconnect then instant re-connect
        assertTrue(this.agent.destroyPlatformServiceProxy(SERVICE1));
        assertNotNull(this.agent.getPlatformServiceProxy(SERVICE1));
        verify(listener, timeout(timeout)).onDisconnected(
            eq(PlatformUtils.composeProxyName(SERVICE1, this.agent.getAgentName())));
        verify(listener, timeout(timeout).times(2)).onConnected(
            eq(PlatformUtils.composeProxyName(SERVICE1, this.agent.getAgentName())));
    }

    @Test
    public void testAddServiceAvailableListenerAfterCreatingService() throws IOException
    {
        final String SERVICE1 = logStart();
        final String SERVICE2 = this.name.getMethodName() + "2";
        final String SERVICE3 = this.name.getMethodName() + "3";
        createAgent();
        TestServiceAvailableListener listener = new TestServiceAvailableListener();
        assertTrue(this.agent.addServiceAvailableListener(listener));

        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        listener.verifyOnServiceAvailableCalled(STD_TIMEOUT, SERVICE1);

        assertTrue(this.agent.createPlatformServiceInstance(SERVICE2, this.primary, this.agentHost, servicePort2,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        listener.verifyOnServiceAvailableCalled(STD_TIMEOUT, SERVICE2);

        assertTrue(this.agent.destroyPlatformServiceInstance(SERVICE1, this.primary));
        listener.verifyOnServiceUnavailableCalled(STD_TIMEOUT, SERVICE1);

        TestServiceAvailableListener listener2 = new TestServiceAvailableListener();
        assertTrue(this.agent.addServiceAvailableListener(listener2));

        // no effect expected
        assertFalse(this.agent.destroyPlatformServiceInstance(SERVICE1, this.primary));
        assertFalse(this.agent.destroyPlatformServiceInstance(SERVICE3, this.primary));

        listener2.verifyOnServiceAvailableCalled(STD_TIMEOUT, SERVICE2);

        listener.verifyNoMoreInteractions();
        listener2.verifyNoMoreInteractions();
    }

    @Test
    public void testCannotCreateDuplicateNamedLocalService() throws IOException
    {
        final String SERVICE1 = logStart();
        createAgent();
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        assertFalse(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
    }

    @Test
    public void testDuplicatePortBetweenAgents() throws IOException
    {
        final String SERVICE1 = logStart();
        final String SERVICE2 = this.name.getMethodName() + "2";
        createAgent();
        createAgent008();
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        assertFalse(this.agent008.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
    }

    @Test
    public void testServiceDetectedBetweenAgents() throws IOException
    {
        final String SERVICE1 = logStart();
        final String SERVICE2 = this.name.getMethodName() + "2";
        createAgent();
        createAgent008();
        TestServiceAvailableListener listener = new TestServiceAvailableListener();
        assertTrue(this.agent.addServiceAvailableListener(listener));
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

        // start another agent, register the service available listener late, check we get all
        // notifications
        TestServiceAvailableListener listener008 = new TestServiceAvailableListener();
        assertFalse(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort2,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE2, this.primary, this.agentHost, servicePort3,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        assertTrue(this.agent008.addServiceAvailableListener(listener008));

        listener.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);
        listener008.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);

        listener.verifyNoMoreInteractions();
        listener008.verifyNoMoreInteractions();
    }

    @Test
    public void testDetectWhenPlatformServiceDies() throws IOException
    {
        final String SERVICE1 = logStart();
        createAgent();
        TestServiceAvailableListener listener = new TestServiceAvailableListener();
        assertTrue(this.agent.addServiceAvailableListener(listener));
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        listener.verifyOnServiceAvailableCalled(STD_TIMEOUT, SERVICE1);

        // this simulates a 'dirty' shutdown
        this.agent.destroyPlatformServiceInstance(SERVICE1, this.primary);

        listener.verifyOnServiceUnavailableCalled(STD_TIMEOUT, SERVICE1);

        listener.verifyNoMoreInteractions();
    }

    @Test
    public void testDetectWhenPlatformServiceInstanceStartedAndDestroyed() throws Exception
    {
        final String SERVICE1 = logStart();
        createAgent();
        createAgent008();
        TestServiceInstanceAvailableListener listener = new TestServiceInstanceAvailableListener();
        TestServiceInstanceAvailableListener listener2 = new TestServiceInstanceAvailableListener();
        assertTrue(this.agent.addServiceInstanceAvailableListener(listener));
        assertTrue(this.agent008.addServiceInstanceAvailableListener(listener2));

        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

        assertTrue(this.agent008.createPlatformServiceInstance(SERVICE1, this.secondary, this.agentHost, servicePort2,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

        String serviceInstance1 = PlatformUtils.composePlatformServiceInstanceID(SERVICE1, this.primary);
        String serviceInstance2 = PlatformUtils.composePlatformServiceInstanceID(SERVICE1, this.secondary);

        listener.verifyOnServiceInstanceAvailableCalled(STD_TIMEOUT, serviceInstance1, serviceInstance2);
        listener2.verifyOnServiceInstanceAvailableCalled(STD_TIMEOUT, serviceInstance1, serviceInstance2);

        final String[] familyAndMember = PlatformUtils.decomposePlatformServiceInstanceID(serviceInstance1);
        IPlatformServiceProxy proxy =
            this.agent.getPlatformServiceInstanceProxy(familyAndMember[0], familyAndMember[1]);
        proxy.setReconnectPeriodMillis(RECONNECT_PERIOD / 2);

        assertNotNull(proxy);

        // check the RPC for the service appears
        int i = 0;
        int maxCheckCount = 300;
        while ((proxy.getAllRpcs() == null || proxy.getAllRpcs().size() == 0) && i++ < maxCheckCount)
        {
            Thread.sleep(100);
        }
        assertTrue("Got: " + proxy.getAllRpcs(), proxy.getAllRpcs().size() > 0);

        // this simulates a 'dirty' shutdown
        this.agent.destroyPlatformServiceInstance(SERVICE1, this.primary);

        listener.verifyOnServiceInstanceUnavailableCalled(STD_TIMEOUT, serviceInstance1);
        listener2.verifyOnServiceInstanceUnavailableCalled(STD_TIMEOUT, serviceInstance1);

        // check the RPC disappears
        i = 0;
        while (proxy.getAllRpcs().size() > 0 && i++ < maxCheckCount)
        {
            Thread.sleep(100);
        }
        assertEquals(0, proxy.getAllRpcs().size());

        // give time for IO to settle
        Thread.sleep(500);

        // re-create service instance 1 - SAME port
        i = 0;
        while (!this.agent008.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT) && i++ < 60)
        {
            Thread.sleep(1000);
        }

        listener.verifyOnServiceInstanceAvailableCalled(STD_TIMEOUT, serviceInstance1);
        listener2.verifyOnServiceInstanceAvailableCalled(STD_TIMEOUT, serviceInstance1);

        // check the RPC for the service appears again
        i = 0;
        while ((proxy.getAllRpcs() == null || proxy.getAllRpcs().size() == 0) && i++ < maxCheckCount)
        {
            Thread.sleep(100);
        }
        final int size = proxy.getAllRpcs().size();
        assertTrue("Got: " + size, size > 0);

        listener.verifyNoMoreInteractions();
        listener2.verifyNoMoreInteractions();
    }

    @Test
    public void testDetectWhenPlatformServiceDiesAndResurrects() throws IOException
    {
        final String SERVICE1 = logStart();
        Log.log(this, ">>>>> START testDetectWhenPlatformServiceDiesAndResurrects");
        createAgent();
        TestServiceAvailableListener listener = new TestServiceAvailableListener();
        assertTrue(this.agent.addServiceAvailableListener(listener));
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

        listener.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1);

        // this simulates a 'dirty' shutdown
        this.agent.destroyPlatformServiceInstance(SERVICE1, this.primary);
        listener.verifyOnServiceUnavailableCalled(STD_TIMEOUT, SERVICE1);

        // re-create
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        listener.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1);

        listener.verifyNoMoreInteractions();
    }

    @Test
    public void testUsingServiceProxiesBetweenServices() throws IOException, InterruptedException
    {
        final String SERVICE1 = logStart();
        final String SERVICE2 = this.name.getMethodName() + "2";
        Log.log(this, ">>>>> START testUsingServiceProxiesBetweenServices");
        createAgent();
        createAgent008();
        try
        {
            TestServiceAvailableListener listener = new TestServiceAvailableListener();
            assertTrue(this.agent.addServiceAvailableListener(listener));
            assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
                WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

            TestServiceAvailableListener listener008 = new TestServiceAvailableListener();
            assertTrue(this.agent008.createPlatformServiceInstance(SERVICE2, this.primary, this.agentHost, servicePort2,
                WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
            assertTrue(this.agent008.addServiceAvailableListener(listener008));

            listener.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);
            listener008.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);

            // get a proxy for each service from each agent
            this.agent.waitForPlatformService(SERVICE1);
            this.agent.waitForPlatformService(SERVICE2);
            IPlatformServiceProxy agent_service1Proxy = this.agent.getPlatformServiceProxy(SERVICE1);
            IPlatformServiceProxy agent_service2Proxy = this.agent.getPlatformServiceProxy(SERVICE2);
            assertNotNull(agent_service1Proxy);
            assertTrue("Should be a proxy service", agent_service1Proxy instanceof PlatformServiceProxy);
            assertNotNull(agent_service2Proxy);
            this.agent008.waitForPlatformService(SERVICE1);
            this.agent008.waitForPlatformService(SERVICE2);
            IPlatformServiceProxy agent008_service1Proxy = this.agent008.getPlatformServiceProxy(SERVICE1);
            IPlatformServiceProxy agent008_service2Proxy = this.agent008.getPlatformServiceProxy(SERVICE2);
            assertNotNull(agent008_service1Proxy);
            assertNotNull(agent008_service2Proxy);
            assertTrue("Should be a proxy service", agent008_service2Proxy instanceof PlatformServiceProxy);

            listener.verifyNoMoreInteractions();
            listener008.verifyNoMoreInteractions();
        }
        finally
        {
            Log.log(this, ">>>>> END testUsingServiceProxiesBetweenServices");
        }
    }

    @Test
    public void testDetectWhenPlatformRegistryDestroyed() throws InterruptedException, IOException
    {
        final String SERVICE1 = logStart();
        final String SERVICE2 = this.name.getMethodName() + "2";
        createAgent();
        createAgent008();
        final AtomicReference<CountDownLatch> agentRegistryConnectedLatch = new AtomicReference<>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> agentRegistryDisconnectedLatch = new AtomicReference<>(new CountDownLatch(1));
        this.agent.addRegistryAvailableListener(new IRegistryAvailableListener()
        {
            @Override
            public void onRegistryDisconnected()
            {
                agentRegistryDisconnectedLatch.get().countDown();
            }

            @Override
            public void onRegistryConnected()
            {
                agentRegistryConnectedLatch.get().countDown();
            }
        });

        assertTrue("Agent not connected to registry?",
            agentRegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
        agentRegistryDisconnectedLatch.set(new CountDownLatch(1));

        TestServiceAvailableListener listener = new TestServiceAvailableListener();
        assertTrue(this.agent.addServiceAvailableListener(listener));
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

        TestServiceAvailableListener listener008 = new TestServiceAvailableListener();
        assertTrue(this.agent008.createPlatformServiceInstance(SERVICE2, this.primary, this.agentHost, servicePort2,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        assertTrue(this.agent008.addServiceAvailableListener(listener008));

        listener.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);
        listener008.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);

        this.registry.destroy();

        assertTrue("Agent not disconnected from registry?",
            agentRegistryDisconnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));

        listener.verifyOnServiceUnavailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);
        listener008.verifyOnServiceUnavailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);
        listener.verifyNoMoreInteractions();
        listener008.verifyNoMoreInteractions();

        // NOTE we can't get a proxy if the registry is down - we don't have the service record
        IPlatformServiceProxy service2Proxy = this.agent.getPlatformServiceProxy(SERVICE2);
        assertNull(service2Proxy);
        IPlatformServiceProxy service1Proxy = this.agent008.getPlatformServiceProxy(SERVICE1);
        assertNull(service1Proxy);

    }

    @Test
    @Ignore
    // this is ignored as it only tests an agent re-connecting
    // see testReconnectToOtherPlatformRegistryAfterActiveOneIsDestroyed
    public void testWithOneAgentOnlyReconnectToOtherPlatformRegistryAfterActiveOneIsDestroyed()
        throws IOException, InterruptedException
    {
        final String SERVICE1 = logStart();
        Log.log(this, ">>>>> START testWithOneAgentOnlyReconnectToOtherPlatformRegistryAfterActiveOneIsDestroyed");

        int oldPort = registryPort;
        int newPort = registryPort += 1;

        EndPointAddress alternate = new EndPointAddress(this.registryHost, newPort);
        this.agent =
            new PlatformRegistryAgent(getAgentName(), new EndPointAddress(this.registryHost, oldPort), alternate);
        this.agent.setRegistryReconnectPeriodMillis(RECONNECT_PERIOD);

        final AtomicReference<CountDownLatch> agentRegistryConnectedLatch = new AtomicReference<>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> agentRegistryDisconnectedLatch = new AtomicReference<>(new CountDownLatch(1));
        this.agent.addRegistryAvailableListener(new IRegistryAvailableListener()
        {
            @Override
            public void onRegistryDisconnected()
            {
                agentRegistryDisconnectedLatch.get().countDown();
            }

            @Override
            public void onRegistryConnected()
            {
                agentRegistryConnectedLatch.get().countDown();
            }
        });

        assertTrue("Agent not connected to registry?",
            agentRegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
        agentRegistryConnectedLatch.set(new CountDownLatch(1));
        agentRegistryDisconnectedLatch.set(new CountDownLatch(1));

        PlatformRegistry otherRegistry = new PlatformRegistry(getPlatformName(), this.registryHost, newPort);
        try
        {
            this.registry.destroy();
            assertTrue("Agent not disconnected from registry?",
                agentRegistryDisconnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
            agentRegistryDisconnectedLatch.set(new CountDownLatch(1));

            // we should be connecting to the other registry...
            assertTrue("Agent not re-connected to other registry?",
                agentRegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
            agentRegistryConnectedLatch.set(new CountDownLatch(1));

            // restart the old registry
            this.registry = new PlatformRegistry(getPlatformName(), this.registryHost, oldPort);

            // destroy the other, check we connect to the new one
            otherRegistry.destroy();
            assertTrue("Agent not disconnected from other registry?",
                agentRegistryDisconnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));

            assertTrue("Agent not re-connected to registry?",
                agentRegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
        }
        finally
        {
            otherRegistry.destroy();
            Log.log(this, ">>>>> END testWithOneAgentOnlyReconnectToOtherPlatformRegistryAfterActiveOneIsDestroyed");
        }
    }

    @Test
    public void testReconnectToOtherPlatformRegistryAfterActiveOneIsDestroyed_twoAgents()
        throws IOException, InterruptedException
    {
        final String SERVICE1 = logStart();
        final String SERVICE2 = this.name.getMethodName() + "2";

        Log.log(this, ">>>>> START testReconnectToOtherPlatformRegistryAfterActiveOneIsDestroyed");

        int oldPort = registryPort;
        int newPort = registryPort += 1;
        EndPointAddress alternate = new EndPointAddress(this.registryHost, newPort);
        // construct the agents...
        this.agent =
            new PlatformRegistryAgent(getAgentName(), new EndPointAddress(this.registryHost, oldPort), alternate);
        this.agent.setRegistryReconnectPeriodMillis(RECONNECT_PERIOD);

        this.agent008 = new PlatformRegistryAgent(getAgentName() + "_008",
            new EndPointAddress(this.registryHost, oldPort), alternate);
        this.agent008.setRegistryReconnectPeriodMillis(RECONNECT_PERIOD);

        // setup the registry available listeners
        final AtomicReference<CountDownLatch> agentRegistryConnectedLatch = new AtomicReference<>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> agentRegistryDisconnectedLatch = new AtomicReference<>(new CountDownLatch(1));
        this.agent.addRegistryAvailableListener(new IRegistryAvailableListener()
        {
            @Override
            public void onRegistryDisconnected()
            {
                agentRegistryDisconnectedLatch.get().countDown();
            }

            @Override
            public void onRegistryConnected()
            {
                agentRegistryConnectedLatch.get().countDown();
            }
        });
        final AtomicReference<CountDownLatch> agent008RegistryConnectedLatch = new AtomicReference<>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> agent008RegistryDisconnectedLatch = new AtomicReference<>(new CountDownLatch(1));
        this.agent008.addRegistryAvailableListener(new IRegistryAvailableListener()
        {
            @Override
            public void onRegistryDisconnected()
            {
                agent008RegistryDisconnectedLatch.get().countDown();
            }

            @Override
            public void onRegistryConnected()
            {
                agent008RegistryConnectedLatch.get().countDown();
            }
        });

        assertTrue("Agent not connected to registry?",
            agentRegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
        agentRegistryConnectedLatch.set(new CountDownLatch(1));
        agentRegistryDisconnectedLatch.set(new CountDownLatch(1));
        assertTrue("Agent008 not connected to registry?",
            agent008RegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
        agent008RegistryConnectedLatch.set(new CountDownLatch(1));
        agent008RegistryDisconnectedLatch.set(new CountDownLatch(1));

        PlatformRegistry otherRegistry = new PlatformRegistry(getPlatformName(), this.registryHost, newPort);
        try
        {
            TestServiceAvailableListener listener = new TestServiceAvailableListener();
            assertTrue(this.agent.addServiceAvailableListener(listener));
            assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
                WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

            TestServiceAvailableListener listener008 = new TestServiceAvailableListener();
            assertTrue(this.agent008.createPlatformServiceInstance(SERVICE2, this.primary, this.agentHost, servicePort2,
                WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
            assertTrue(this.agent008.addServiceAvailableListener(listener008));

            listener.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);
            listener008.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);

            assertTrue(agentRegistryDisconnectedLatch.get().getCount() > 0);
            assertTrue(agent008RegistryDisconnectedLatch.get().getCount() > 0);
            assertTrue(agentRegistryConnectedLatch.get().getCount() > 0);
            assertTrue(agent008RegistryConnectedLatch.get().getCount() > 0);

            this.registry.destroy();

            Thread.sleep(500);

            assertTrue("Agent not disconnected from registry?",
                agentRegistryDisconnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
            agentRegistryDisconnectedLatch.set(new CountDownLatch(1));
            assertTrue("Agent008 not disconnected from registry?",
                agent008RegistryDisconnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
            agent008RegistryDisconnectedLatch.set(new CountDownLatch(1));

            // we should be connecting to the other registry...
            assertTrue("Agent not re-connected to other registry?",
                agentRegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
            agentRegistryConnectedLatch.set(new CountDownLatch(1));
            assertTrue("Agent008 not re-connected to other registry?",
                agent008RegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
            agent008RegistryConnectedLatch.set(new CountDownLatch(1));

            this.agent.waitForPlatformService(SERVICE2);
            IPlatformServiceProxy service2Proxy = this.agent.getPlatformServiceProxy(SERVICE2);
            assertNotNull(service2Proxy);
            this.agent008.waitForPlatformService(SERVICE1);
            IPlatformServiceProxy service1Proxy = this.agent008.getPlatformServiceProxy(SERVICE1);
            assertNotNull(service1Proxy);

            // restart the old registry
            this.registry = null;
            int i = 0;
            while (this.registry == null && i++ < 60)
            {
                try
                {
                    this.registry = new PlatformRegistry(getPlatformName(), this.registryHost, oldPort);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    Thread.sleep(1000);
                }
            }

            IPlatformServiceProxy oldP1 = service1Proxy;
            IPlatformServiceProxy oldP2 = service2Proxy;
            this.agent008.destroyPlatformServiceProxy(SERVICE1);
            this.agent.destroyPlatformServiceProxy(SERVICE2);

            // destroy the other, check we connect to the new one

            assertTrue(agentRegistryDisconnectedLatch.get().getCount() > 0);
            assertTrue(agent008RegistryDisconnectedLatch.get().getCount() > 0);
            assertTrue(agentRegistryConnectedLatch.get().getCount() > 0);
            assertTrue(agent008RegistryConnectedLatch.get().getCount() > 0);

            otherRegistry.destroy();
            assertTrue("Agent not disconnected from other registry?",
                agentRegistryDisconnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
            assertTrue("Agent008 not disconnected from other registry?",
                agent008RegistryDisconnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));

            assertTrue("Agent not re-connected to registry?",
                agentRegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
            assertTrue("Agent008 not re-connected to registry?",
                agent008RegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));

            Log.log(this, ">>>>> this.agent.getPlatformServiceProxy(SERVICE2)");
            this.agent.waitForPlatformService(SERVICE2);
            service2Proxy = this.agent.getPlatformServiceProxy(SERVICE2);
            assertNotNull(service2Proxy);
            // check we don't get the same proxy (we should be a new proxy instance connecting to
            // the new registry)
            assertNotSame(oldP2, service2Proxy);
            Log.log(this, ">>>>> this.agent008.getPlatformServiceProxy(SERVICE1)");
            this.agent008.waitForPlatformService(SERVICE1);
            service1Proxy = this.agent008.getPlatformServiceProxy(SERVICE1);
            assertNotNull(service1Proxy);
            assertNotSame(oldP1, service1Proxy);

            listener.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);
            listener008.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);
            listener.verifyOnServiceUnavailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);
            listener008.verifyOnServiceUnavailableCalled(VERIFY_TIMEOUT, SERVICE1, SERVICE2);
        }
        finally
        {
            otherRegistry.destroy();
            Log.log(this, ">>>>> END testReconnectToOtherPlatformRegistryAfterActiveOneIsDestroyed");
        }
    }

    @Ignore
    @Test
    // note: we ignore this as there is already a test doing this with two agents ->
    // testReconnectToOtherPlatformRegistryAfterActiveOneIsDestroyed_twoAgents
    public void testReconnectToOtherPlatformRegistryAfterActiveOneIsDestroyed() throws IOException, InterruptedException
    {
        final String SERVICE1 = logStart();

        Log.log(this, ">>>>> START testReconnectToOtherPlatformRegistryAfterActiveOneIsDestroyed");

        int oldPort = registryPort;
        int newPort = registryPort += 1;
        EndPointAddress alternate = new EndPointAddress(this.registryHost, newPort);
        // construct the agents...
        this.agent =
            new PlatformRegistryAgent(getAgentName(), new EndPointAddress(this.registryHost, oldPort), alternate);
        this.agent.setRegistryReconnectPeriodMillis(RECONNECT_PERIOD);

        // setup the registry available listeners
        final AtomicReference<CountDownLatch> agentRegistryConnectedLatch = new AtomicReference<>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> agentRegistryDisconnectedLatch = new AtomicReference<>(new CountDownLatch(1));
        this.agent.addRegistryAvailableListener(new IRegistryAvailableListener()
        {
            @Override
            public void onRegistryDisconnected()
            {
                agentRegistryDisconnectedLatch.get().countDown();
            }

            @Override
            public void onRegistryConnected()
            {
                agentRegistryConnectedLatch.get().countDown();
            }
        });

        assertTrue("Agent not connected to registry?",
            agentRegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
        agentRegistryConnectedLatch.set(new CountDownLatch(1));
        agentRegistryDisconnectedLatch.set(new CountDownLatch(1));

        PlatformRegistry otherRegistry = new PlatformRegistry(getPlatformName(), this.registryHost, newPort);
        try
        {
            TestServiceAvailableListener listener = new TestServiceAvailableListener();
            assertTrue(this.agent.addServiceAvailableListener(listener));
            assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
                WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));

            listener.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1);

            assertTrue(agentRegistryDisconnectedLatch.get().getCount() > 0);
            assertTrue(agentRegistryConnectedLatch.get().getCount() > 0);

            this.registry.destroy();

            Thread.sleep(500);

            assertTrue("Agent not disconnected from registry?",
                agentRegistryDisconnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
            agentRegistryDisconnectedLatch.set(new CountDownLatch(1));

            // we should be connecting to the other registry...
            assertTrue("Agent not re-connected to other registry?",
                agentRegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));
            agentRegistryConnectedLatch.set(new CountDownLatch(1));

            this.agent.waitForPlatformService(SERVICE1);
            IPlatformServiceProxy service1Proxy = this.agent.getPlatformServiceProxy(SERVICE1);
            assertNotNull(service1Proxy);

            // restart the old registry
            this.registry = null;
            int i = 0;
            while (this.registry == null && i++ < 60)
            {
                try
                {
                    this.registry = new PlatformRegistry(getPlatformName(), this.registryHost, oldPort);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    Thread.sleep(1000);
                }
            }

            IPlatformServiceProxy oldP2 = service1Proxy;
            this.agent.destroyPlatformServiceProxy(SERVICE1);

            // destroy the other, check we connect to the new one

            assertTrue(agentRegistryDisconnectedLatch.get().getCount() > 0);
            assertTrue(agentRegistryConnectedLatch.get().getCount() > 0);

            otherRegistry.destroy();
            assertTrue("Agent not disconnected from other registry?",
                agentRegistryDisconnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));

            assertTrue("Agent not re-connected to registry?",
                agentRegistryConnectedLatch.get().await(STD_TIMEOUT, TimeUnit.MILLISECONDS));

            this.agent.waitForPlatformService(SERVICE1);
            service1Proxy = this.agent.getPlatformServiceProxy(SERVICE1);
            assertNotNull(service1Proxy);
            // check we don't get the same proxy (we should be a new proxy instance connecting to
            // the new registry)
            assertNotSame(oldP2, service1Proxy);

            listener.verifyOnServiceAvailableCalled(VERIFY_TIMEOUT, SERVICE1);
            listener.verifyOnServiceUnavailableCalled(VERIFY_TIMEOUT, SERVICE1);
        }
        finally
        {
            otherRegistry.destroy();
            Log.log(this, ">>>>> END testReconnectToOtherPlatformRegistryAfterActiveOneIsDestroyed");
        }
    }

    @Test
    public void testPlatformRegistryDefaultPort() throws IOException, EventFailedException, InterruptedException
    {
        final String SERVICE1 = logStart();

        this.registry.destroy();
        this.registry = new PlatformRegistry(getPlatformName(), this.registryHost);

        this.agent = new PlatformRegistryAgent(getAgentName(), this.registry.publisher.getEndPointAddress().getNode());
        this.agent.setRegistryReconnectPeriodMillis(RECONNECT_PERIOD);

        verifyPlatformName(this.agent);
        assertTrue(this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT));
        assertEquals(getPlatformName(),
            this.agent.getPlatformServiceInstance(SERVICE1, this.primary).getPlatformName());
    }

    String getAgentName()
    {
        return "agent:" + PlatformUtils.composeHostQualifiedName() + "-" + this.name.getMethodName();
    }

    String getPlatformName()
    {
        return TEST_PLATFORM + this.name.getMethodName();
    }

    @Test
    public void testPlatformServices() throws InterruptedException, IOException
    {
        final String SERVICE1 = logStart();

        Log.log(this, "START testPlatformServices");

        createAgent();
        createAgent008();

        final AtomicReference<CountDownLatch> serviceLatch = new AtomicReference<>(new CountDownLatch(1));
        final AtomicReference<IRecord> serviceRecordImage = new AtomicReference<>();
        IRecordListener serviceListener = new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                serviceRecordImage.set(ImmutableSnapshotRecord.create(imageCopy));
                serviceLatch.get().countDown();
            }
        };
        this.agent.registryProxy.addObserver(serviceListener, PlatformRegistry.IRegistryRecordNames.SERVICES);

        final long timeoutSecs = 5;
        assertTrue(serviceLatch.get().await(timeoutSecs, TimeUnit.SECONDS));
        assertNotNull("Got: " + serviceRecordImage.get(), serviceRecordImage.get());
        assertEquals("Got: " + serviceRecordImage.get(), 1, serviceRecordImage.get().size());

        serviceLatch.set(new CountDownLatch(1));

        final AtomicReference<CountDownLatch> serviceInstanceLatch = new AtomicReference<>(new CountDownLatch(2));
        final AtomicReference<IRecord> serviceInstanceRecordImage = new AtomicReference<>();
        IRecordListener serviceInstanceListener = new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                serviceInstanceRecordImage.set(ImmutableSnapshotRecord.create(imageCopy));
                serviceInstanceLatch.get().countDown();
            }
        };
        this.agent.registryProxy.addObserver(serviceInstanceListener,
            PlatformRegistry.IRegistryRecordNames.SERVICE_INSTANCES_PER_SERVICE_FAMILY);

        // create the first service instance
        this.agent.createPlatformServiceInstance(SERVICE1, this.primary, this.agentHost, servicePort += 1,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT);

        assertTrue(serviceLatch.get().await(timeoutSecs, TimeUnit.SECONDS));
        assertNotNull("Got: " + serviceRecordImage.get(), serviceRecordImage.get());
        assertEquals("Got: " + serviceRecordImage.get(), 2, serviceRecordImage.get().size());

        assertTrue(serviceInstanceLatch.get().await(timeoutSecs, TimeUnit.SECONDS));
        assertEquals("Got: " + serviceRecordImage.get(), 2, serviceRecordImage.get().size());
        assertNotNull("Got: " + serviceInstanceRecordImage.get(), serviceInstanceRecordImage.get());
        // one service instance is the PlatformRegistry itself
        assertEquals("Got: " + serviceInstanceRecordImage.get(), 2,
            serviceInstanceRecordImage.get().getSubMapKeys().size());
        assertEquals("Got: " + serviceInstanceRecordImage.get(), 1,
            serviceInstanceRecordImage.get().getOrCreateSubMap(SERVICE1).size());

        // create a new instance of the same service
        serviceLatch.set(new CountDownLatch(1));
        serviceInstanceLatch.set(new CountDownLatch(1));
        this.agent008.createPlatformServiceInstance(SERVICE1, this.secondary, this.agentHost, servicePort += 1,
            WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT);

        assertTrue(serviceInstanceLatch.get().await(timeoutSecs, TimeUnit.SECONDS));
        assertNotNull("Got: " + serviceInstanceRecordImage.get(), serviceInstanceRecordImage.get());
        // one service instance is the PlatformRegistry itself
        assertEquals("Got: " + serviceInstanceRecordImage.get(), 2,
            serviceInstanceRecordImage.get().getSubMapKeys().size());
        assertEquals("Got: " + serviceInstanceRecordImage.get(), 2,
            serviceInstanceRecordImage.get().getOrCreateSubMap(SERVICE1).size());

        // destroy an instance of the same service
        serviceLatch.set(new CountDownLatch(1));
        serviceInstanceLatch.set(new CountDownLatch(1));
        this.agent008.destroyPlatformServiceInstance(SERVICE1, this.primary);
        this.agent008.destroyPlatformServiceInstance(SERVICE1, this.secondary);

        assertTrue(serviceInstanceLatch.get().await(timeoutSecs, TimeUnit.SECONDS));
        assertEquals("Got: " + serviceRecordImage.get(), 2, serviceRecordImage.get().size());
        assertNotNull("Got: " + serviceInstanceRecordImage.get(), serviceInstanceRecordImage.get());
        // one service instance is the PlatformRegistry itself
        assertEquals("Got: " + serviceInstanceRecordImage.get(), 2,
            serviceInstanceRecordImage.get().getSubMapKeys().size());
        assertEquals("Got: " + serviceInstanceRecordImage.get(), 1,
            serviceInstanceRecordImage.get().getOrCreateSubMap(SERVICE1).size());

        // destroy the last service
        serviceLatch.set(new CountDownLatch(1));
        serviceInstanceLatch.set(new CountDownLatch(1));
        this.agent.destroyPlatformServiceInstance(SERVICE1, this.primary);

        assertTrue(serviceLatch.get().await(timeoutSecs, TimeUnit.SECONDS));
        TestUtils.waitForEvent(new EventCheckerWithFailureReason()
        {
            @Override
            public Object got()
            {
                return serviceRecordImage.get().size();
            }

            @Override
            public Object expect()
            {
                return 1;
            }

            @Override
            public String getFailureReason()
            {
                return "Got: " + serviceRecordImage.get();
            }
        });

        assertTrue(serviceInstanceLatch.get().await(timeoutSecs, TimeUnit.SECONDS));
        assertEquals("Got: " + serviceRecordImage.get(), 1, serviceRecordImage.get().size());
        assertNotNull("Got: " + serviceInstanceRecordImage.get(), serviceInstanceRecordImage.get());
        // one service instance is the PlatformRegistry itself
        assertEquals("Got: " + serviceInstanceRecordImage.get(), 1,
            serviceInstanceRecordImage.get().getSubMapKeys().size());
    }

    void waitForPrimaryToBeActive(final String SERVICE1)
    {
        int activateTimeout = 10000;
        IFtStatusListener ftStatusListener1 = mock(IFtStatusListener.class);
        this.agent.getPlatformServiceInstance(SERVICE1, this.primary).addFtStatusListener(ftStatusListener1);
        verify(ftStatusListener1, timeout(activateTimeout)).onActive(eq(SERVICE1), eq(this.primary));
        // when adding a listener, we are not guaranteed to be standby
        verify(ftStatusListener1, atMost(2)).onStandby(eq(SERVICE1), eq(this.primary));
    }

    private final static void checkFieldLongValue(final IRecord record, final String fieldName, final long expect)
        throws InterruptedException, EventFailedException
    {
        waitForEvent(new EventChecker()
        {
            @Override
            public Object expect()
            {
                return expect;
            }

            @Override
            public Object got()
            {
                try
                {
                    return record.get(fieldName).longValue();
                }
                catch (NullPointerException e)
                {
                    return -1L;
                }
            }
        });
    }

    private static void ensureRpcCount(int i, IPlatformServiceInstance... instances) throws InterruptedException
    {
        for (IPlatformServiceInstance instance : instances)
        {
            while (instance.getAllRpcs().size() < i)
            {
                Log.log(PlatformTest.class, "Waiting for RPCs count for " + instance);
                Thread.sleep(50);
            }
        }
    }

    private static void checkRecordSize(final IRecord record, final int expect)
        throws InterruptedException, EventFailedException
    {
        waitForEvent(new EventCheckerWithFailureReason()
        {
            @Override
            public Object expect()
            {
                return expect;
            }

            @Override
            public Object got()
            {
                try
                {
                    return record.size();
                }
                catch (NullPointerException e)
                {
                    return -1;
                }
            }

            @Override
            public String getFailureReason()
            {
                return "Record was: " + record.toString();
            }
        });
    }

    private static void checkRecordSubmapKeySize(final AtomicReference<IRecord> record, final int expect)
        throws InterruptedException, EventFailedException
    {
        waitForEvent(new EventCheckerWithFailureReason()
        {
            @Override
            public Object expect()
            {
                return expect;
            }

            @Override
            public Object got()
            {
                try
                {
                    return record.get().getSubMapKeys().size();
                }
                catch (NullPointerException e)
                {
                    return -1;
                }
            }

            @Override
            public String getFailureReason()
            {
                return "Record was: " + record.toString();
            }
        });
    }

    private void verifyPlatformName(final PlatformRegistryAgent agent) throws EventFailedException, InterruptedException
    {
        waitForEvent(new EventChecker()
        {
            @Override
            public Object expect()
            {
                return getPlatformName();
            }

            @Override
            public Object got()
            {
                return agent.getPlatformName();
            }
        });
    }
}
