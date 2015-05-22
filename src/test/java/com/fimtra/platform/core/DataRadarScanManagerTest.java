/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
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
package com.fimtra.platform.core;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.datafission.field.ValueComparatorEnum;
import com.fimtra.platform.WireProtocolEnum;
import com.fimtra.platform.core.DataRadarScanManager;
import com.fimtra.platform.core.DataRadarSpecification;
import com.fimtra.platform.core.PlatformRegistry;
import com.fimtra.platform.core.PlatformServiceInstance;
import com.fimtra.platform.event.IRecordAvailableListener;
import com.fimtra.platform.expression.AndExpression;
import com.fimtra.platform.expression.DataSignatureExpression;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ThreadUtils;

import static org.mockito.Matchers.eq;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Live-fire tests for the {@link DataRadarScanManager}
 * 
 * @author Ramon Servadei
 */
public class DataRadarScanManagerTest
{
    private static final int TIMEOUT_SECS = 5;
    private static final String FIELD_LONG1 = "fieldLong1";
    private static int PORT = 21110;
    private static final String PLATFORM_NAME = "DataSignatureScannerFunctionalTest";
    PlatformRegistry registry;
    PlatformServiceInstance service;
    DataRadarScanManager candidate;

    ScheduledExecutorService executor;

    @After
    public void tearDown() throws Exception
    {
        ThreadUtils.newThread(new Runnable()
        {
            @Override
            public void run()
            {
                DataRadarScanManagerTest.this.registry.destroy();
                DataRadarScanManagerTest.this.service.destroy();
                DataRadarScanManagerTest.this.executor.shutdownNow();
            }
        }, "tearDown").start();
        // IO sensitive
        Thread.sleep(100);
    }

    @Before
    public void setUp() throws Exception
    {
        PORT += TIMEOUT_SECS;
        this.executor = ThreadUtils.newScheduledExecutorService(getClass().getSimpleName(), TIMEOUT_SECS);
        this.registry = new PlatformRegistry(PLATFORM_NAME, TcpChannelUtils.LOOPBACK, PORT);
        PORT += TIMEOUT_SECS;
        this.service =
            new PlatformServiceInstance(null, "TestPlatformService", "PRIMARY", WireProtocolEnum.STRING,
                TcpChannelUtils.LOOPBACK, PORT);
        this.candidate = this.service.dataRadarScanManager;

        // setup records with constantly changing data:
        // count 0->10, then repeat
        final String name = "record1";
        final IRecord r1 = this.service.getOrCreateRecord(name);
        final String fl1 = FIELD_LONG1;
        r1.put(fl1, 0);
        long delay = 50;
        this.executor.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                long value = r1.get(fl1).longValue();
                if (value > 10)
                {
                    value = 0;
                }
                r1.put(fl1, ++value);
                if (value % 2 == 0)
                {
                    DataRadarScanManagerTest.this.service.publishRecord(r1);
                }
            }
        }, delay, delay, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testDestroy() throws Exception
    {
        final String dataRadarRecordName = "testDestroy";

        DataRadarSpecification spec =
            new DataRadarSpecification(dataRadarRecordName, new DataSignatureExpression(FIELD_LONG1,
                ValueComparatorEnum.GT, LongValue.valueOf(5)));

        this.service.getAllRpcs().get(DataRadarScanManager.RPC_REGISTER_DATA_RADAR_SPECIFICATION).execute(
            TextValue.valueOf(spec.toWireString()));

        final IRecordAvailableListener recordAvailableListener = mock(IRecordAvailableListener.class);
        this.service.addRecordAvailableListener(recordAvailableListener);

        final CountDownLatch emptyCount = new CountDownLatch(2);
        final CountDownLatch matchCount = new CountDownLatch(2);
        this.service.addRecordListener(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                Log.log(this, "GOT: " + imageCopy);
                if (imageCopy.size() == 0)
                {
                    emptyCount.countDown();
                }
                else
                {
                    matchCount.countDown();
                }
            }
        }, dataRadarRecordName);

        assertTrue(emptyCount.await(TIMEOUT_SECS, TimeUnit.SECONDS));
        assertTrue(matchCount.await(TIMEOUT_SECS, TimeUnit.SECONDS));

        this.candidate.destroy();

        verify(recordAvailableListener).onRecordAvailable(eq(dataRadarRecordName));
        verify(recordAvailableListener, timeout(1000)).onRecordUnavailable(eq(dataRadarRecordName));

        assertEquals(0, this.candidate.dataRadarScans.size());
        assertEquals(0, this.candidate.dataRadarScansPerField.size());

    }

    @Test
    public void testRegisterAndDeregisterSimpleDataRadar() throws Exception
    {
        final String dataRadarRecordName = "testRegisterAndDeregisterSimpleDataRadar";

        DataRadarSpecification spec =
            new DataRadarSpecification(dataRadarRecordName, new DataSignatureExpression(FIELD_LONG1,
                ValueComparatorEnum.GT, LongValue.valueOf(5)));

        doRegisterDeregisterTest(dataRadarRecordName, spec);
    }

    @Test
    public void testRegisterAndDeregisterCompoundDataRadar() throws Exception
    {
        final String dataRadarRecordName = "testRegisterAndDeregisterCompountDataRadar";

        DataRadarSpecification spec =
            new DataRadarSpecification(dataRadarRecordName, new AndExpression(new DataSignatureExpression(FIELD_LONG1,
                ValueComparatorEnum.GT, LongValue.valueOf(5)), new DataSignatureExpression(FIELD_LONG1,
                ValueComparatorEnum.GT, LongValue.valueOf(7))));

        doRegisterDeregisterTest(dataRadarRecordName, spec);
    }

    void doRegisterDeregisterTest(final String dataRadarRecordName, DataRadarSpecification spec)
        throws TimeOutException, ExecutionException, InterruptedException
    {
        this.service.getAllRpcs().get(DataRadarScanManager.RPC_REGISTER_DATA_RADAR_SPECIFICATION).execute(
            TextValue.valueOf(spec.toWireString()));

        final IRecordAvailableListener recordAvailableListener = mock(IRecordAvailableListener.class);
        this.service.addRecordAvailableListener(recordAvailableListener);

        final CountDownLatch emptyCount = new CountDownLatch(2);
        final CountDownLatch matchCount = new CountDownLatch(2);
        this.service.addRecordListener(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                Log.log(this, "GOT: " + imageCopy);
                if (imageCopy.size() == 0)
                {
                    emptyCount.countDown();
                }
                else
                {
                    matchCount.countDown();
                }
            }
        }, dataRadarRecordName);

        assertTrue(emptyCount.await(TIMEOUT_SECS, TimeUnit.SECONDS));
        assertTrue(matchCount.await(TIMEOUT_SECS, TimeUnit.SECONDS));

        this.service.getAllRpcs().get(DataRadarScanManager.RPC_DEREGISTER_DATA_RADAR_SPECIFICATION).execute(
            TextValue.valueOf(spec.toWireString()));

        verify(recordAvailableListener).onRecordAvailable(eq(dataRadarRecordName));
        verify(recordAvailableListener, timeout(1000)).onRecordUnavailable(eq(dataRadarRecordName));

        assertEquals(0, this.candidate.dataRadarScans.size());
        assertEquals(0, this.candidate.dataRadarScansPerField.size());
    }

    @Test
    public void testRegisterDataSignatureScanner_receivesInitialState() throws Exception
    {
        final String dataRadarRecordName = "lateJoiner";

        final IRecord rec1 = this.service.getOrCreateRecord("rec1");
        rec1.put(FIELD_LONG1, 900);
        this.service.publishRecord(rec1);

        final CountDownLatch latch = new CountDownLatch(1);
        this.service.addRecordListener(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                Log.log(this, imageCopy.toString());
                if (imageCopy.containsKey(FIELD_LONG1))
                {
                    latch.countDown();
                }
            }
        }, rec1.getName());

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        DataRadarSpecification spec =
            new DataRadarSpecification(dataRadarRecordName, new DataSignatureExpression(FIELD_LONG1,
                ValueComparatorEnum.GT, LongValue.valueOf(400)));

        this.service.getAllRpcs().get(DataRadarScanManager.RPC_REGISTER_DATA_RADAR_SPECIFICATION).execute(
            TextValue.valueOf(spec.toWireString()));

        final IRecordAvailableListener recordAvailableListener = mock(IRecordAvailableListener.class);
        this.service.addRecordAvailableListener(recordAvailableListener);

        final CountDownLatch matchCount = new CountDownLatch(1);
        this.service.addRecordListener(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                Log.log(this, "GOT: " + imageCopy);
                if (imageCopy.size() == 0)
                {
                }
                else
                {
                    matchCount.countDown();
                }
            }
        }, dataRadarRecordName);

        assertTrue(matchCount.await(TIMEOUT_SECS, TimeUnit.SECONDS));
    }
}
