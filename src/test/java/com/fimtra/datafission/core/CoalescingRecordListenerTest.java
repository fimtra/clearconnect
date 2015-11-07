/*
 * Copyright (c) 2014 Ramon Servadei 
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
package com.fimtra.datafission.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.CoalescingRecordListener;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.thimble.ThimbleExecutor;
import com.fimtra.util.TestUtils;
import com.fimtra.util.TestUtils.EventChecker;

/**
 * Tests for the {@link CoalescingRecordListener}
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings("unused")
public class CoalescingRecordListenerTest
{

    private final static String name = "test";
    private static final String K1 = "1";
    private static final String K2 = "2";
    private final static String K5 = "5";
    private static final IValue V1 = new DoubleValue(1);
    private static final IValue V1n = new DoubleValue(1.2);
    private static final IValue V2 = new DoubleValue(2);
    private static final IValue V2p = new DoubleValue(2.1);
    private final static IValue V5 = new DoubleValue(5);

    private ThimbleExecutor executor;
    Context candidate;

    @Before
    public void setUp() throws Exception
    {
        this.executor = new ThimbleExecutor(1);
        this.candidate = new Context("testContext");
    }

    @After
    public void tearDown() throws Exception
    {
        this.executor.destroy();
    }

    private IRecordListener wrap(TestCachingAtomicChangeObserver observer)
    {
        return new CoalescingRecordListener(this.executor, observer, name);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testSimpleCoalescing() throws InterruptedException
    {
        final IRecord instance = this.candidate.createRecord(name);
        instance.put(K1, V1);
        instance.put(K2, V2);
        final Map<String, IValue> submap = instance.getOrCreateSubMap("subMap1");
        submap.put(K1, V1);
        this.candidate.publishAtomicChange(instance);

        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        observer.latch = new CountDownLatch(1);
        this.candidate.addObserver(wrap(observer), name);

        assertTrue(observer.latch.await(1, TimeUnit.SECONDS));
        assertEquals(instance, observer.getLatestImage());

        for (int i = 0; i < 100; i++)
        {
            instance.put(K1, i);
            submap.put(K2, LongValue.valueOf(i));
            this.candidate.publishAtomicChange(instance);
        }

        TestUtils.waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                return observer.getLatestImage().get(K1).longValue();
            }

            @Override
            public Object expect()
            {
                return 99l;
            }
        });

        // verify coalescing worked - we should not get 99 updates!
        assertTrue("Got: " + observer.changes.size(), observer.changes.size() < 70);
        assertEquals(instance, observer.getLatestImage());
    }

    @SuppressWarnings("boxing")
    @Test
    public void testHeavyLoadCoalescing_100000_updates() throws InterruptedException
    {
        final IRecord instance = this.candidate.createRecord(name);

        Map<String, IValue> recordsPerService;
        final int serviceCount = 5;
        final int recordCount = 10000;
        for (int i = 0; i < serviceCount; i++)
        {
            recordsPerService = instance.getOrCreateSubMap("service" + i);
            for (int j = 0; j < recordCount; j++)
            {
                recordsPerService.put("rec" + j, LongValue.valueOf(0));
            }
        }
        this.candidate.publishAtomicChange(instance);

        final TestCachingAtomicChangeObserver observer = new TestCachingAtomicChangeObserver();
        observer.latch = new CountDownLatch(1);
        this.candidate.addObserver(wrap(observer), name);

        assertTrue(observer.latch.await(2, TimeUnit.SECONDS));

        // update all records
        for (int i = 0; i < serviceCount; i++)
        {
            recordsPerService = instance.getOrCreateSubMap("service" + i);
            for (int j = 0; j < recordCount; j++)
            {
                recordsPerService.put("rec" + j, LongValue.valueOf(1));
                this.candidate.publishAtomicChange(instance);
            }
        }

        TestUtils.waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                return observer.getLatestImage().getOrCreateSubMap("service" + (serviceCount - 1)).get(
                    "rec" + (recordCount - 1)).longValue();
            }

            @Override
            public Object expect()
            {
                return 1l;
            }
        });

        // update all records again
        for (int i = 0; i < serviceCount; i++)
        {
            recordsPerService = instance.getOrCreateSubMap("service" + i);
            for (int j = 0; j < recordCount; j++)
            {
                recordsPerService.put("rec" + j, LongValue.valueOf(0));
                this.candidate.publishAtomicChange(instance);
            }
        }
        TestUtils.waitForEvent(new EventChecker()
        {
            @Override
            public Object got()
            {
                return observer.getLatestImage().getOrCreateSubMap("service" + (serviceCount - 1)).get(
                    "rec" + (recordCount - 1)).longValue();
            }

            @Override
            public Object expect()
            {
                return 0l;
            }
        });

        assertTrue("Got: " + observer.changes.size(), observer.changes.size() < 200);
    }
}
