/*
 * Copyright (c) 2013 Ramon Servadei 
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IPermissionFilter;
import com.fimtra.datafission.IPublisherContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.core.Record;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.util.SubscriptionManager;

/**
 * Tests for {@link ContextUtils}
 * 
 * @author Ramon Servadei
 */
public class ContextUtilsTest
{
    private static final String TEST_SERIALIZE_AND_RESOLVE_CONTEXT = "./testSerializeAndResolveContext";
    private static final String TEST_SERIALIZE_AND_RESOLVE_RECORD = "testSerializeAndResolveRecord";

    @Before
    public void setUp() throws Exception
    {
    }

    @After
    public void tearDown() throws Exception
    {
        File dir = new File(TEST_SERIALIZE_AND_RESOLVE_CONTEXT);
        if (dir.isDirectory())
        {
            for (File file : dir.listFiles())
            {
                file.delete();
            }
            dir.delete();
        }
        File recordFile = new File(TEST_SERIALIZE_AND_RESOLVE_RECORD + ".record");
        if (recordFile.exists())
        {
            recordFile.delete();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalCharacters()
    {
        assertTrue(ContextUtils.checkLegalCharacters("hello"));
        ContextUtils.checkLegalCharacters("hello $\\");
    }

    @Test
    public void testFieldCopy()
    {
        Context c = new Context("ContextUtilsTest");
        final IRecord record = c.createRecord("fieldCopy");
        Map<String, IValue> source = new HashMap<String, IValue>();
        ContextUtils.fieldCopy(source, "lasers", record, "lasers");
        assertNull(record.get("lasers"));
        TextValue value = TextValue.valueOf("firing");
        source.put("lasers", value);
        ContextUtils.fieldCopy(source, "lasers", record, "lasers");
        assertEquals(value, record.get("lasers"));
    }

    @Test
    public void testSerializeAndResolveRecord() throws IOException
    {
        Context c = new Context("ContextUtilsTest");
        final IRecord record = c.createRecord(TEST_SERIALIZE_AND_RESOLVE_RECORD);
        updateRecordNoSubmaps(record);
        File dir = new File(".");
        ContextUtils.serializeRecordToFile(record, dir);

        Context c2 = new Context("C2");
        final IRecord resolvedRecord = c2.createRecord(TEST_SERIALIZE_AND_RESOLVE_RECORD);
        IRecordListener observer = mock(IRecordListener.class);
        c2.addObserver(observer, TEST_SERIALIZE_AND_RESOLVE_RECORD);
        verify(observer, timeout(1000)).onChange(any(IRecord.class), any(IRecordChange.class));
        reset(observer);
        ContextUtils.resolveRecordFromFile(resolvedRecord, dir);
        c2.publishAtomicChange(resolvedRecord);
        verify(observer, timeout(1000)).onChange(any(IRecord.class), any(IRecordChange.class));

        assertEquals(((Record) record).data, ((Record) resolvedRecord).data);
        assertEquals(((Record) record).subMaps, ((Record) resolvedRecord).subMaps);
    }

    @Test
    public void testSerializeAndResolveRecordWithSubmaps() throws IOException
    {
        Context c = new Context("ContextUtilsTest");
        final IRecord record = c.createRecord(TEST_SERIALIZE_AND_RESOLVE_RECORD);
        updateRecord(record);
        File dir = new File(".");
        ContextUtils.serializeRecordToFile(record, dir);

        Context c2 = new Context("C2");
        final IRecord resolvedRecord = c2.createRecord(TEST_SERIALIZE_AND_RESOLVE_RECORD);
        IRecordListener observer = mock(IRecordListener.class);
        c2.addObserver(observer, TEST_SERIALIZE_AND_RESOLVE_RECORD);
        verify(observer, timeout(1000)).onChange(any(IRecord.class), any(IRecordChange.class));
        reset(observer);
        ContextUtils.resolveRecordFromFile(resolvedRecord, dir);
        c2.publishAtomicChange(resolvedRecord);
        verify(observer, timeout(1000)).onChange(any(IRecord.class), any(IRecordChange.class));

        assertEquals(((Record) record).data, ((Record) resolvedRecord).data);
        assertEquals(((Record) record).subMaps, ((Record) resolvedRecord).subMaps);
    }

    @Test
    public void testDeleteRecordFile() throws IOException
    {
        Context c = new Context("ContextUtilsTest");
        final IRecord record = c.createRecord(TEST_SERIALIZE_AND_RESOLVE_RECORD);
        File dir = new File(".");
        ContextUtils.serializeRecordToFile(record, dir);

        assertTrue(Arrays.asList(dir.list()).contains(TEST_SERIALIZE_AND_RESOLVE_RECORD + ".record"));
        ContextUtils.deleteRecordFile(record.getName(), dir);
        assertFalse(Arrays.asList(dir.list()).contains(TEST_SERIALIZE_AND_RESOLVE_RECORD + ".record"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCannotResolveImmutableRecord() throws IOException
    {
        Context c = new Context("ContextUtilsTest");
        final IRecord record = c.createRecord(TEST_SERIALIZE_AND_RESOLVE_RECORD);
        updateRecord(record);
        File dir = new File(".");
        ContextUtils.serializeRecordToFile(record, dir);

        Context c2 = new Context("C2");
        final IRecord resolvedRecord = c2.createRecord(TEST_SERIALIZE_AND_RESOLVE_RECORD).getImmutableInstance();
        ContextUtils.resolveRecordFromFile(resolvedRecord, dir);
    }

    @Test
    public void testMergeDemergeMaps()
    {
        Map<String, IValue> map = new HashMap<String, IValue>();
        map.put("KEY1", new TextValue("Value1"));
        map.put("KEY2", new DoubleValue(0.1324d));
        map.put("KEY3", LongValue.valueOf(543285734l));

        Map<String, Map<String, IValue>> subMaps = new HashMap<String, Map<String, IValue>>();
        subMaps.put("subMap1", new HashMap<String, IValue>(map));
        subMaps.put("subMap2", new HashMap<String, IValue>(map));

        final Map<?, ?>[] result = ContextUtils.demergeMaps(ContextUtils.mergeMaps(map, subMaps));

        assertEquals("data map", map, result[0]);
        assertEquals("subMaps", subMaps, result[1]);
    }

    static void updateRecord(final IRecord record)
    {
        record.put("KEY1", new TextValue("Value1"));
        record.put("KEY2", new DoubleValue(0.1324d));
        record.put("KEY3", LongValue.valueOf(543285734l));

        final Map<String, IValue> subMap = record.getOrCreateSubMap("busmap");
        subMap.put("KEY!", new TextValue("Value1"));

        Random rnd = new Random();
        final int limit = rnd.nextInt(200);
        for (int i = 0; i < limit; i++)
        {
            record.put("KEY" + i, rnd.nextBoolean() ? new TextValue("value" + rnd.nextInt()) : rnd.nextBoolean()
                ? LongValue.valueOf(rnd.nextLong()) : new DoubleValue(rnd.nextDouble()));

            if (rnd.nextBoolean())
            {
                subMap.put("KEY" + i, rnd.nextBoolean() ? new TextValue("value" + rnd.nextInt()) : rnd.nextBoolean()
                    ? LongValue.valueOf(rnd.nextLong()) : new DoubleValue(rnd.nextDouble()));
            }
        }
    }

    static void updateRecordNoSubmaps(final IRecord record)
    {
        record.put("KEY1", new TextValue("Value1"));
        record.put("KEY2", new DoubleValue(0.1324d));
        record.put("KEY3", LongValue.valueOf(543285734l));

        Random rnd = new Random();
        final int limit = rnd.nextInt(200);
        for (int i = 0; i < limit; i++)
        {
            record.put("KEY" + i, rnd.nextBoolean() ? new TextValue("value" + rnd.nextInt()) : rnd.nextBoolean()
                ? LongValue.valueOf(rnd.nextLong()) : new DoubleValue(rnd.nextDouble()));
        }
    }

    @Test
    public void testSerializeAndResolveContext() throws IOException
    {
        Context c = new Context("ContextUtilsTest");
        final IRecord record1 = c.createRecord("rec1");
        final IRecord record2 = c.createRecord("rec2");
        updateRecord(record1);
        updateRecord(record2);
        File dir = new File(TEST_SERIALIZE_AND_RESOLVE_CONTEXT);
        dir.mkdir();
        ContextUtils.serializeContextToDirectory(c, dir);

        Context c2 = new Context("C2");
        ContextUtils.resolveContextFromDirectory(c2, dir);

        assertEquals(((Record) record1).data, ((Record) c2.getRecord("rec1")).data);
        assertEquals(((Record) record1).subMaps, ((Record) c2.getRecord("rec1")).subMaps);
        assertEquals(((Record) record2).data, ((Record) c2.getRecord("rec2")).data);
        assertEquals(((Record) record2).subMaps, ((Record) c2.getRecord("rec2")).subMaps);
    }

    @Test
    public void testResubscribeRecordsForContext()
    {
        SubscriptionManager<String, IRecordListener> recordSubscribers =
            new SubscriptionManager<String, IRecordListener>(IRecordListener.class);
        final String rec1 = "record1";
        final String rec2 = "record2";
        IRecordListener listener1 = mock(IRecordListener.class);
        IRecordListener listener2 = mock(IRecordListener.class);
        recordSubscribers.addSubscriberFor(rec1, listener1);
        recordSubscribers.addSubscriberFor(rec1, listener2);
        recordSubscribers.addSubscriberFor(rec2, listener2);

        IObserverContext context = mock(IObserverContext.class);
        ContextUtils.resubscribeRecordsForContext(context, recordSubscribers, new ConcurrentHashMap<String, String>(),
            rec2, rec1);

        verify(context).removeObserver(eq(listener1), eq(rec1));
        verify(context).removeObserver(eq(listener2), eq(rec1));
        verify(context).removeObserver(eq(listener2), eq(rec2));

        verify(context).addObserver(eq(IPermissionFilter.DEFAULT_PERMISSION_TOKEN), eq(listener1), eq(rec1));
        verify(context).addObserver(eq(IPermissionFilter.DEFAULT_PERMISSION_TOKEN), eq(listener2), eq(rec1));
        verify(context).addObserver(eq(IPermissionFilter.DEFAULT_PERMISSION_TOKEN), eq(listener2), eq(rec2));
    }

    @Test
    public void testClearNonSystemRecords()
    {
        IPublisherContext context = mock(IPublisherContext.class);
        Set<String> names = new HashSet<String>();
        names.add(ISystemRecordNames.CONTEXT_CONNECTIONS);
        names.add(ISystemRecordNames.CONTEXT_RECORDS);
        names.add(ISystemRecordNames.CONTEXT_RPCS);
        names.add(ISystemRecordNames.CONTEXT_STATUS);
        names.add(ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);
        names.add(ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS);
        names.add(ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_RECORDS);
        names.add(ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_RPCS);
        names.add(ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_SUBSCRIPTIONS);
        names.add(ProxyContext.RECORD_CONNECTION_STATUS_NAME);
        names.add("lasers");
        when(context.getRecordNames()).thenReturn(names);
        IRecord rec = mock(IRecord.class);
        when(context.getRecord(eq("lasers"))).thenReturn(rec);

        ContextUtils.clearNonSystemRecords(context);

        verify(context).getRecordNames();
        verify(context).getRecord(eq("lasers"));
        verify(rec).clear();
        verify(context).publishAtomicChange(eq(rec));
    }
}
