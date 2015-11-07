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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.Record;
import com.fimtra.datafission.core.SubMap;
import com.fimtra.datafission.field.DoubleValue;

/**
 * Test cases for the record
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings({ "unused" })
public class RecordTest
{

    private static final String SUBMAP_KEY = "mk";
    private final static String name = "test";
    private static final String K1 = "1";
    private static final String K2 = "2";
    private final static String K5 = "5";
    private static final IValue V1 = new DoubleValue(1);
    private static final IValue V2 = new DoubleValue(2);
    private final static IValue V5 = new DoubleValue(5);

    Record candidate;

    Context context;

    TestCachingAtomicChangeObserver listener;

    @Before
    public void setUp() throws Exception
    {
        this.context = new Context("testContext");
        this.candidate = (Record) this.context.createRecord(name);
        this.listener = new TestCachingAtomicChangeObserver();
        this.context.addObserver(this.listener, name);
        // wait for the listener to be triggered
        while (this.listener.changes.size() == 0)
        {

        }
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testSnapshot() throws InterruptedException
    {
        addK1K2ToCandidate();

        Record snapshot1 = Record.snapshot(this.candidate);
        assertEquals(this.candidate, snapshot1);
        assertNotSame(this.candidate, snapshot1);

        Record snapshot2 = Record.snapshot(ImmutableSnapshotRecord.create(this.candidate));
        assertEquals(this.candidate, snapshot2);
        assertNotSame(this.candidate, snapshot2);
        assertEquals(snapshot1, snapshot2);
        assertNotSame(snapshot1, snapshot2);

        this.candidate.put(K1, V1);
        this.candidate.put(K2, V5);
        this.candidate.put(K2, V2);
        this.candidate.put(K5, V5);

        assertFalse(this.candidate.equals(snapshot1));
    }

    @Test
    public void testClear() throws InterruptedException
    {
        addK1K2ToCandidate();
        this.context.publishAtomicChange(name).await();
        assertEquals(V1, this.candidate.get(K1));
        assertEquals(V2, this.candidate.get(K2));
        assertEquals("size", 2, this.candidate.size());
        assertEquals("removed events", 0, getLatestChange().getRemovedEntries().size());
        assertEquals("put events", 2, getLatestChange().getPutEntries().size());

        this.listener.reset();
        this.candidate.clear();
        assertTrue(this.candidate.isEmpty());
        this.context.publishAtomicChange(name).await();
        assertEquals("size", 0, this.candidate.size());
        assertEquals("removed events", 2, getLatestChange().getRemovedEntries().size());
        assertEquals("put events", 0, getLatestChange().getPutEntries().size());
    }

    @Test
    public void testContainsKey()
    {
        addK1K2ToCandidate();
        assertFalse("Should not find " + K5, this.candidate.containsKey(K5));
        assertTrue("Should find " + K2, this.candidate.containsKey(K2));
    }

    @Test
    public void testContainsValue()
    {
        addK1K2ToCandidate();
        assertFalse("Should not find " + V5, this.candidate.containsValue(V5));
        assertTrue("Should find " + V2, this.candidate.containsValue(V2));
    }

    @Test
    public void testEntrySet() throws InterruptedException
    {
        this.candidate.put(K1, V1);
        this.candidate.put(K2, V5);
        this.candidate.put(K2, V2);
        this.candidate.put(K5, V5);
        assertEquals("size", 3, this.candidate.size());
        final Collection<?> entrySet = this.candidate.entrySet();
        assertEquals("size", 3, entrySet.size());
        final Iterator<?> iterator = entrySet.iterator();
        int count = 0;
        while (iterator.hasNext())
        {
            count++;
            final Entry<?, ?> next = (Entry<?, ?>) iterator.next();
            if (next.getKey().equals(K5))
            {
                iterator.remove();
            }
        }
        this.context.publishAtomicChange(name).await();
        assertEquals("iterator count", 3, count);
        assertEquals("size", 2, entrySet.size());
        assertEquals("size", 2, this.candidate.size());

        assertEquals("put", 2, getLatestChange().getPutEntries().size());
        assertEquals("removed", 1, getLatestChange().getRemovedEntries().size());
        assertEquals("removed contents: " + getLatestChange().getRemovedEntries(), V5,
            getLatestChange().getRemovedEntries().get(K5));
    }

    @Test
    public void testGet()
    {
        addK1K2ToCandidate();
        assertEquals(this.candidate.get(K1), V1);
        assertNull("Should be null", this.candidate.get(K5));
    }

    @Test
    public void testIsEmpty()
    {
        assertTrue(this.candidate.isEmpty());
        this.candidate.put(K1, V1);
        assertFalse(this.candidate.isEmpty());
    }

    @Test
    public void testKeySet() throws InterruptedException
    {
        this.candidate.put(K1, V1);
        this.candidate.put(K2, V5);
        this.candidate.put(K2, V2);
        this.candidate.put(K5, V5);
        assertEquals("size", 3, this.candidate.size());
        final Collection<?> keySet = this.candidate.keySet();
        doCollectionTest(keySet, K5);
    }

    @Test(expected = NullPointerException.class)
    public void testPutWithNullKey()
    {
        this.candidate.put(null, V1);
    }

    @Test(expected = NullPointerException.class)
    public void testPutWithNullKeyAndValue() throws InterruptedException
    {
        this.candidate.put(null, (IValue) null);
    }

    @Test
    public void testPutWithNullValue() throws InterruptedException
    {
        this.candidate.put(K1, (IValue) null);
        this.context.publishAtomicChange(name).await();
        assertNull("put contents: " + getLatestChange().getPutEntries(), getLatestChange().getPutEntries().get(K1));
    }

    @Test
    public void testPut() throws InterruptedException
    {
        this.candidate.put(K1, V1);
        this.context.publishAtomicChange(name).await();
        assertEquals("put", 1, getLatestChange().getPutEntries().size());
        assertEquals("previous", 1, getLatestChange().getPutEntries().size());
        assertEquals("removed", 0, getLatestChange().getRemovedEntries().size());
        assertEquals("put contents: " + getLatestChange().getPutEntries(), V1,
            getLatestChange().getPutEntries().get(K1));
        assertNull("previous contents: " + getLatestChange().getOverwrittenEntries(),
            getLatestChange().getOverwrittenEntries().get(K1));

        this.listener.reset();

        // test update
        this.candidate.put(K1, V2);
        this.context.publishAtomicChange(name).await();
        assertEquals("put", 1, getLatestChange().getPutEntries().size());
        assertEquals("previous", 1, getLatestChange().getPutEntries().size());
        assertEquals("removed", 0, getLatestChange().getRemovedEntries().size());
        assertEquals("put contents: " + getLatestChange().getPutEntries(), V2,
            getLatestChange().getPutEntries().get(K1));
        assertEquals("previous contents: " + getLatestChange().getOverwrittenEntries(), V1,
            getLatestChange().getOverwrittenEntries().get(K1));
    }

    @Test
    public void testPutWithDuplicates() throws InterruptedException
    {
        this.candidate.put(K1, V1);
        this.context.publishAtomicChange(name).await();
        assertEquals("put", 1, getLatestChange().getPutEntries().size());
        assertEquals("previous", 1, getLatestChange().getPutEntries().size());
        assertEquals("removed", 0, getLatestChange().getRemovedEntries().size());
        assertEquals("put contents: " + getLatestChange().getPutEntries(), V1,
            getLatestChange().getPutEntries().get(K1));
        assertNull("previous contents: " + getLatestChange().getOverwrittenEntries(),
            getLatestChange().getOverwrittenEntries().get(K1));

        this.listener.reset();

        // no change for duplicate
        this.candidate.put(K1, V1);
        this.context.publishAtomicChange(name).await();
        assertEquals("got " + this.listener.changes, 0, this.listener.changes.size());
    }

    @Test
    public void testPutAll() throws InterruptedException
    {
        Map<String, IValue> record = new HashMap<String, IValue>();
        record.put(K1, V1);
        record.put(K2, V2);
        this.candidate.putAll(record);
        this.context.publishAtomicChange(name).await();

        assertEquals("put", 2, getLatestChange().getPutEntries().size());
        assertEquals("previous", 2, getLatestChange().getPutEntries().size());
        assertEquals("removed", 0, getLatestChange().getRemovedEntries().size());

        assertEquals("put contents: " + getLatestChange().getPutEntries(), V1,
            getLatestChange().getPutEntries().get(K1));
        assertEquals("put contents: " + getLatestChange().getPutEntries(), V2,
            getLatestChange().getPutEntries().get(K2));
        assertNull("previous contents: " + getLatestChange().getOverwrittenEntries(),
            getLatestChange().getOverwrittenEntries().get(K1));
        assertNull("previous contents: " + getLatestChange().getOverwrittenEntries(),
            getLatestChange().getOverwrittenEntries().get(K2));

        // check previous values get updated
        record = new HashMap<String, IValue>();
        record.put(K1, V1); // duplicate
        record.put(K2, V1); // change
        this.candidate.putAll(record);
        this.context.publishAtomicChange(name).await();

        assertEquals("put", 1, getLatestChange().getPutEntries().size());
        assertEquals("previous", 1, getLatestChange().getPutEntries().size());
        assertEquals("removed", 0, getLatestChange().getRemovedEntries().size());

        assertEquals("put contents: " + getLatestChange().getPutEntries(), V1,
            getLatestChange().getPutEntries().get(K2));
        assertEquals("previous contents: " + getLatestChange().getOverwrittenEntries(), V2,
            getLatestChange().getOverwrittenEntries().get(K2));
    }

    @Test
    public void testRemoveOfNonExistentKeyValue() throws InterruptedException
    {
        assertNull(this.candidate.remove("sdf1"));
        this.context.publishAtomicChange(name).await();
        assertEquals("removed events", 0, getLatestChange().getRemovedEntries().size());
    }

    @Test
    public void testRemoveKeyWhenAtomicChangeIsPending() throws InterruptedException
    {
        this.candidate.put(K1, V1);
        this.candidate.remove(K1);
        this.context.publishAtomicChange(name).await();
        assertEquals("put events", 0, getLatestChange().getPutEntries().size());
        assertEquals("updated events", 0, getLatestChange().getOverwrittenEntries().size());
        assertEquals("removed events", 1, getLatestChange().getRemovedEntries().size());
    }

    @Test
    public void testRemove() throws InterruptedException
    {
        this.candidate.put(K1, V5); // this will not showup in the listener
        addK1K2ToCandidate();
        assertEquals("size", 2, this.candidate.size());
        this.candidate.remove(new Integer(1123));
        this.candidate.remove(K1);
        assertEquals("size", 1, this.candidate.size());
        this.context.publishAtomicChange(name).await();

        assertEquals("put events: " + getLatestChange().getPutEntries(), 1, getLatestChange().getPutEntries().size());
        assertEquals("removed events", 1, getLatestChange().getRemovedEntries().size());

        assertEquals("put contents: " + getLatestChange().getPutEntries(), V2,
            getLatestChange().getPutEntries().get(K2));
        assertEquals("removed contents: " + getLatestChange().getRemovedEntries(), V1,
            getLatestChange().getRemovedEntries().get(K1));
    }

    @Test
    public void testValues() throws InterruptedException
    {
        addK1K2ToCandidate();
        this.candidate.put(K5, V5);
        assertEquals("size", 3, this.candidate.size());
        final Collection<?> values = this.candidate.values();
        doCollectionTest(values, V5);
    }

    private static Map<String, IValue> createMap(String string, IValue two2)
    {
        Map<String, IValue> record = new HashMap<String, IValue>();
        record.put(string, two2);
        return record;
    }

    /**
     * Helper that checks there are 3 items in the collection, iterates over it and removes an item
     * via the iterator, checks there are 2 items left.
     * 
     * @throws InterruptedException
     */
    private void doCollectionTest(final Collection<?> collection, Object itemToRemove) throws InterruptedException
    {
        assertEquals("size", 3, collection.size());
        final Iterator<?> iterator = collection.iterator();
        int count = 0;
        while (iterator.hasNext())
        {
            count++;
            final Object next = iterator.next();
            if (next.equals(itemToRemove))
            {
                iterator.remove();
            }
        }
        this.context.publishAtomicChange(name).await();
        assertEquals("iterator count", 3, count);
        assertEquals("size", 2, collection.size());
        assertEquals("size", 2, this.candidate.size());
        assertEquals("removed", 1, getLatestChange().getRemovedEntries().size());
        assertEquals("removed contents: " + getLatestChange().getRemovedEntries(), V5,
            getLatestChange().getRemovedEntries().get(K5));
    }

    private IRecordChange getLatestChange()
    {
        return this.listener.changes.get(this.listener.changes.size() - 1);
    }

    @Test
    public void testStoreNullTriggersRemoveEvent() throws InterruptedException
    {
        this.candidate.put(K1, V1);
        this.candidate.put(K1, (IValue) null);
        this.context.publishAtomicChange(name).await();
        assertEquals("removed", 1, getLatestChange().getRemovedEntries().size());
        assertEquals("removed contents: " + getLatestChange().getRemovedEntries(), V1,
            getLatestChange().getRemovedEntries().get(K1));
    }

    @Test
    public void testGetSubMap() throws InterruptedException
    {
        addK1K2ToCandidate();

        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        assertNotNull(subMap);
        subMap.put(K1, V1);
        subMap.put(K2, V2);

        assertEquals(subMap, this.candidate.getOrCreateSubMap(SUBMAP_KEY));

        assertEquals(2, this.candidate.size());
        assertEquals(this.candidate.toString(), V1, this.candidate.get(K1));
        assertEquals(this.candidate.toString(), V2, this.candidate.get(K2));

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 1);
    }

    @Test
    public void testGetSubMapStoreNull() throws InterruptedException
    {
        addK1K2ToCandidate();

        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        assertNotNull(subMap);
        assertNull(subMap.put(K1, V1));
        assertEquals(V1, subMap.put(K1, null));
        assertNull(subMap.put(K1, null));
    }

    @Test
    public void testGetSubMapPutAll() throws InterruptedException
    {
        addK1K2ToCandidate();

        Map<String, IValue> m = new HashMap<String, IValue>();
        m.put(K1, V1);
        m.put(K2, V2);

        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        assertNull(subMap.get(K1));
        assertNull(subMap.get(K2));

        subMap.putAll(m);

        assertEquals(V1, subMap.get(K1));
        assertEquals(V2, subMap.get(K2));
    }

    @Test
    public void testGetSubMapKeys() throws InterruptedException
    {
        addK1K2ToCandidate();

        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        assertEquals(1, this.candidate.getSubMapKeys().size());

        assertNotNull(subMap);
        subMap.put(K1, V1);
        subMap.put(K2, V2);
        assertEquals(1, this.candidate.getSubMapKeys().size());
        assertTrue(this.candidate.getSubMapKeys().contains(SUBMAP_KEY));

        Map<String, IValue> subMap2 = this.candidate.getOrCreateSubMap(SUBMAP_KEY + 2);
        subMap2.put(K1, V1);
        subMap2.put(K2, V2);
        assertEquals(2, this.candidate.getSubMapKeys().size());
        assertTrue(this.candidate.getSubMapKeys().contains(SUBMAP_KEY + 2));

        assertEquals(subMap, this.candidate.getOrCreateSubMap(SUBMAP_KEY));
        assertEquals(subMap2, this.candidate.getOrCreateSubMap(SUBMAP_KEY + 2));

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 2);

        this.candidate.removeSubMap(SUBMAP_KEY);
        assertEquals("Got:" + this.candidate.getSubMapKeys(), 1, this.candidate.getSubMapKeys().size());
        assertTrue(this.candidate.getSubMapKeys().contains(SUBMAP_KEY + 2));

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 1);

        // test removing map keys removes the subMapKey only when the map is empty
        subMap2.remove(K1);
        assertEquals(1, this.candidate.getSubMapKeys().size());
        assertTrue(this.candidate.getSubMapKeys().contains(SUBMAP_KEY + 2));
        subMap2.remove(K2);
        assertEquals(1, this.candidate.getSubMapKeys().size());

        this.context.publishAtomicChange(name).await();
        assertEquals("got " + this.listener.getLatestImage(), 2, this.listener.getLatestImage().size());
    }

    @Test
    public void testGetSubMapRemoveNonExisting() throws InterruptedException
    {
        assertNull(this.candidate.getOrCreateSubMap(SUBMAP_KEY).remove(K1));
    }

    @Test
    public void testGetSubMapGetRemovePut() throws InterruptedException
    {
        addK1K2ToCandidate();
        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        assertNull(subMap.get(K1));
        assertNull(subMap.put(K1, V1));
        assertEquals(V1, subMap.get(K1));
        assertEquals(V1, subMap.put(K1, V5));
        assertEquals(V5, subMap.remove(K1));
        assertNull(subMap.get(K1));
        assertNull(subMap.put(K5, V5));

        subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        assertNull(subMap.get(K1));
        assertEquals(V5, subMap.get(K5));

        assertEquals(V1, this.candidate.get(K1));
        assertEquals(V2, this.candidate.get(K2));
        assertNull(this.candidate.get(SubMap.encodeSubMapKey(SUBMAP_KEY) + K5));
        assertEquals(2, this.candidate.size());
    }

    @Test
    public void testGetSubMapContainsKeyValue() throws InterruptedException
    {
        addK1K2ToCandidate();
        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        assertFalse(subMap.containsKey(K1));
        assertFalse(subMap.containsValue(V1));
        assertFalse(subMap.containsValue(V5));
        subMap.put(K1, V5);
        subMap.put(K2, V5);
        assertTrue(subMap.containsKey(K1));
        assertFalse(subMap.containsValue(V1));
        assertTrue(subMap.containsValue(V5));
    }

    @Test
    public void testGetSubMapClearIsEmptyAndSize() throws InterruptedException
    {
        addK1K2ToCandidate();

        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        assertNotNull(subMap);
        assertTrue(subMap.isEmpty());
        subMap.put(K1, V5);
        subMap.put(K2, V5);
        assertFalse(subMap.isEmpty());
        assertEquals(V5, subMap.get(K1));
        assertEquals("got " + subMap, 2, subMap.size());

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 1);

        assertEquals(2, this.candidate.size());
        assertEquals("got " + this.candidate, 2, this.candidate.getOrCreateSubMap(SUBMAP_KEY).size());
        Map<String, IValue> subMap2 = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        subMap2.clear();
        assertEquals(0, subMap2.size());
        assertTrue(subMap2.isEmpty());
        Map<String, IValue> subMap3 = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        assertEquals(0, subMap3.size());
        assertFalse(this.candidate.isEmpty());
        assertEquals(2, this.candidate.size());

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 0);
    }

    @Test
    public void testGetSubMapEntrySetIteration() throws InterruptedException
    {
        addK1K2ToCandidate();

        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        subMap.put(K1, V5);
        subMap.put(K2, V5);

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 1);

        int entrycount = 0;
        Set<Entry<String, IValue>> entrySet = subMap.entrySet();
        for (Iterator<Entry<String, IValue>> iterator = entrySet.iterator(); iterator.hasNext();)
        {
            Entry<String, IValue> entry = iterator.next();
            entrycount++;
        }
        assertEquals(2, entrycount);
        entrySet = subMap.entrySet();
        for (Iterator<Entry<String, IValue>> iterator = entrySet.iterator(); iterator.hasNext();)
        {
            Entry<String, IValue> entry = iterator.next();
            iterator.remove();
            break;
        }
        assertEquals("got " + this.candidate, 2, this.candidate.size());
        // check non-subMap fields are still there
        assertTrue("got " + this.candidate, this.candidate.containsKey(K1));
        assertTrue("got " + this.candidate, this.candidate.containsKey(K2));

        assertEquals(1, subMap.size());
        assertEquals(1, this.candidate.getOrCreateSubMap(SUBMAP_KEY).size());

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 1);
    }

    @Test
    public void testGetSubMapValuesIteration() throws InterruptedException
    {
        addK1K2ToCandidate();

        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        subMap.put(K1, V5);
        subMap.put(K2, V2);

        Set<IValue> expected = new HashSet<IValue>();
        expected.add(V5);
        expected.add(V2);
        int entrycount = 0;
        Collection<IValue> values = subMap.values();
        for (Iterator<IValue> iterator = values.iterator(); iterator.hasNext();)
        {
            IValue iValue = iterator.next();
            entrycount++;
            assertTrue("Did not find " + iValue + " in " + expected, expected.contains(iValue));
        }
        assertEquals(2, entrycount);
        values = subMap.values();
        for (Iterator<IValue> iterator = values.iterator(); iterator.hasNext();)
        {
            iterator.next();
            iterator.remove();
            break;
        }
        assertEquals("got " + this.candidate, 2, this.candidate.size());
        // check non-subMap fields are still there
        assertTrue("got " + this.candidate, this.candidate.containsKey(K1));
        assertTrue("got " + this.candidate, this.candidate.containsKey(K2));

        assertEquals(1, subMap.size());
        assertEquals(1, this.candidate.getOrCreateSubMap(SUBMAP_KEY).size());

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 1);
    }

    @Test
    public void testGetSubMapKeySetIteration() throws InterruptedException
    {
        addK1K2ToCandidate();

        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        subMap.put(K1, V5);
        subMap.put(K2, V2);

        Set<String> expected = new HashSet<String>();
        expected.add(K1);
        expected.add(K2);
        int entrycount = 0;
        Collection<String> keySet = subMap.keySet();
        for (Iterator<String> iterator = keySet.iterator(); iterator.hasNext();)
        {
            String iValue = iterator.next();
            assertTrue("Did not find " + iValue + " in " + expected, expected.contains(iValue));
            entrycount++;
        }
        assertEquals(2, entrycount);
        keySet = subMap.keySet();
        for (Iterator<String> iterator = keySet.iterator(); iterator.hasNext();)
        {
            String iValue = iterator.next();
            iterator.remove();
            break;
        }
        assertEquals("got " + this.candidate, 2, this.candidate.size());
        // check non-subMap fields are still there
        assertTrue("got " + this.candidate, this.candidate.containsKey(K1));
        assertTrue("got " + this.candidate, this.candidate.containsKey(K2));

        assertEquals(1, subMap.size());
        assertEquals(1, this.candidate.getOrCreateSubMap(SUBMAP_KEY).size());

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 1);
    }

    @Test
    public void testPutSubmapDuplicateKeyAndValue() throws InterruptedException
    {
        addK1K2ToCandidate();

        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        subMap.put(K1, V5);
        this.context.publishAtomicChange(name).await();

        verifyImageSizes(2, 1);

        this.listener.changes.clear();
        this.listener.images.clear();

        // put a duplicate
        subMap.put(K1, V5);
        this.context.publishAtomicChange(name).await();

        assertEquals("got " + this.listener.changes, 0, this.listener.changes.size());
    }

    @Test
    public void testPutClearThenPut() throws InterruptedException
    {
        addK1K2ToCandidate();
        Map<String, IValue> m = new HashMap<String, IValue>();

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 0);

        this.candidate.clear();
        this.listener.reset();

        // re-add
        addK1K2ToCandidate();

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 0);
    }

    @Test
    public void testClearRecordAndSubMap() throws InterruptedException
    {
        addK1K2ToCandidate();
        Map<String, IValue> m = new HashMap<String, IValue>();
        m.put(K1, V1);
        m.put(K2, V2);
        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        subMap.putAll(m);

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 1);

        this.candidate.clear();

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(0, 0);
    }

    @Test
    public void testCloneRecord() throws InterruptedException
    {
        addK1K2ToCandidate();
        Map<String, IValue> m = new HashMap<String, IValue>();
        m.put(K1, V1);
        m.put(K2, V2);
        
        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 0);
        
        Record clone = this.candidate.clone();
        
        assertEquals(this.candidate, clone);
        assertEquals(this.candidate.sequence.get(), clone.sequence.get());
        assertNotSame(this.candidate, clone);
    }
    
    @Test
    public void testCloneRecordAndSubMap() throws InterruptedException
    {
        addK1K2ToCandidate();
        Map<String, IValue> m = new HashMap<String, IValue>();
        m.put(K1, V1);
        m.put(K2, V2);
        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        subMap.putAll(m);

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 1);

        Record clone = this.candidate.clone();

        assertEquals(this.candidate, clone);
        assertEquals(this.candidate.sequence.get(), clone.sequence.get());
        assertNotSame(this.candidate, clone);
        Set<String> subMapKeys = this.candidate.getSubMapKeys();
        for (String subMapKey : subMapKeys)
        {
            assertEquals(this.candidate.getOrCreateSubMap(subMapKey), clone.getOrCreateSubMap(subMapKey));
            assertNotSame(this.candidate.getOrCreateSubMap(subMapKey), clone.getOrCreateSubMap(subMapKey));
        }
    }

    @Test
    public void testRemoveMap() throws InterruptedException
    {
        addK1K2ToCandidate();

        Map<String, IValue> m = new HashMap<String, IValue>();
        m.put(K1, V1);
        m.put(K2, V2);

        Map<String, IValue> subMap = this.candidate.getOrCreateSubMap(SUBMAP_KEY);
        subMap.putAll(m);
        assertEquals(V1, subMap.get(K1));
        assertEquals(V2, subMap.get(K2));

        this.context.publishAtomicChange(name).await();
        verifyImageSizes(2, 1);

        assertEquals(1, this.candidate.getSubMapKeys().size());
        this.candidate.removeSubMap(SUBMAP_KEY);
        assertEquals("got " + this.candidate, 2, this.candidate.size());
        assertEquals(0, this.candidate.getSubMapKeys().size());
        this.context.publishAtomicChange(name).await();
        assertEquals("got " + this.listener.getLatestImage(), 2, this.listener.getLatestImage().size());
    }

    void verifyImageSizes(int recordSize, int submapSize)
    {
        assertEquals("got " + this.listener.getLatestImage(), recordSize, this.listener.getLatestImage().size());
        assertEquals("got " + this.listener.getLatestImage(), submapSize,
            this.listener.getLatestImage().getSubMapKeys().size());
    }

    void addK1K2ToCandidate()
    {
        this.candidate.put(K1, V1);
        this.candidate.put(K2, V2);
        assertEquals(2, this.candidate.size());
    }
}
