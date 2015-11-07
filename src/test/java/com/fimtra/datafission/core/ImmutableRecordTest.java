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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.ImmutableRecord;
import com.fimtra.datafission.core.Record;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;

/**
 * Tests for the {@link ImmutableRecord}
 * 
 * @author Ramon Servadei
 */
public class ImmutableRecordTest
{

    private static final LongValue SUB_MAP_V1 = LongValue.valueOf(546);
    static final String CTX_NAME = "ctx1";
    private static final Context CONTEXT = new Context(CTX_NAME);
    static final String NAME = "imRec1";
    private static final LongValue V2 = LongValue.valueOf(34);
    private static final LongValue V1 = LongValue.valueOf(12);
    private static final TextValue VALUE_3 = TextValue.valueOf("value3");
    private static final TextValue VALUE_4 = TextValue.valueOf("value4");
    private static final String KEY_4 = "k4";
    private static final String KEY_3 = "K3";
    private static final String KEY2 = "key2";
    private static final String KEY1 = "key1";
    private static final String SUB_MAP_KEY = "submapkey1";
    private static final String SUB_MAP_KEY2 = "submapkey2";
    ImmutableRecord candidate;
    Map<String, IValue> image;
    Record template;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        this.image = new HashMap<String, IValue>();
        this.image.put(KEY1, V1);
        this.image.put(KEY2, V2);
        this.template = new Record(NAME, this.image, CONTEXT);
        this.template.getOrCreateSubMap(SUB_MAP_KEY).put(KEY1, SUB_MAP_V1);
        this.candidate = new ImmutableRecord(this.template);
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#hashCode()}.
     */
    @Test
    public void testHashCode()
    {
        assertEquals(this.candidate.getName().hashCode(), this.candidate.hashCode());
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#size()}.
     */
    @Test
    public void testSize()
    {
        assertEquals(this.image.size(), this.candidate.size());
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#isEmpty()}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testIsEmpty()
    {
        assertEquals(this.image.isEmpty(), this.candidate.isEmpty());
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#containsKey(java.lang.Object)}.
     */
    @Test
    public void testContainsKey()
    {
        assertTrue(this.candidate.containsKey(KEY1));
        assertFalse(this.candidate.containsKey(KEY1 + 324));
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#containsValue(java.lang.Object)}.
     */
    @Test
    public void testContainsValue()
    {
        assertTrue(this.candidate.containsValue(V1));
        assertFalse(this.candidate.containsValue(KEY1 + 324));
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#get(java.lang.Object)}.
     */
    @Test
    public void testGet()
    {
        assertEquals(V1, this.candidate.get(KEY1));
        assertNull(this.candidate.get(KEY1 + 234));
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#put(java.lang.String, com.fimtra.datafission.IValue)}
     * .
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testPutStringIValue()
    {
        this.candidate.put(KEY1, V2);
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#remove(java.lang.Object)}.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testRemove()
    {
        this.candidate.remove(KEY1);
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#putAll(java.util.Map)}.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testPutAll()
    {
        this.candidate.putAll(this.image);
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#clear()}.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testClear()
    {
        this.candidate.clear();
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#keySet()}.
     */
    @Test
    public void testKeySet()
    {
        assertEquals(this.image.keySet(), this.candidate.keySet());
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#keySet()}.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testKeySetImmutable()
    {
        final Iterator<String> iterator = this.candidate.keySet().iterator();
        iterator.next();
        iterator.remove();
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#values()}.
     */
    @Test
    public void testValues()
    {
        assertEquals(new HashSet<IValue>(this.image.values()), new HashSet<Object>(this.candidate.values()));
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#values()}.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testValuesImmutable()
    {
        final Iterator<IValue> iterator = this.candidate.values().iterator();
        iterator.next();
        iterator.remove();
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#entrySet()}.
     */
    @Test
    public void testEntrySet()
    {
        assertEquals(this.image.entrySet(), this.candidate.entrySet());
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#entrySet()}.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testEntrySetImmutable()
    {
        final Iterator<Entry<String, IValue>> iterator = this.candidate.entrySet().iterator();
        iterator.next();
        iterator.remove();
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#equals(java.lang.Object)}.
     */
    @Test
    public void testEqualsObject()
    {
        final Record other = new Record(NAME, this.image, CONTEXT);
        other.getOrCreateSubMap(SUB_MAP_KEY).put(KEY1, SUB_MAP_V1);
        assertTrue(this.candidate.equals(other));
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#getName()}.
     */
    @Test
    public void testGetName()
    {
        assertEquals(NAME, this.candidate.getName());
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#getContextName()}.
     */
    @Test
    public void testGetContextName()
    {
        assertEquals(CTX_NAME, this.candidate.getContextName());
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#put(java.lang.String, long)}.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testPutStringLong()
    {
        this.candidate.put(KEY1, 3l);
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#put(java.lang.String, double)}.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testPutStringDouble()
    {
        this.candidate.put(KEY1, 3d);
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#put(java.lang.String, java.lang.String)}
     * .
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testPutStringString()
    {
        this.candidate.put(KEY1, "");
    }

    /**
     * Test method for {@link com.fimtra.datafission.core.ImmutableRecord#getSubMapKeys()}.
     */
    @Test
    public void testGetSubMapKeys()
    {
        assertEquals(1, this.candidate.getSubMapKeys().size());
        assertTrue(this.candidate.getSubMapKeys().contains(SUB_MAP_KEY));
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#getOrCreateSubMap(java.lang.String)}
     * .
     */
    @Test
    public void testGetSubMap()
    {
        assertEquals(SUB_MAP_V1, this.candidate.getOrCreateSubMap(SUB_MAP_KEY).get(KEY1));
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#getOrCreateSubMap(java.lang.String)}
     * .
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testGetSubMapImmutable()
    {
        this.candidate.getOrCreateSubMap(SUB_MAP_KEY).put("", V2);
    }

    /**
     * Test method for
     * {@link com.fimtra.datafission.core.ImmutableRecord#removeSubMap(java.lang.String)}.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveSubMap()
    {
        this.candidate.removeSubMap(SUB_MAP_KEY);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSubMapIterator()
    {
        final Iterator<Entry<String, IValue>> iterator =
            this.candidate.getOrCreateSubMap(SUB_MAP_KEY).entrySet().iterator();
        iterator.next();
        iterator.remove();
    }

    @Test
    public void testLiveImmutableSeesRecordChanges_createdBeforeSubMap()
    {
        this.template = new Record(NAME, new HashMap<String, IValue>(), CONTEXT);
        this.candidate = new ImmutableRecord(this.template);

        assertEquals(0, this.candidate.keySet().size());
        assertEquals(0, this.candidate.getSubMapKeys().size());

        // alter the source record
        this.template.put(KEY_3, VALUE_3);

        this.template.getOrCreateSubMap(SUB_MAP_KEY).put(KEY1, SUB_MAP_V1);
        this.template.getOrCreateSubMap(SUB_MAP_KEY2).put(KEY_4, VALUE_4);

        assertEquals(1, this.candidate.keySet().size());
        assertEquals(2, this.candidate.getSubMapKeys().size());
    }

    @Test
    public void testLiveImmutableSeesRecordChanges()
    {
        assertEquals(2, this.candidate.keySet().size());
        assertEquals(1, this.candidate.getSubMapKeys().size());
        assertTrue(this.candidate.getSubMapKeys().contains(SUB_MAP_KEY));

        this.template.put(KEY_3, VALUE_3);
        Map<String, IValue> subMap2 = this.template.getOrCreateSubMap(SUB_MAP_KEY2);
        subMap2.put(KEY_4, VALUE_4);
        assertEquals(3, this.candidate.keySet().size());
        assertEquals(2, this.candidate.getSubMapKeys().size());
    }
    
    @Test
    public void testSnapshotOfLiveImmutableDoesNotSeeRecordChanges()
    {
        assertEquals(2, this.candidate.keySet().size());
        assertEquals(1, this.candidate.getSubMapKeys().size());
        assertTrue(this.candidate.getSubMapKeys().contains(SUB_MAP_KEY));

        ImmutableRecord snapshot = ImmutableSnapshotRecord.create(this.candidate);
        
        // check the live image sees the change
        this.template.put(KEY_3, VALUE_3);
        Map<String, IValue> subMap2 = this.template.getOrCreateSubMap(SUB_MAP_KEY2);
        subMap2.put(KEY_4, VALUE_4);
        assertEquals(3, this.candidate.keySet().size());
        assertEquals(2, this.candidate.getSubMapKeys().size());
        
        // check the snapshot does not
        assertEquals(2, snapshot.keySet().size());
        assertEquals(1, snapshot.getSubMapKeys().size());
        assertEquals(this.candidate.getContextName(), snapshot.getContextName());
    }

    @Test
    public void testSnapshotImmutableDoesNotSeeRecordChanges()
    {
        this.candidate = ImmutableSnapshotRecord.create(this.template);

        this.template.getOrCreateSubMap(SUB_MAP_KEY).put(KEY1, SUB_MAP_V1);

        this.template.put(KEY_3, VALUE_3);
        Map<String, IValue> subMap2 = this.template.getOrCreateSubMap(SUB_MAP_KEY2);
        subMap2.put(KEY_4, VALUE_4);

        assertEquals(2, this.candidate.keySet().size());
        assertEquals(1, this.candidate.getSubMapKeys().size());
    }
}
