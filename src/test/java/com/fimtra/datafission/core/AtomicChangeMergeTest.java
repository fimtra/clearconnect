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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.AtomicChange;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.field.TextValue;

/**
 * Merge tests for the {@link AtomicChange}
 * 
 * @author Ramon Servadei
 */
public class AtomicChangeMergeTest
{
    final static String[] k = { "k0", "k1", "k2", "k3", "k4" };
    final static IValue[] v = { TextValue.valueOf("v0"), TextValue.valueOf("v1"), TextValue.valueOf("v2"),
        TextValue.valueOf("v3"), TextValue.valueOf("v4"), };

    String name;
    List<IRecordChange> changes;
    Map<String, IValue> empty;
    Map<String, IValue> put1;
    Map<String, IValue> put2;
    Map<String, IValue> overwritten1;
    Map<String, IValue> overwritten2;
    Map<String, IValue> removed1;
    Map<String, IValue> removed2;

    @Before
    public void setUp() throws Exception
    {
        this.name = "mergeTestRec";
        this.changes = new LinkedList<IRecordChange>();
        this.empty = ContextUtils.EMPTY_MAP;
        this.put1 = new HashMap<String, IValue>();
        this.put2 = new HashMap<String, IValue>();
        this.overwritten1 = new HashMap<String, IValue>();
        this.overwritten2 = new HashMap<String, IValue>();
        this.removed1 = new HashMap<String, IValue>();
        this.removed2 = new HashMap<String, IValue>();
    }

    static AtomicChange createCandidate(String name)
    {
        final Map<String, IValue> putEntries = new HashMap<String, IValue>();
        final Map<String, IValue> overwrittenEntries = new HashMap<String, IValue>();
        final Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        return new AtomicChange(name, putEntries, overwrittenEntries, removedEntries);
    }

    @Test
    public void testMergeAtomicChanges_allNulls()
    {
        this.changes.add(new AtomicChange(this.name, null, null, null));
        this.changes.add(new AtomicChange(this.name, null, null, null));
        this.changes.add(new AtomicChange(this.name, null, null, null));
        
        this.changes.get(2).setSequence(56);
        this.changes.get(2).setScope('d');
        
        AtomicChange result = createCandidate(this.name);
        result.coalesce(this.changes);

        assertEquals(this.name, result.getName());
        assertEquals(56, result.getSequence());
        assertEquals('d', result.getScope());
        assertEquals(this.empty, result.getPutEntries());
        assertEquals(this.empty, result.getOverwrittenEntries());
        assertEquals(this.empty, result.getRemovedEntries());
    }

    @Test
    public void testMergeAtomicChanges_allEmpty()
    {
        this.changes.add(new AtomicChange(this.name, this.empty, this.empty, this.empty));
        this.changes.add(new AtomicChange(this.name, this.empty, this.empty, this.empty));
        this.changes.add(new AtomicChange(this.name, this.empty, this.empty, this.empty));
        
        this.changes.get(2).setSequence(56);
        this.changes.get(2).setScope('d');
        
        AtomicChange result = createCandidate(this.name);
        result.coalesce(this.changes);

        assertEquals(this.name, result.getName());
        assertEquals(56, result.getSequence());
        assertEquals('d', result.getScope());
        assertEquals(this.empty, result.getPutEntries());
        assertEquals(this.empty, result.getOverwrittenEntries());
        assertEquals(this.empty, result.getRemovedEntries());
    }

    @Test
    public void testMergeAtomicChanges_oneRemoveChange()
    {
        // the first update
        this.removed1.put(k[1], v[1]);

        this.changes.add(new AtomicChange(this.name, this.put1, this.overwritten1, this.removed1));

        AtomicChange result = createCandidate(this.name);
        result.coalesce(this.changes);

        assertEquals(this.name, result.getName());
        assertEquals(this.put1, result.getPutEntries());
        assertEquals(this.overwritten1, result.getOverwrittenEntries());
        assertEquals(this.removed1, result.getRemovedEntries());
    }

    @Test
    public void testMergeAtomicChanges_oneChange()
    {
        // the first update
        this.put1.put(k[1], v[1]);
        this.put1.put(k[2], v[2]);

        this.changes.add(new AtomicChange(this.name, this.put1, this.overwritten1, this.removed1));

        AtomicChange result = createCandidate(this.name);
        result.coalesce(this.changes);

        assertEquals(this.name, result.getName());
        assertEquals(this.put1, result.getPutEntries());
        assertEquals(this.overwritten1, result.getOverwrittenEntries());
        assertEquals(this.removed1, result.getRemovedEntries());
    }

    @Test
    public void testMergeAtomicChanges_twoChanges()
    {
        // the first update
        this.put1.put(k[1], v[1]);
        this.put1.put(k[2], v[2]);

        // the second
        this.put2.put(k[4], v[4]);
        this.put2.put(k[2], v[3]);
        this.overwritten2.put(k[2], v[2]);
        this.removed2.put(k[1], v[1]);

        this.changes.add(new AtomicChange(this.name, this.put1, this.overwritten1, this.removed1));
        this.changes.add(new AtomicChange(this.name, this.put2, this.overwritten2, this.removed2));

        Map<String, IValue> expectedPut = new HashMap<String, IValue>();
        Map<String, IValue> expectedOverwritten = new HashMap<String, IValue>();
        Map<String, IValue> expectedRemoved = new HashMap<String, IValue>();

        expectedPut.put(k[2], v[3]);
        expectedPut.put(k[4], v[4]);
        expectedOverwritten.put(k[2], v[2]);
        expectedRemoved.put(k[1], v[1]);

        AtomicChange result = createCandidate(this.name);
        result.coalesce(this.changes);

        assertEquals(this.name, result.getName());
        assertEquals(expectedPut, result.getPutEntries());
        assertEquals(expectedOverwritten, result.getOverwrittenEntries());
        assertEquals(expectedRemoved, result.getRemovedEntries());
    }

    @Test
    public void testMergeAtomicChanges_singleChangeWithSubMaps()
    {
        // the first update
        this.put1.put(k[1], v[1]);
        this.put1.put(k[2], v[2]);

        Map<String, IValue> subPut1 = new HashMap<String, IValue>();
        subPut1.put(k[3], v[3]);
        AtomicChange change = new AtomicChange(this.name, this.put1, this.overwritten1, this.removed1);
        change.internalGetSubMapAtomicChange("sdf1").internalGetPutEntries().putAll(subPut1);
        this.changes.add(change);

        AtomicChange result = createCandidate(this.name);
        result.coalesce(this.changes);

        assertEquals(this.name, result.getName());
        assertEquals(this.put1, result.getPutEntries());
        assertEquals(this.overwritten1, result.getOverwrittenEntries());
        assertEquals(this.removed1, result.getRemovedEntries());
        assertEquals(subPut1, result.getSubMapAtomicChange("sdf1").getPutEntries());
    }

    @Test
    public void testMergeAtomicChanges_twoChangesWithSubMaps()
    {
        // the first update
        this.put1.put(k[1], v[1]);
        this.put1.put(k[2], v[2]);

        Map<String, IValue> subPut1 = new HashMap<String, IValue>();
        subPut1.put(k[3], v[3]);
        subPut1.put(k[0], v[0]);
        AtomicChange change1 = new AtomicChange(this.name, this.put1, this.overwritten1, this.removed1);
        change1.internalGetSubMapAtomicChange("sdf1").internalGetPutEntries().putAll(subPut1);

        this.changes.add(change1);

        // the second
        this.put2.put(k[4], v[4]);
        this.put2.put(k[2], v[3]);
        this.overwritten2.put(k[2], v[2]);
        this.removed2.put(k[1], v[1]);

        Map<String, IValue> subPut2 = new HashMap<String, IValue>();
        subPut2.put(k[3], v[4]);
        Map<String, IValue> subRemoved2 = new HashMap<String, IValue>();
        subRemoved2.put(k[0], v[0]);
        AtomicChange change2 = new AtomicChange(this.name, this.put2, this.overwritten2, this.removed2);
        change2.internalGetSubMapAtomicChange("sdf1").internalGetPutEntries().putAll(subPut2);
        change2.internalGetSubMapAtomicChange("sdf1").internalGetOverwrittenEntries().putAll(subPut1);
        change2.internalGetSubMapAtomicChange("sdf1").internalGetRemovedEntries().putAll(subRemoved2);

        this.changes.add(change2);

        Map<String, IValue> expectedPut = new HashMap<String, IValue>();
        Map<String, IValue> expectedOverwritten = new HashMap<String, IValue>();
        Map<String, IValue> expectedRemoved = new HashMap<String, IValue>();

        expectedPut.put(k[2], v[3]);
        expectedPut.put(k[4], v[4]);
        expectedOverwritten.put(k[2], v[2]);
        expectedRemoved.put(k[1], v[1]);

        AtomicChange result = createCandidate(this.name);
        result.coalesce(this.changes);

        assertEquals(this.name, result.getName());
        assertEquals(expectedPut, result.getPutEntries());
        assertEquals(expectedOverwritten, result.getOverwrittenEntries());
        assertEquals(expectedRemoved, result.getRemovedEntries());

        Map<String, IValue> expectedOverwrittenSub = new HashMap<String, IValue>();
        expectedOverwrittenSub.put(k[3], v[3]);

        assertEquals(subPut2, result.getSubMapAtomicChange("sdf1").getPutEntries());
        assertEquals(expectedOverwrittenSub, result.getSubMapAtomicChange("sdf1").getOverwrittenEntries());
        assertEquals(subRemoved2, result.getSubMapAtomicChange("sdf1").getRemovedEntries());
    }

    @Test
    public void testMergeAtomicChanges_twoChangesSubMapsOnly()
    {
        Map<String, IValue> subPut1 = new HashMap<String, IValue>();
        subPut1.put(k[3], v[3]);
        AtomicChange change = new AtomicChange(this.name, this.put1, this.overwritten1, this.removed1);
        change.internalGetSubMapAtomicChange("sdf1").internalGetPutEntries().putAll(subPut1);
        this.changes.add(change);

        Map<String, IValue> subPut2 = new HashMap<String, IValue>();
        subPut2.put(k[4], v[4]);
        AtomicChange change2 = new AtomicChange(this.name, this.put1, this.overwritten1, this.removed1);
        change2.internalGetSubMapAtomicChange("sdf1").internalGetPutEntries().putAll(subPut2);
        this.changes.add(change2);

        AtomicChange result = createCandidate(this.name);
        result.internalGetSubMapAtomicChange("sdf1").internalGetPutEntries().put(k[2], v[2]);

        result.coalesce(this.changes);

        assertEquals(this.name, result.getName());
        assertEquals(this.put1, result.getPutEntries());
        assertEquals(this.overwritten1, result.getOverwrittenEntries());
        assertEquals(this.removed1, result.getRemovedEntries());
        assertEquals(v[2], result.getSubMapAtomicChange("sdf1").getPutEntries().get(k[2]));
        assertEquals(v[3], result.getSubMapAtomicChange("sdf1").getPutEntries().get(k[3]));
        assertEquals(v[4], result.getSubMapAtomicChange("sdf1").getPutEntries().get(k[4]));
    }
}
