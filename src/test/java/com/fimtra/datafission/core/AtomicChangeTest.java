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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.AtomicChange;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.IAtomicChangeManager;
import com.fimtra.datafission.core.Record;
import com.fimtra.datafission.field.DoubleValue;

/**
 * Tests for the {@link AtomicChange}
 * 
 * @author Ramon Servadei
 */
public class AtomicChangeTest
{
    private static final String SUBMAP_KEY1 = "submapKey1";
    private static final String SUBMAP_KEY2 = "submapKey2";
    static final String name = "test";
    private static final String K1 = "1";
    private static final String K2 = "2";
    private static final IValue V1 = new DoubleValue(1);
    private static final IValue V1p = new DoubleValue(1.1);
    private static final IValue V2 = new DoubleValue(2);
    private static final IValue V2p = new DoubleValue(2.1);

    AtomicChange candidate;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new AtomicChange(name);
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testGetName()
    {
        assertEquals(name, this.candidate.getName());
    }

    @Test
    public void testPutRemovePut()
    {
        final AtomicChange atomicChange = this.candidate;

        assertEquals(0, atomicChange.getPutEntries().size());
        assertEquals(0, atomicChange.getOverwrittenEntries().size());
        assertEquals(0, atomicChange.getRemovedEntries().size());

        // initial put
        atomicChange.mergeEntryUpdatedChange(K1, V1p, null);

        assertEquals(1, atomicChange.getPutEntries().size());
        assertEquals(0, atomicChange.getOverwrittenEntries().size());
        assertEquals(0, atomicChange.getRemovedEntries().size());
        assertEquals(V1p, atomicChange.getPutEntries().get(K1));
        assertNull(atomicChange.getOverwrittenEntries().get(K1));

        // update
        atomicChange.mergeEntryUpdatedChange(K1, V1, V1p);

        assertEquals(1, atomicChange.getPutEntries().size());
        assertEquals(1, atomicChange.getOverwrittenEntries().size());
        assertEquals(0, atomicChange.getRemovedEntries().size());
        assertEquals(V1, atomicChange.getPutEntries().get(K1));
        assertEquals(V1p, atomicChange.getOverwrittenEntries().get(K1));

        atomicChange.mergeEntryUpdatedChange(K2, V2, V2p);

        assertEquals(2, atomicChange.getPutEntries().size());
        assertEquals(2, atomicChange.getOverwrittenEntries().size());
        assertEquals(0, atomicChange.getRemovedEntries().size());
        assertEquals(V2, atomicChange.getPutEntries().get(K2));
        assertEquals(V2p, atomicChange.getOverwrittenEntries().get(K2));

        atomicChange.mergeEntryRemovedChange(K1, V1);

        assertEquals(1, atomicChange.getPutEntries().size());
        assertEquals(1, atomicChange.getOverwrittenEntries().size());
        assertEquals(1, atomicChange.getRemovedEntries().size());
        assertEquals(V1, atomicChange.getRemovedEntries().get(K1));

        atomicChange.mergeEntryUpdatedChange(K1, V1, V1p);

        assertEquals(2, atomicChange.getPutEntries().size());
        assertEquals(2, atomicChange.getOverwrittenEntries().size());
        assertEquals(0, atomicChange.getRemovedEntries().size());
    }

    @Test
    public void testUnmodifyableChanges()
    {
        try
        {
            this.candidate.getOverwrittenEntries().clear();
            fail("Should throw exception");
        }
        catch (Exception e)
        {
        }
        try
        {
            this.candidate.getPutEntries().clear();
            fail("Should throw exception");
        }
        catch (Exception e)
        {
        }
        try
        {
            this.candidate.getRemovedEntries().clear();
            fail("Should throw exception");
        }
        catch (Exception e)
        {
        }
        try
        {
            this.candidate.getSubMapAtomicChange(SUBMAP_KEY1).getPutEntries().put(K1, V1);
            fail("Should throw exception");
        }
        catch (Exception e)
        {
        }
        try
        {
            this.candidate.getSubMapAtomicChange(SUBMAP_KEY1).getRemovedEntries().put(K1, V1);
            fail("Should throw exception");
        }
        catch (Exception e)
        {
        }
        try
        {
            this.candidate.getSubMapAtomicChange(SUBMAP_KEY1).getOverwrittenEntries().put(K1, V1);
            fail("Should throw exception");
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testSubMapAtomicChangeUnmodifyable()
    {
        this.candidate.mergeSubMapEntryRemovedChange(SUBMAP_KEY1, K1, V1);
        try
        {
            this.candidate.getSubMapAtomicChange(SUBMAP_KEY1).getPutEntries().clear();
            fail("Should throw exception");
        }
        catch (Exception e)
        {
        }
        try
        {
            this.candidate.getSubMapAtomicChange(SUBMAP_KEY1).getRemovedEntries().clear();
            fail("Should throw exception");
        }
        catch (Exception e)
        {
        }
        try
        {
            this.candidate.getSubMapAtomicChange(SUBMAP_KEY1).getOverwrittenEntries().clear();
            fail("Should throw exception");
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testSubMapChanges()
    {
        this.candidate.mergeEntryUpdatedChange(K1, V1, V1p);
        this.candidate.mergeSubMapEntryUpdatedChange(SUBMAP_KEY1, K1, V1, V1p);
        this.candidate.mergeSubMapEntryUpdatedChange(SUBMAP_KEY2, K1, V1, V1p);
        this.candidate.mergeSubMapEntryUpdatedChange(SUBMAP_KEY2, K2, V2, null);
        this.candidate.mergeSubMapEntryRemovedChange(SUBMAP_KEY2, K1, V1);

        assertEquals(1, this.candidate.getPutEntries().size());
        assertEquals(1, this.candidate.getOverwrittenEntries().size());
        assertEquals(0, this.candidate.getRemovedEntries().size());

        assertEquals(2, this.candidate.getSubMapKeys().size());
        assertEquals(1, this.candidate.getSubMapAtomicChange(SUBMAP_KEY1).getPutEntries().size());
        assertEquals(1, this.candidate.getSubMapAtomicChange(SUBMAP_KEY1).getOverwrittenEntries().size());
        assertEquals(0, this.candidate.getSubMapAtomicChange(SUBMAP_KEY1).getRemovedEntries().size());

        assertEquals(1, this.candidate.getSubMapAtomicChange(SUBMAP_KEY2).getPutEntries().size());
        assertEquals(0, this.candidate.getSubMapAtomicChange(SUBMAP_KEY2).getOverwrittenEntries().size());
        assertEquals(1, this.candidate.getSubMapAtomicChange(SUBMAP_KEY2).getRemovedEntries().size());
    }

    @Test
    public void testIsEmpty()
    {
        assertTrue(this.candidate.isEmpty());
        this.candidate.mergeEntryRemovedChange(K1, V1);
        assertFalse(this.candidate.isEmpty());
    }

    @Test
    public void testIsEmptySubMapChanges()
    {
        assertTrue(this.candidate.isEmpty());
        this.candidate.mergeSubMapEntryRemovedChange(SUBMAP_KEY1, K1, V1);
        assertFalse(this.candidate.isEmpty());
    }

    @Test
    public void testIsEmptyDataAndSubMapChanges()
    {
        assertTrue(this.candidate.isEmpty());
        this.candidate.mergeEntryRemovedChange(K1, V1);
        this.candidate.mergeSubMapEntryRemovedChange(SUBMAP_KEY1, K1, V1);
        assertFalse(this.candidate.isEmpty());
    }

    @Test
    public void testApplyTo()
    {
        this.candidate.mergeEntryUpdatedChange(K1, V1, V1p);
        this.candidate.mergeEntryRemovedChange(K2, V1);
        this.candidate.mergeSubMapEntryUpdatedChange(SUBMAP_KEY1, K1, V1, V1p);
        Map<String, IValue> target = new HashMap<String, IValue>();
        target.put(K2, V2);
        assertNotNull(target.get(K2));

        this.candidate.applyTo(target);
        assertEquals(1, target.size());
        assertTrue(target.containsKey(K1));
        assertEquals(V1, target.get(K1));
        assertNull(target.get(K2));
    }

    @Test
    public void testApplyCompleteAtomicChangeToRecord()
    {
        this.candidate.mergeEntryUpdatedChange(K1, V1, V1p);
        this.candidate.mergeEntryRemovedChange(K2, V1);
        this.candidate.mergeSubMapEntryUpdatedChange(SUBMAP_KEY1, K1, V1, V1p);
        final IAtomicChangeManager mock = mock(IAtomicChangeManager.class);
        Record target = new Record("test", ContextUtils.EMPTY_MAP, mock);

        target.put(K2, V2);
        assertNotNull(target.get(K2));

        this.candidate.applyCompleteAtomicChangeToRecord(target);
        assertEquals(1, target.size());
        assertEquals(1, target.getSubMapKeys().size());
        assertTrue(target.containsKey(K1));
        assertEquals(V1, target.get(K1));
        assertEquals(V1, target.getOrCreateSubMap(SUBMAP_KEY1).get(K1));
        assertNull(target.get(K2));
        assertNull(target.getOrCreateSubMap(SUBMAP_KEY1).get(K2));
    }
}
