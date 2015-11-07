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

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.AtomicChange;
import com.fimtra.datafission.core.AtomicChangeTeleporter;
import com.fimtra.datafission.field.TextValue;

/**
 * Tests for the {@link AtomicChangeTeleporter}
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings("boxing")
public class AtomicChangeTeleporterTest
{
    private static final String K1 = "K1";
    private static final String K2 = "K2";
    private static final String K3 = "K3";
    private static final String K4 = "K4";
    private static final String K5 = "K5";
    private static final String K6 = "K6";
    private static final IValue V0 = TextValue.valueOf("V0");
    private static final IValue V1 = TextValue.valueOf("V1");
    private static final IValue V2 = TextValue.valueOf("V2");
    private static final IValue V3 = TextValue.valueOf("V3");
    private static final IValue V4 = TextValue.valueOf("V4");
    private static final IValue V5 = TextValue.valueOf("V5");
    private static final IValue V6 = TextValue.valueOf("V6");
    AtomicChangeTeleporter candidate;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new AtomicChangeTeleporter(3);
    }

    static void populateChange(AtomicChange change)
    {
        Map<String, IValue> putEntries = change.internalGetPutEntries();
        putEntries.put(K1, V1);
        putEntries.put(K2, V2);
        putEntries.put(K3, V3);
        putEntries.put(K4, V4);
        putEntries.put(K5, V5);

        Map<String, IValue> overwrittenEntries = change.internalGetOverwrittenEntries();
        overwrittenEntries.put(K1, V0);
        overwrittenEntries.put(K2, V0);
        overwrittenEntries.put(K3, V0);
        overwrittenEntries.put(K4, V0);
        overwrittenEntries.put(K5, V0);

        Map<String, IValue> removedEntries = change.internalGetRemovedEntries();
        removedEntries.put(K1, V0);
        removedEntries.put(K2, V0);
        removedEntries.put(K3, V0);
        removedEntries.put(K4, V0);
        removedEntries.put(K5, V0);
    }

    @Test
    public void testEmptyChange()
    {
        AtomicChange expected = new AtomicChange("c1");
        expected.setScope(IRecordChange.IMAGE_SCOPE);
        expected.setSequence(System.currentTimeMillis());

        doPartsTest(expected);
    }

    @Test
    public void testSinglePart_underLimit()
    {
        AtomicChange expected = new AtomicChange("c1");
        expected.setScope(IRecordChange.IMAGE_SCOPE);
        expected.setSequence(System.currentTimeMillis());
        Map<String, IValue> putEntries = expected.internalGetPutEntries();
        putEntries.put(K1, V1);
        putEntries.put(K2, V2);

        doPartsTest(expected);
    }

    @Test
    public void testSinglePart_atLimit()
    {
        AtomicChange expected = new AtomicChange("c1");
        expected.setScope(IRecordChange.IMAGE_SCOPE);
        expected.setSequence(System.currentTimeMillis());
        Map<String, IValue> putEntries = expected.internalGetPutEntries();
        putEntries.put(K1, V1);
        putEntries.put(K2, V2);
        putEntries.put(K3, V3);

        doPartsTest(expected);
    }

    @Test
    public void test2Parts()
    {
        AtomicChange expected = new AtomicChange("c1");
        expected.setScope(IRecordChange.IMAGE_SCOPE);
        expected.setSequence(System.currentTimeMillis());
        Map<String, IValue> putEntries = expected.internalGetPutEntries();
        putEntries.put(K1, V1);
        putEntries.put(K2, V2);
        putEntries.put(K3, V3);
        putEntries.put(K4, V4);

        doPartsTest(expected);
    }
    
    @Test
    public void test2Parts_exact()
    {
        AtomicChange expected = new AtomicChange("c1");
        expected.setScope(IRecordChange.IMAGE_SCOPE);
        expected.setSequence(System.currentTimeMillis());
        Map<String, IValue> putEntries = expected.internalGetPutEntries();
        putEntries.put(K1, V1);
        putEntries.put(K2, V2);
        putEntries.put(K3, V3);
        putEntries.put(K4, V4);
        putEntries.put(K5, V5);
        putEntries.put(K6, V6);

        doPartsTest(expected);
    }

    @Test
    public void testMultipleParts()
    {
        AtomicChange expected = new AtomicChange("c1");
        expected.setScope(IRecordChange.IMAGE_SCOPE);
        expected.setSequence(System.currentTimeMillis());
        populateChange(expected);
        populateChange(expected.internalGetSubMapAtomicChange("subMap1"));
        populateChange(expected.internalGetSubMapAtomicChange("subMap2"));
        populateChange(expected.internalGetSubMapAtomicChange("subMap3"));

        doPartsTest(expected);
    }

    void doPartsTest(AtomicChange expected)
    {
        AtomicChange[] parts = this.candidate.split(expected);
        AtomicChange result = null;
        for (int i = 0; i < parts.length; i++)
        {
            result = this.candidate.combine(parts[i]);
        }
        assertEquals(AtomicChangeTeleporter.getFieldChangeCount(expected),
            AtomicChangeTeleporter.getFieldChangeCount(result));
        assertEquals(expected, result);
    }

}
