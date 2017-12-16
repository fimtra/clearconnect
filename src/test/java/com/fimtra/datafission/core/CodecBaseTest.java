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
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.BlobValue;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;

/**
 * Tests for the {@link ICodec} implementations
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public abstract class CodecBaseTest
{
    String name = "myName";
    ICodec<?> candidate;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = constructCandidate();
    }

    @After
    public void tearDown() throws Exception
    {
    }

    abstract ICodec constructCandidate();

    private static void createRemoveEntries(Map<String, IValue> removedEntries)
    {
        final Random random = new Random();
        for (int i = 0; i < 2; i++)
        {
            removedEntries.put("rem_d" + i, new DoubleValue(random.nextDouble()));
            removedEntries.put("rem_l" + i, LongValue.valueOf(random.nextLong()));
            removedEntries.put("rem_s" + i, TextValue.valueOf(new Date().toString()));
        }
    }

    private static void createAddEntries(Map<String, IValue> addedEntries)
    {
        final Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            addedEntries.put("add=_Kd" + i, new DoubleValue(random.nextDouble()));
            addedEntries.put("=add_Kl" + i, LongValue.valueOf(random.nextLong()));
            addedEntries.put("add\\=_Ks" + i, TextValue.valueOf("== date \\= " + new Date().toString()));
        }
    }

    @Test
    public void testTxAndRxChangeWithAddAndRemove()
    {
        prepareForRemove();

        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        createAddEntries(addedEntries);
        createRemoveEntries(removedEntries);
        final IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, addedEntries, removedEntries, changeFromRxData);
    }

    @Test
    public void testTxAndRxChangeWithNoAddOrRemove()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        final IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, addedEntries, removedEntries, changeFromRxData);
    }

    @Test
    public void testTxAndRxChangeWithSimpleAddOnly()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        addedEntries.put("aDouble", new DoubleValue(3.1415926535898d));
        addedEntries.put("aLong", LongValue.valueOf(1234));
        addedEntries.put("aText", TextValue.valueOf("hello"));
        addedEntries.put("aBlob", BlobValue.valueOf("hello".getBytes()));
        final IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, addedEntries, removedEntries, changeFromRxData);
    }

    @Test
    public void testTxAndRxChangeWithAddOnly()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        createAddEntries(addedEntries);
        final IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, addedEntries, removedEntries, changeFromRxData);
    }

    @Test
    public void testTxAndRxChangeWithRemoveOnly()
    {
        // first send the adds
        prepareForRemove();

        // now send just the removes
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        createRemoveEntries(removedEntries);
        IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, addedEntries, removedEntries, changeFromRxData);
    }

    private void prepareForRemove()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        createRemoveEntries(addedEntries);
        IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, addedEntries, removedEntries, changeFromRxData);
    }

    @Test
    public void testGetTxMessageForAtomicChangeWithNullPutEntries()
    {
        final String txStringForChange = new String(constructCandidate().getTxMessageForAtomicChange(
            new AtomicChange("null put entries", null, new HashMap<String, IValue>(), new HashMap<String, IValue>())));
        assertNotNull(txStringForChange);
    }

    @Test
    public void testGetTxMessageForAtomicChangeWithNullOverwrittenEntries()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        ICodec<?> candidate = constructCandidate();

        byte[] data = candidate.finalEncode(candidate.getTxMessageForAtomicChange(
            new AtomicChange("null overwritten entries", addedEntries, null, removedEntries)));
        final IRecordChange changeFromRxData = candidate.getAtomicChangeFromRxMessage(ByteBuffer.wrap(data));
        checkResults("null overwritten entries", addedEntries, removedEntries, changeFromRxData);
    }

    @Test
    public void testGetTxMessageForAtomicChangeWithNullRemovedEntries()
    {
        final String txStringForChange =
            new String(constructCandidate().getTxMessageForAtomicChange(new AtomicChange("null removed entries",
                new HashMap<String, IValue>(), new HashMap<String, IValue>(), null)));
        assertNotNull(txStringForChange);
    }

    @Test
    public void testGetTxMessageForAtomicChangeWithNullKeyAndData()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        addedEntries.put(null, null);
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        removedEntries.put(null, null);
        final IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, ContextUtils.EMPTY_MAP, removedEntries, changeFromRxData);
    }

    @Test
    public void testTxAndRxChangeWith1Put()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        addedEntries.put("k1", LongValue.valueOf(1l));
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        final IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, addedEntries, removedEntries, changeFromRxData);
    }

    @Test
    public void testTxAndRxChangeWithSlashes()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        addedEntries.put("k1", TextValue.valueOf("\\"));
        addedEntries.put("k2", TextValue.valueOf("\\\\"));
        addedEntries.put("k3", TextValue.valueOf("\\\\\\"));
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        final IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, addedEntries, removedEntries, changeFromRxData);
    }

    @Test
    public void testTxAndRxChangeWith2Puts()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        // addedEntries.put("k1", DoubleValue.valueOf(0.9567168631967422));
        // addedEntries.put("k2", LongValue.valueOf(-6009208550497218990l));
        addedEntries.put("k1", DoubleValue.valueOf(0.7105620648679962));
        addedEntries.put("k2", LongValue.valueOf(2889721482211017439l));
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        final IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, addedEntries, removedEntries, changeFromRxData);
    }

    @Test
    public void testTxAndRxChangeWithPutRemove()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        addedEntries.put("k1", LongValue.valueOf(1));

        // this will remove the put
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        removedEntries.put("k1", LongValue.valueOf(1));

        final IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, ContextUtils.EMPTY_MAP, removedEntries, changeFromRxData);
    }

    @Test
    public void testTxAndRxChangeWithNullData()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        addedEntries.put("null value", null);

        // this will remove the put
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        removedEntries.put("null value", null);

        final IRecordChange changeFromRxData = performTxRxAndGetChange(this.name, addedEntries, removedEntries);
        checkResults(this.name, ContextUtils.EMPTY_MAP, removedEntries, changeFromRxData);
    }

    @Test
    public void testRpcDetailsEncodeDecode()
    {
        IValue[] args = new IValue[] { TextValue.valueOf("lasers"), new DoubleValue(123d) };
        final ICodec constructCandidate = constructCandidate();
        byte[] txMessageForRpc = constructCandidate.finalEncode(
            constructCandidate.getTxMessageForRpc("testRpc", args, "The result record name"));

        IRecordChange rpcDetails = constructCandidate.getRpcFromRxMessage(constructCandidate.decode(ByteBuffer.wrap(txMessageForRpc)));

        Map<String, IValue> expected = new HashMap<String, IValue>();
        expected.put(RpcInstance.Remote.ARG_ + "0", TextValue.valueOf("lasers"));
        expected.put(RpcInstance.Remote.ARG_ + "1", new DoubleValue(123d));
        expected.put(RpcInstance.Remote.RESULT_RECORD_NAME, TextValue.valueOf("The result record name"));
        expected.put(RpcInstance.Remote.ARGS_COUNT, LongValue.valueOf(2));

        assertEquals("testRpc", rpcDetails.getName());
        assertEquals("got: " + rpcDetails, expected, rpcDetails.getPutEntries());
    }

    @Test
    public void testRpcDetailsEncodeDecodeWithSpecialCharacters()
    {
        final ICodec constructCandidate = constructCandidate();
        IValue[] args = new IValue[] { TextValue.valueOf("lasers |\\="), new DoubleValue(123d) };
        byte[] txMessageForRpc = constructCandidate.finalEncode(constructCandidate.getTxMessageForRpc(
            "testRpcDetailsEncodeDecodeWithSpecialCharacters", args, "The result record name\\||="));

        IRecordChange rpcDetails = constructCandidate.getRpcFromRxMessage(constructCandidate.decode(ByteBuffer.wrap(txMessageForRpc)));

        Map<String, IValue> expected = new HashMap<String, IValue>();
        expected.put(RpcInstance.Remote.ARG_ + "0", TextValue.valueOf("lasers |\\="));
        expected.put(RpcInstance.Remote.RESULT_RECORD_NAME, TextValue.valueOf("The result record name\\||="));
        expected.put(RpcInstance.Remote.ARGS_COUNT, LongValue.valueOf(2));
        expected.put(RpcInstance.Remote.ARG_ + "1", new DoubleValue(123d));

        assertEquals("testRpcDetailsEncodeDecodeWithSpecialCharacters", rpcDetails.getName());
        assertEquals("got: " + rpcDetails, expected, rpcDetails.getPutEntries());
    }

    private static void checkResults(String name, Map<String, IValue> addedEntries, Map<String, IValue> removedEntries,
        final IRecordChange changeFromRxData)
    {
        assertEquals(name, changeFromRxData.getName());
        assertEquals(addedEntries, changeFromRxData.getPutEntries());
        assertEquals(removedEntries.keySet(), changeFromRxData.getRemovedEntries().keySet());
    }

    private IRecordChange performTxRxAndGetChange(String name, Map<String, IValue> putEntries,
        Map<String, IValue> removedEntries)
    {
        IRecordChange changeFromRxData = this.candidate.getAtomicChangeFromRxMessage(
            ByteBuffer.wrap(this.candidate.finalEncode(this.candidate.getTxMessageForAtomicChange(
                new AtomicChange(name, putEntries, new HashMap<String, IValue>(), removedEntries)))));

        // do it again to test codecs that send images
        changeFromRxData = this.candidate.getAtomicChangeFromRxMessage(
            ByteBuffer.wrap(this.candidate.finalEncode(this.candidate.getTxMessageForAtomicChange(
                new AtomicChange(name, putEntries, new HashMap<String, IValue>(), removedEntries)))));
        return changeFromRxData;
    }

    @Test
    public void testTxAndRxChangeWithSubMaps()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        addedEntries.put("aDouble0", new DoubleValue(3.1415926535898d));
        addedEntries.put("aLong0", LongValue.valueOf(1234));
        addedEntries.put("aBlob0", BlobValue.valueOf("01234567".getBytes()));
        addedEntries.put("aText0", TextValue.valueOf("hello"));
        final AtomicChange atomicChange =
            new AtomicChange(this.name, addedEntries, new HashMap<String, IValue>(), removedEntries);
        for (int i = 0; i < 3; i++)
        {
            for (int j = 0; j < 3; j++)
            {
                final String subMapKey = "SubMap" + j;
                atomicChange.mergeSubMapEntryUpdatedChange(subMapKey, "aDouble" + i, new DoubleValue(i + 3.141d), null);
                atomicChange.mergeSubMapEntryUpdatedChange(subMapKey, "aLong" + i, LongValue.valueOf(i), null);
                atomicChange.mergeSubMapEntryUpdatedChange(subMapKey, "aText" + i, TextValue.valueOf("This is " + i),
                    null);
            }
        }
        final byte[] txStringForChange =
            this.candidate.finalEncode(this.candidate.getTxMessageForAtomicChange(atomicChange));
        final IRecordChange changeFromRxData = this.candidate.getAtomicChangeFromRxMessage(ByteBuffer.wrap(txStringForChange));

        checkChanges(this.name, atomicChange, changeFromRxData);
    }

    @Test
    public void testTxAndRxChangeWithSubMapRemoves()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        addedEntries.put("aDouble0", new DoubleValue(3.1415926535898d));
        addedEntries.put("aLong0", LongValue.valueOf(1234));
        addedEntries.put("aBlob0", BlobValue.valueOf("01234567".getBytes()));
        addedEntries.put("aText0", TextValue.valueOf("hello"));
        final AtomicChange atomicChange =
            new AtomicChange(this.name, addedEntries, new HashMap<String, IValue>(), removedEntries);
        for (int i = 0; i < 1; i++)
        {
            for (int j = 0; j < 3; j++)
            {
                final String subMapKey = "SubMap" + j;
                atomicChange.mergeSubMapEntryRemovedChange(subMapKey, "aDouble" + i, new DoubleValue(i + 3.141d));
                atomicChange.mergeSubMapEntryRemovedChange(subMapKey, "aLong" + i, LongValue.valueOf(i));
                atomicChange.mergeSubMapEntryRemovedChange(subMapKey, "aText" + i, TextValue.valueOf("This is " + i));
            }
        }
        final byte[] txStringForChange =
            this.candidate.finalEncode(this.candidate.getTxMessageForAtomicChange(atomicChange));
        final IRecordChange changeFromRxData = this.candidate.getAtomicChangeFromRxMessage(ByteBuffer.wrap(txStringForChange));

        checkChanges(this.name, atomicChange, changeFromRxData);
    }

    static void checkChanges(String name, final AtomicChange atomicChange, final IRecordChange changeFromRxData)
    {
        assertEquals(name, changeFromRxData.getName());
        assertEquals(atomicChange.getPutEntries(), changeFromRxData.getPutEntries());
        assertEquals(atomicChange.getOverwrittenEntries(), changeFromRxData.getOverwrittenEntries());
        assertEquals(atomicChange.getRemovedEntries(), changeFromRxData.getRemovedEntries());
        assertEquals(atomicChange.getSubMapKeys(), changeFromRxData.getSubMapKeys());
        for (String smk : atomicChange.getSubMapKeys())
        {
            assertEquals(atomicChange.getSubMapAtomicChange(smk).getPutEntries(),
                changeFromRxData.getSubMapAtomicChange(smk).getPutEntries());
            assertEquals(atomicChange.getSubMapAtomicChange(smk).getOverwrittenEntries(),
                changeFromRxData.getSubMapAtomicChange(smk).getOverwrittenEntries());
            assertEquals(atomicChange.getSubMapAtomicChange(smk).getRemovedEntries().keySet(),
                changeFromRxData.getSubMapAtomicChange(smk).getRemovedEntries().keySet());
        }
    }
}
