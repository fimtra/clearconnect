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
package com.fimtra.tcpchannel;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.fimtra.tcpchannel.ByteArrayFragment.ByteArrayFragmentUtils;
import com.fimtra.tcpchannel.ByteArrayFragment.IncorrectSequenceException;
import com.fimtra.tcpchannel.ByteArrayFragmentResolver.RawByteHeaderByteArrayFragmentResolver;
import com.fimtra.tcpchannel.ByteArrayFragmentResolver.UTF8HeaderByteArrayFragmentResolver;
import com.fimtra.util.ByteBufferUtils;
import com.fimtra.util.LowGcLinkedList;

/**
 * Tests for the {@link ByteArrayFragment}
 * 
 * @author Ramon Servadei
 */
public class ByteArrayFragmentTest
{

    final static TxByteArrayFragment[][][] fragmentsArrayPool;
    final static LowGcLinkedList<TxByteArrayFragment> fragmentsPool;
    static
    {
        fragmentsArrayPool = new TxByteArrayFragment[1][2][];
        fragmentsPool = new LowGcLinkedList<TxByteArrayFragment>();
    }

    /**
     * Test method for
     * {@link com.fimtra.tcpchannel.ByteArrayFragment#getFragmentsForTxData(byte[], int)}.
     */
    @Test
    public void testGetFragmentsForTxData()
    {
        byte[] data = "hello".getBytes();
        doTestGetFragmentsForTxData(data, 1, 5);
        doTestGetFragmentsForTxData(data, 2, 3);
        doTestGetFragmentsForTxData(data, 3, 2);
        doTestGetFragmentsForTxData(data, 4, 2);
        doTestGetFragmentsForTxData(data, 5, 1);
        doTestGetFragmentsForTxData(data, 6, 1);
    }

    private static void doTestGetFragmentsForTxData(byte[] data, int max, int expectedNumber)
    {
        final ByteArrayFragment[] fragments =
            TxByteArrayFragment.getFragmentsForTxData(data, max, fragmentsArrayPool, fragmentsPool);
        assertEquals(expectedNumber, fragments.length);
        for (int i = 0; i < fragments.length; i++)
        {
            ByteArrayFragment byteArrayFragment = fragments[i];
            assertTrue("Got: " + byteArrayFragment.length + " but max is " + max, byteArrayFragment.length <= max);
        }
    }

    @Test
    public void testGetFragmentsForTxDataInternals()
    {
        byte[] data = "hellohellohellohello".getBytes();
        ByteArrayFragment[] fragmentsForTxData =
            TxByteArrayFragment.getFragmentsForTxData(data, 5, fragmentsArrayPool, fragmentsPool);
        assertEquals(4, fragmentsForTxData.length);
        for (int i = 0; i < fragmentsForTxData.length; i++)
        {
            assertEquals(fragmentsForTxData[0].id, fragmentsForTxData[i].id);
            assertEquals(i, fragmentsForTxData[i].sequenceId);
            final ByteBuffer resolved = fragmentsForTxData[i].getData();
            assertEquals("hello",
                new String(resolved.array(), resolved.position(), resolved.limit() - resolved.position()));
            if (i < (fragmentsForTxData.length - 1))
            {
                assertFalse(fragmentsForTxData[i].isLastElement());
            }
            else
            {
                assertTrue(fragmentsForTxData[i].isLastElement());
            }
        }
    }

    @Test
    public void testToTxAndFromRxBytesRawByteHeader()
    {
        for (int j = 0; j < 100; j += 5)
        {
            byte[] data = getReallyBigString(j).getBytes();
            for (int i = 1; i < data.length * 2; i++)
            {
                doToTxAndFromRxBytesRawByteHeaderTest(data, i);
            }
        }
    }

    private static void doToTxAndFromRxBytesRawByteHeaderTest(byte[] data, final int max)
    {
        TxByteArrayFragment[] fragmentsForTxData =
            TxByteArrayFragment.getFragmentsForTxData(data, max, fragmentsArrayPool, fragmentsPool);
        ByteArrayFragment resolved;
        for (TxByteArrayFragment fragment : fragmentsForTxData)
        {
            resolved = ByteArrayFragment.fromRxBytesRawByteHeader(joinBuffers(fragment.toTxBytesRawByteHeader()));
            assertEquals(fragment, resolved);
            assertEquals(fragment.sequenceId, resolved.sequenceId);
            assertEquals(fragment.lastElement, resolved.lastElement);
            assertArrayEquals(ByteBufferUtils.asBytes(fragment.getData()), ByteBufferUtils.asBytes(resolved.getData()));
        }
    }

    private static ByteBuffer joinBuffers(ByteBuffer[] buffers)
    {
        return (ByteBufferUtils.join(buffers));
    }

    /**
     * Test method for
     * {@link com.fimtra.tcpchannel.ByteArrayFragment#merge(com.fimtra.tcpchannel.ByteArrayFragment)}
     * .
     * 
     * @throws IncorrectSequenceException
     */
    @Test
    public void testMerge() throws IncorrectSequenceException
    {
        for (int j = 0; j < 100; j += 5)
        {
            byte[] data = getReallyBigString(j).getBytes();
            for (int i = 1; i < data.length * 2; i++)
            {
                doMergeTest(data, i);
            }
        }
    }

    @Test(expected = IncorrectSequenceException.class)
    public void testMergeForIncorrectSequence() throws IncorrectSequenceException
    {
        byte[] data = "hello".getBytes();
        ByteArrayFragment[] fragmentsForTxData =
            TxByteArrayFragment.getFragmentsForTxData(data, 1, fragmentsArrayPool, fragmentsPool);
        // force incorrect sequence here
        fragmentsForTxData[0].merge(fragmentsForTxData[2]);
    }

    @SuppressWarnings("null")
    private static void doMergeTest(byte[] data, final int max) throws IncorrectSequenceException
    {
        ByteArrayFragment[] fragmentsForTxData =
            TxByteArrayFragment.getFragmentsForTxData(data, max, fragmentsArrayPool, fragmentsPool);
        ByteArrayFragment resolved = null;
        for (int i = 0; i < fragmentsForTxData.length; i++)
        {
            if (resolved == null)
            {
                resolved = fragmentsForTxData[i];
            }
            else
            {
                resolved.merge(fragmentsForTxData[i]);
            }
        }
        assertArrayEquals(data, ByteBufferUtils.asBytes(resolved.getData()));
    }

    private static String getReallyBigString(int max)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < max; i++)
        {
            sb.append("hello");
        }
        return sb.toString();
    }

    @Test
    public void testRawByteHeaderResolver()
    {
        ByteArrayFragmentResolver resolver = new RawByteHeaderByteArrayFragmentResolver();
        for (int j = 0; j < 100; j += 5)
        {
            byte[] data = getReallyBigString(j).getBytes();
            for (int i = 1; i < data.length * 2; i++)
            {
                doTestResolverRawBytes(data, i, resolver);
            }
        }
        assertEquals(0, resolver.fragments.size());
    }

    private static void doTestResolverRawBytes(byte[] data, final int max, ByteArrayFragmentResolver resolver)
    {
        TxByteArrayFragment[] fragmentsForTxData =
            TxByteArrayFragment.getFragmentsForTxData(data, max, fragmentsArrayPool, fragmentsPool);
        ByteBuffer resolved = null;
        for (int i = 0; i < fragmentsForTxData.length; i++)
        {
            resolved = resolver.resolve(joinBuffers(fragmentsForTxData[i].toTxBytesRawByteHeader()));
        }
        assertEquals(0, resolver.fragments.size());
        assertArrayEquals(data, ByteBufferUtils.asBytes(resolved));
    }

    @Test
    public void testUTF8HeaderResolver()
    {
        ByteArrayFragmentResolver resolver = new UTF8HeaderByteArrayFragmentResolver();
        for (int j = 0; j < 100; j += 5)
        {
            byte[] data = getReallyBigString(j).getBytes();
            for (int i = 1; i < data.length * 2; i++)
            {
                doTestResolverUTF8(data, i, resolver);
            }
        }
        assertEquals(0, resolver.fragments.size());
    }

    private static void doTestResolverUTF8(byte[] data, final int max, ByteArrayFragmentResolver resolver)
    {
        TxByteArrayFragment[] fragmentsForTxData =
            TxByteArrayFragment.getFragmentsForTxData(data, max, fragmentsArrayPool, fragmentsPool);
        ByteBuffer resolved = null;
        for (int i = 0; i < fragmentsForTxData.length; i++)
        {
            resolved = resolver.resolve(joinBuffers(fragmentsForTxData[i].toTxBytesUTF8Header()));
        }
        assertEquals(0, resolver.fragments.size());
        assertArrayEquals(data, ByteBufferUtils.asBytes(resolved));
    }

    @Test
    public void testMergeForIncorrectSequenceUTF8HeaderResolver()
    {
        byte[] data = "hello".getBytes();
        TxByteArrayFragment[] fragmentsForTxData =
            TxByteArrayFragment.getFragmentsForTxData(data, 1, fragmentsArrayPool, fragmentsPool);
        ByteArrayFragmentResolver resolver = new UTF8HeaderByteArrayFragmentResolver();
        assertNull(resolver.resolve(joinBuffers(fragmentsForTxData[0].toTxBytesUTF8Header())));
        assertEquals(1, resolver.fragments.size());
        // force incorrect sequence here
        assertNull(resolver.resolve(joinBuffers(fragmentsForTxData[2].toTxBytesUTF8Header())));
        assertEquals(0, resolver.fragments.size());
    }

    @Test
    public void testMergeForIncorrectSequenceRawBytesHeaderResolver()
    {
        byte[] data = "hello".getBytes();
        TxByteArrayFragment[] fragmentsForTxData =
            TxByteArrayFragment.getFragmentsForTxData(data, 1, fragmentsArrayPool, fragmentsPool);
        ByteArrayFragmentResolver resolver = new RawByteHeaderByteArrayFragmentResolver();
        assertNull(resolver.resolve(joinBuffers(fragmentsForTxData[0].toTxBytesRawByteHeader())));
        assertEquals(1, resolver.fragments.size());
        // force incorrect sequence here
        assertNull(resolver.resolve(joinBuffers(fragmentsForTxData[2].toTxBytesRawByteHeader())));
        assertEquals(0, resolver.fragments.size());
    }

    @Test
    public void testMultiChangingDataRawByteHeader()
    {
        ByteArrayFragmentResolver resolver = new RawByteHeaderByteArrayFragmentResolver();

        final int messageCount = 5000;
        int i = 0;
        List<TxByteArrayFragment> fragments = new ArrayList<TxByteArrayFragment>(messageCount);
        while (i < messageCount)
        {
            TxByteArrayFragment[] fragmentsForTxData = TxByteArrayFragment.getFragmentsForTxData(("" + ++i).getBytes(),
                65535, fragmentsArrayPool, fragmentsPool);
            for (TxByteArrayFragment byteArrayFragment : fragmentsForTxData)
            {
                fragments.add(byteArrayFragment);

                // add interleaved data that is not part of our check
                final TxByteArrayFragment[] hbs = TxByteArrayFragment.getFragmentsForTxData(new byte[] { 0x3 }, 65535,
                    fragmentsArrayPool, fragmentsPool);
                for (TxByteArrayFragment byteArrayFragment2 : hbs)
                {
                    fragments.add(byteArrayFragment2);
                }
            }
        }
        int last = 0;
        int current;
        ByteBuffer resolved;
        for (TxByteArrayFragment byteArrayFragment : fragments)
        {
            resolved = resolver.resolve(joinBuffers(byteArrayFragment.toTxBytesRawByteHeader()));
            if (resolved != null)
            {
                try
                {
                    current = Integer.parseInt(
                        new String(resolved.array(), resolved.position(), resolved.limit() - resolved.position()));
                    assertTrue("current=" + current + ", last=" + last, current == (++last));
                }
                catch (NumberFormatException e)
                {
                    // this is for the heartbeats
                }
            }
        }
    }

    @Test
    public void testToTxAndFromRxBytesUTF8Header()
    {
        for (int j = 0; j < 100; j += 5)
        {
            byte[] data = getReallyBigString(j).getBytes();
            for (int i = 1; i < data.length * 2; i++)
            {
                doToTxAndFromRxBytesUTF8HeaderTest(data, i);
            }
        }
    }

    private static void doToTxAndFromRxBytesUTF8HeaderTest(byte[] data, final int max)
    {
        TxByteArrayFragment[] fragmentsForTxData =
            TxByteArrayFragment.getFragmentsForTxData(data, max, fragmentsArrayPool, fragmentsPool);
        ByteArrayFragment resolved;
        for (TxByteArrayFragment fragment : fragmentsForTxData)
        {
            resolved = ByteArrayFragment.fromRxBytesUTF8Header(joinBuffers(fragment.toTxBytesUTF8Header()));
            assertEquals(fragment, resolved);
            assertEquals(fragment.sequenceId, resolved.sequenceId);
            assertEquals(fragment.lastElement, resolved.lastElement);
            assertArrayEquals(ByteBufferUtils.asBytes(fragment.getData()), ByteBufferUtils.asBytes(resolved.getData()));
        }
    }

    @Test
    public void testSplit3NumbersByPipe()
    {
        assertArrayEquals(new int[] { 1, 2, 3 }, ByteArrayFragmentUtils.split3NumbersByPipe("1|2|3|"));
        assertArrayEquals(new int[] { 1, 2, 3 }, ByteArrayFragmentUtils.split3NumbersByPipe("1|2|3"));
    }

    @Test
    public void testPad4DigitWithLeadingZeros()
    {
        assertEquals("0000", ByteArrayFragmentUtils.pad4DigitWithLeadingZeros(0));
        assertEquals("0001", ByteArrayFragmentUtils.pad4DigitWithLeadingZeros(1));
        assertEquals("0010", ByteArrayFragmentUtils.pad4DigitWithLeadingZeros(10));
        assertEquals("0100", ByteArrayFragmentUtils.pad4DigitWithLeadingZeros(100));
        assertEquals("1000", ByteArrayFragmentUtils.pad4DigitWithLeadingZeros(1000));
    }
}
