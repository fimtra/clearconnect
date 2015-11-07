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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.fimtra.tcpchannel.ByteArrayFragment;
import com.fimtra.tcpchannel.ByteArrayFragmentResolver;
import com.fimtra.tcpchannel.ByteArrayFragment.ByteArrayFragmentUtils;
import com.fimtra.tcpchannel.ByteArrayFragment.IncorrectSequenceException;
import com.fimtra.tcpchannel.ByteArrayFragmentResolver.RawByteHeaderByteArrayFragmentResolver;
import com.fimtra.tcpchannel.ByteArrayFragmentResolver.UTF8HeaderByteArrayFragmentResolver;

/**
 * Tests for the {@link ByteArrayFragment}
 * 
 * @author Ramon Servadei
 */
public class ByteArrayFragmentTest
{

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
        final ByteArrayFragment[] fragments = ByteArrayFragment.getFragmentsForTxData(data, max);
        assertEquals(expectedNumber, fragments.length);
        for (int i = 0; i < fragments.length; i++)
        {
            ByteArrayFragment byteArrayFragment = fragments[i];
            assertTrue("Got: " + byteArrayFragment.data.length + " but max is " + max,
                byteArrayFragment.data.length <= max);
        }
    }

    @Test
    public void testGetFragmentsForTxDataInternals()
    {
        byte[] data = "hellohellohellohello".getBytes();
        ByteArrayFragment[] fragmentsForTxData = ByteArrayFragment.getFragmentsForTxData(data, 5);
        assertEquals(4, fragmentsForTxData.length);
        for (int i = 0; i < fragmentsForTxData.length; i++)
        {
            assertEquals(fragmentsForTxData[0].id, fragmentsForTxData[i].id);
            assertEquals(i, fragmentsForTxData[i].sequenceId);
            assertEquals("hello", new String(fragmentsForTxData[i].data));
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
        ByteArrayFragment[] fragmentsForTxData = ByteArrayFragment.getFragmentsForTxData(data, max);
        ByteArrayFragment resolved;
        for (ByteArrayFragment fragment : fragmentsForTxData)
        {
            resolved = ByteArrayFragment.fromRxBytesRawByteHeader(fragment.toTxBytesRawByteHeader());
            assertEquals(fragment, resolved);
            assertEquals(fragment.sequenceId, resolved.sequenceId);
            assertEquals(fragment.lastElement, resolved.lastElement);
            assertArrayEquals(fragment.data, resolved.data);
        }
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
        ByteArrayFragment[] fragmentsForTxData = ByteArrayFragment.getFragmentsForTxData(data, 1);
        // force incorrect sequence here
        fragmentsForTxData[0].merge(fragmentsForTxData[2]);
    }

    @SuppressWarnings("null")
    private static void doMergeTest(byte[] data, final int max) throws IncorrectSequenceException
    {
        ByteArrayFragment[] fragmentsForTxData = ByteArrayFragment.getFragmentsForTxData(data, max);
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
        assertArrayEquals(data, resolved.getData());
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
        ByteArrayFragment[] fragmentsForTxData = ByteArrayFragment.getFragmentsForTxData(data, max);
        byte[] resolved = null;
        for (int i = 0; i < fragmentsForTxData.length; i++)
        {
            resolved = resolver.resolve(fragmentsForTxData[i].toTxBytesRawByteHeader());
        }
        assertEquals(0, resolver.fragments.size());
        assertArrayEquals(data, resolved);
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
        ByteArrayFragment[] fragmentsForTxData = ByteArrayFragment.getFragmentsForTxData(data, max);
        byte[] resolved = null;
        for (int i = 0; i < fragmentsForTxData.length; i++)
        {
            resolved = resolver.resolve(fragmentsForTxData[i].toTxBytesUTF8Header());
        }
        assertEquals(0, resolver.fragments.size());
        assertArrayEquals(data, resolved);
    }

    @Test
    public void testMergeForIncorrectSequenceUTF8HeaderResolver()
    {
        byte[] data = "hello".getBytes();
        ByteArrayFragment[] fragmentsForTxData = ByteArrayFragment.getFragmentsForTxData(data, 1);
        ByteArrayFragmentResolver resolver = new UTF8HeaderByteArrayFragmentResolver();
        assertNull(resolver.resolve(fragmentsForTxData[0].toTxBytesUTF8Header()));
        assertEquals(1, resolver.fragments.size());
        // force incorrect sequence here
        assertNull(resolver.resolve(fragmentsForTxData[2].toTxBytesUTF8Header()));
        assertEquals(0, resolver.fragments.size());
    }

    @Test
    public void testMergeForIncorrectSequenceRawBytesHeaderResolver()
    {
        byte[] data = "hello".getBytes();
        ByteArrayFragment[] fragmentsForTxData = ByteArrayFragment.getFragmentsForTxData(data, 1);
        ByteArrayFragmentResolver resolver = new RawByteHeaderByteArrayFragmentResolver();
        assertNull(resolver.resolve(fragmentsForTxData[0].toTxBytesRawByteHeader()));
        assertEquals(1, resolver.fragments.size());
        // force incorrect sequence here
        assertNull(resolver.resolve(fragmentsForTxData[2].toTxBytesRawByteHeader()));
        assertEquals(0, resolver.fragments.size());
    }

    @Test
    public void testMultiChangingDataRawByteHeader()
    {
        ByteArrayFragmentResolver resolver = new RawByteHeaderByteArrayFragmentResolver();

        final int messageCount = 5000;
        int i = 0;
        List<ByteArrayFragment> fragments = new ArrayList<ByteArrayFragment>(messageCount);
        while (i < messageCount)
        {
            ByteArrayFragment[] fragmentsForTxData =
                ByteArrayFragment.getFragmentsForTxData(("" + ++i).getBytes(), 65535);
            for (ByteArrayFragment byteArrayFragment : fragmentsForTxData)
            {
                fragments.add(byteArrayFragment);

                // add interleaved data that is not part of our check
                final ByteArrayFragment[] hbs = ByteArrayFragment.getFragmentsForTxData(new byte[] { 0x3 }, 65535);
                for (ByteArrayFragment byteArrayFragment2 : hbs)
                {
                    fragments.add(byteArrayFragment2);
                }
            }
        }
        int last = 0;
        int current;
        byte[] resolved;
        for (ByteArrayFragment byteArrayFragment : fragments)
        {
            resolved = resolver.resolve(byteArrayFragment.toTxBytesRawByteHeader());
            if (resolved != null)
            {
                try
                {
                    current = Integer.parseInt(new String(resolved));
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
        ByteArrayFragment[] fragmentsForTxData = ByteArrayFragment.getFragmentsForTxData(data, max);
        ByteArrayFragment resolved;
        for (ByteArrayFragment fragment : fragmentsForTxData)
        {
            resolved = ByteArrayFragment.fromRxBytesUTF8Header(fragment.toTxBytesUTF8Header());
            assertEquals(fragment, resolved);
            assertEquals(fragment.sequenceId, resolved.sequenceId);
            assertEquals(fragment.lastElement, resolved.lastElement);
            assertArrayEquals(fragment.data, resolved.data);
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
