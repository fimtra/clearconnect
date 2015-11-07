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
package com.fimtra.datafission.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.AbstractValue;
import com.fimtra.datafission.field.BlobValue;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;

/**
 * Tests for the {@link IValue} implementations.
 * 
 * @author Ramon Servadei
 */
public class ValueTest
{
    static final int MAX = 10000000;

    @Before
    public void setUp() throws Exception
    {
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testStringConversion()
    {
        final Random random = new Random();
        for (int i = 0; i < 1000; i++)
        {
            doConvertTest(LongValue.valueOf(random.nextLong()));
            doConvertTest(new DoubleValue((random.nextBoolean() ? -random.nextDouble() : random.nextDouble())));
            // note that we do a LONG so that we can test doubleValue and longValue methods
            // with no numberFormatException
            doConvertTest(new TextValue("" + random.nextLong()));
        }
    }

    @Test
    public void testBlankLongValueConversion()
    {
        doConvertTest(new LongValue());
    }

    @Test
    public void testBlankDoubleValueConversion()
    {
        doConvertTest(new DoubleValue());
    }

    @Test
    public void testBlankTextValueConversion()
    {
        doConvertTest(new TextValue());
    }

    public void doConvertTest(IValue v)
    {
        final IValue result = AbstractValue.constructFromStringValue(v.toString());
        assertEquals(v, result);
        assertNotSame(v, result);
        assertEquals(v.doubleValue(), result.doubleValue(), 0.0001);
        assertEquals(v.longValue(), result.longValue());
        assertEquals(v.textValue(), result.textValue());
    }

    @Test
    public void testEquals()
    {
        LongValue l1 = LongValue.valueOf(1), l2 = LongValue.valueOf(2), l3 = LongValue.valueOf(1);
        TextValue t1 = new TextValue("1"), t2 = new TextValue("2"), t3 = new TextValue("1");
        DoubleValue d1 = new DoubleValue(1.1d), d2 = new DoubleValue(1.2d), d3 = new DoubleValue(1.1d);
        assertEquals(l1, l3);
        assertEquals(l1, l1);
        assertEquals(d1, d3);
        assertEquals(d1, d1);
        assertEquals(t1, t3);
        assertEquals(t1, t1);

        assertFalse(l1.equals(l2));
        assertFalse(l1.equals(t1));
        assertFalse(l1.equals(t2));
        assertFalse(l1.equals(t3));
        assertFalse(l1.equals(d1));
        assertFalse(l1.equals(d2));
        assertFalse(l1.equals(d3));

        assertFalse(d1.equals(d2));
        assertFalse(d1.equals(t1));
        assertFalse(d1.equals(t2));
        assertFalse(d1.equals(t3));

        assertFalse(t1.equals(t2));
    }

    @Test
    public void testCompare()
    {
        assertEquals(1, new TextValue("1").compareTo(null));
        assertEquals(1, LongValue.valueOf(1).compareTo(null));
        assertEquals(1, new DoubleValue(1).compareTo(null));

        assertEquals(0, new TextValue("1").compareTo(new TextValue("1")));
        assertTrue(new TextValue("-1").compareTo(new TextValue("1")) < 0);
        assertTrue(new TextValue("1").compareTo(new TextValue("-1")) > 0);

        assertEquals(0, LongValue.valueOf(1).compareTo(LongValue.valueOf(1)));
        assertTrue(LongValue.valueOf(-1).compareTo(LongValue.valueOf(1)) < 0);
        assertTrue(LongValue.valueOf(1).compareTo(LongValue.valueOf(-1)) > 0);

        assertEquals(0, new DoubleValue(1).compareTo(new DoubleValue(1)));
        assertTrue(new DoubleValue(-1).compareTo(new DoubleValue(1)) < 0);
        assertTrue(new DoubleValue(1).compareTo(new DoubleValue(-1)) > 0);
    }

    @Test
    public void testCompareWithDifferingTypes()
    {
        assertEquals(0, new DoubleValue(1).compareTo(LongValue.valueOf(1)));
        assertTrue(new DoubleValue(-1).compareTo(LongValue.valueOf(1)) < 0);
        assertTrue(new DoubleValue(1).compareTo(LongValue.valueOf(-1)) > 0);

        assertEquals(0, LongValue.valueOf(1).compareTo(new DoubleValue(1)));
        assertTrue(LongValue.valueOf(-1).compareTo(new DoubleValue(1)) < 0);
        assertTrue(LongValue.valueOf(1).compareTo(new DoubleValue(-1)) > 0);
        assertTrue(LongValue.valueOf(21).compareTo(new TextValue("20")) > 0);

        assertEquals(0, new DoubleValue(1).compareTo(new TextValue("1.0")));
        assertTrue(new DoubleValue(-1).compareTo(new TextValue("1.0")) < 0);
        assertTrue(new DoubleValue(1).compareTo(new TextValue("-1.0")) > 0);
        assertTrue(new DoubleValue(1).compareTo(new TextValue("")) > 0);
    }

    @Test
    public void testByteConversionLongValue()
    {
        LongValue val;
        ByteBuffer reuse8ByteBuffer = ByteBuffer.allocate(8);
        byte[] bytes;
        for (int i = 0; i < MAX; i++)
        {
            val = LongValue.valueOf(i);
            bytes = AbstractValue.toBytes(val, reuse8ByteBuffer);
            assertEquals(val, AbstractValue.fromBytes(val.getType(), ByteBuffer.wrap(bytes), bytes.length));
            reuse8ByteBuffer.clear();
        }
    }

    @Test
    public void testByteConversionDoubleValue()
    {
        DoubleValue val;
        ByteBuffer reuse8ByteBuffer = ByteBuffer.allocate(8);
        byte[] bytes;
        for (int i = 0; i < MAX; i++)
        {
            val = new DoubleValue(i);
            bytes = AbstractValue.toBytes(val, reuse8ByteBuffer);
            assertEquals(val, AbstractValue.fromBytes(val.getType(), ByteBuffer.wrap(bytes), bytes.length));
            reuse8ByteBuffer.clear();
        }
    }

    @Test
    public void testByteConversionTextValue()
    {
        TextValue val;
        ByteBuffer reuse8ByteBuffer = ByteBuffer.allocate(8);
        byte[] bytes;
        val = new TextValue("" + System.currentTimeMillis() + "-" + new Random().nextDouble());
        bytes = AbstractValue.toBytes(val, reuse8ByteBuffer);
        assertEquals(val, AbstractValue.fromBytes(val.getType(), ByteBuffer.wrap(bytes), bytes.length));
        reuse8ByteBuffer.clear();
    }

    @Test
    public void testByteConversionBlobValue()
    {
        BlobValue val;
        ByteBuffer reuse8ByteBuffer = ByteBuffer.allocate(8);
        byte[] bytes;
        val = new BlobValue("1a0f3416");
        bytes = AbstractValue.toBytes(val, reuse8ByteBuffer);
        assertEquals(val, AbstractValue.fromBytes(val.getType(), ByteBuffer.wrap(bytes), bytes.length));
        reuse8ByteBuffer.clear();
    }
}
