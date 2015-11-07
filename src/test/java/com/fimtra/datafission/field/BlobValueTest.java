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
package com.fimtra.datafission.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IValue.TypeEnum;

/**
 * Tests for the {@link BlobValue}
 * 
 * @author Ramon Servadei
 */
public class BlobValueTest
{

    private static final String _1AF3416 = "1a0f34160a0b0c0d0e0f";
    BlobValue candidate;
    byte[] bytes = new byte[] { 0x1a, 0xf, 0x34, 0x16, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f };

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new BlobValue(this.bytes);
    }

    @Test
    public void testObjectToFromBlob()
    {
        assertNotNull(BlobValue.toBlob(null));
        assertNull(BlobValue.fromBlob(null));
        String s = "Lasers";
        assertEquals(s, BlobValue.fromBlob(BlobValue.toBlob(s)));
        assertNull(BlobValue.fromBlob(BlobValue.toBlob(null)));
    }

    @Test
    public void testEquals()
    {
        assertEquals(new BlobValue(this.bytes), new BlobValue(this.bytes));
        assertFalse(new BlobValue().equals(new BlobValue(this.bytes)));
    }

    @Test
    public void testFromString()
    {
        final BlobValue other = new BlobValue();
        other.fromString(_1AF3416);
        assertEquals(other, this.candidate);
    }

    @Test
    public void testFromStringCapitals()
    {
        final BlobValue other = new BlobValue();
        other.fromString("1a0f34160A0B0C0D0E0F");
        assertEquals(other, this.candidate);
    }

    @Test
    public void testFromChar()
    {
        final BlobValue other = new BlobValue();
        char[] charArray = _1AF3416.toCharArray();
        other.fromChars(charArray, 0, charArray.length);
        assertEquals(other, this.candidate);
    }

    @Test
    public void testGetType()
    {
        assertEquals(TypeEnum.BLOB, this.candidate.getType());
    }

    @Test
    public void testLongValue()
    {
        assertEquals(10, this.candidate.longValue());
    }

    @Test
    public void testDoubleValue()
    {
        assertEquals(10.0, this.candidate.doubleValue(), 1.0);
    }

    @Test
    public void testTextValue()
    {
        assertEquals(_1AF3416, this.candidate.textValue());
    }

    @Test
    public void testSmallToFromString() throws Exception
    {
        final byte[] bytes = new byte[] { 0xf, 0x0, 0x1 };
        this.candidate = new BlobValue(bytes);
        BlobValue other = new BlobValue();
        other.fromString(this.candidate.textValue());
        assertEquals(this.candidate, other);
    }

    @Test
    public void testFullByteRangeToFromString() throws Exception
    {
        final byte[] bytes = new byte[256];
        int i = 0;
        for (int v = -128; v < 128; v++)
        {
            bytes[i++] = (byte) v;
        }
        this.candidate = new BlobValue(bytes);
        BlobValue other = new BlobValue();
        other.fromString(this.candidate.textValue());
        assertEquals(this.candidate, other);
    }
    
    @Test
    public void testGet()
    {
        byte[] sdf = new byte[1];
        assertSame(sdf, BlobValue.get(BlobValue.valueOf(sdf), null));
        assertNull(BlobValue.get(DoubleValue.valueOf(1), null));
        assertNull(BlobValue.get(LongValue.valueOf(1), null));
        assertNull(BlobValue.get(TextValue.valueOf("1"), null));
    }
}
