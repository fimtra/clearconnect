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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.Random;

import com.fimtra.datafission.IValue.TypeEnum;
import org.junit.Test;

/**
 * Tests {@link TextValue}
 * 
 * @author Ramon Servadei
 */
public class TextValueTest
{

    @Test
    public void testCreating()
    {
        TextValue textValue = TextValue.valueOf(TextValue.NULL);
        assertNotNull(textValue.textValue());
        assertSame(TextValue.NULL, textValue.textValue());
        assertEquals(Double.NaN, textValue.doubleValue(), 0.00001);
        assertEquals(0, textValue.longValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatingWithNull()
    {
        TextValue.valueOf(null);
    }

    @Test
    public void testCreatingWithNullString()
    {
        TextValue textValue = TextValue.valueOf("null");
        assertNotNull(textValue.textValue());
        assertSame(TextValue.NULL, textValue.textValue());
        assertEquals(Double.NaN, textValue.doubleValue(), 0.00001);
        assertEquals(0, textValue.longValue());
    }

    @Test
    public void testCreatingWithNewNullString()
    {
        TextValue textValue = TextValue.valueOf(new String("null"));
        assertNotNull(textValue.textValue());
        assertSame(TextValue.NULL, textValue.textValue());
        assertEquals(Double.NaN, textValue.doubleValue(), 0.00001);
        assertEquals(0, textValue.longValue());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testFromCharsWithNull()
    {
        TextValue.valueOf((char[]) null, 0, 1);
    }

    @Test
    public void testFromCharsWithNullString()
    {
        char[] chars = "null".toCharArray();
        TextValue textValue = TextValue.valueOf(chars, 0, chars.length);
        assertNotNull(textValue.textValue());
        assertSame(TextValue.NULL, textValue.textValue());
        assertEquals(Double.NaN, textValue.doubleValue(), 0.00001);
        assertEquals(0, textValue.longValue());
    }

    @Test
    public void testCallingNumericMethods()
    {
        TextValue textValue = TextValue.valueOf("not a number!");
        assertEquals(Double.NaN, textValue.doubleValue(), 0.00001);
        try
        {
            textValue.longValue();
            fail("Must throw exception");
        }
        catch (NumberFormatException e)
        {
        }
    }

    @Test
    public void testTextValueWithLong()
    {
        final Random random = new Random();
        for (int i = 0; i < 1000; i++)
        {
            long nextLong = random.nextLong();
            TextValue textValue = TextValue.valueOf("" + nextLong);
            assertEquals(nextLong, textValue.longValue());
            assertEquals(nextLong, textValue.doubleValue(), 0.00001);
        }
    }

    @Test
    public void testTextValueWithDouble()
    {
        final Random random = new Random();
        for (int i = 0; i < 1000; i++)
        {
            double nextDouble = random.nextDouble();
            TextValue textValue = TextValue.valueOf("" + nextDouble);
            assertEquals(nextDouble, textValue.doubleValue(), 0.00001);
            // Note: pointless testing longValue - it throws a numberFormatException
            try
            {
                textValue.longValue();
                fail("Should be throwing a NumberFormatException");
            }
            catch (NumberFormatException e)
            {
            }
        }
    }

    @Test
    public void testGetType()
    {
        assertEquals(TypeEnum.TEXT, TextValue.valueOf("").getType());
    }

    @Test
    public void testGet()
    {
        assertNull(null, TextValue.get(null, null));
        assertEquals("null", TextValue.get(null, "null"));
        assertEquals("1", TextValue.get(LongValue.valueOf(1), null));
        assertEquals("1.0", TextValue.get(DoubleValue.valueOf(1), null));
        assertEquals("1", TextValue.get(TextValue.valueOf("1"), null));
    }
}
