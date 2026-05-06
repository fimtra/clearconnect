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

import static com.fimtra.datafission.DataFissionProperties.Values.LONG_VALUE_POOL_SIZE;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.fimtra.datafission.IValue.TypeEnum;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link DoubleValue}
 *
 * @author Ramon Servadei
 */
public class DoubleValueTest
{
    static final double delta = 0.000000001d;

    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testCache()
    {
        for (int i = -LONG_VALUE_POOL_SIZE; i <= LONG_VALUE_POOL_SIZE; i++)
        {
            assertEquals(i, LongValue.valueOf(i)
                    .longValue());
            assertSame(LongValue.valueOf(i), LongValue.valueOf(i));
            assertSame(LongValue.valueOf(i)
                    .textValue(), LongValue.valueOf(i)
                    .textValue());
            assertSame(LongValue.valueOf(i)
                    .toString(), LongValue.valueOf(i)
                    .toString());
        }
    }

    @Test
    public void testBeyondCache()
    {
        int lVal = -LONG_VALUE_POOL_SIZE - 1;
        assertEquals(lVal, LongValue.valueOf(lVal)
                .longValue());
        assertNotSame(LongValue.valueOf(lVal), LongValue.valueOf(lVal));
        assertNotSame(LongValue.valueOf(lVal)
                .textValue(), LongValue.valueOf(lVal)
                .textValue());

        lVal = LONG_VALUE_POOL_SIZE + 1;
        assertEquals(lVal, LongValue.valueOf(lVal)
                .longValue());
        assertNotSame(LongValue.valueOf(lVal), LongValue.valueOf(lVal));
        assertNotSame(LongValue.valueOf(lVal)
                .textValue(), LongValue.valueOf(lVal)
                .textValue());
    }

    @Test
    public void testEquals()
    {
        assertEquals(new DoubleValue(1.2), new DoubleValue(1.2));
        assertNotEquals(new DoubleValue(1.2), new DoubleValue(1.21));
    }

    @Test
    public void testGetType()
    {
        assertEquals(TypeEnum.DOUBLE, new DoubleValue(1.2).getType());
    }

    @Test
    public void testInitialisedWithNaN()
    {
        assertEquals(NaN, new DoubleValue().doubleValue(), delta);
    }

    @Test
    public void testGet()
    {
        assertEquals(1.0, DoubleValue.get(DoubleValue.valueOf(1), -1), delta);
        assertTrue(Double.isNaN(DoubleValue.get(null, NaN)));
        assertTrue(Double.isNaN(DoubleValue.get(LongValue.valueOf(1), NaN)));
        assertTrue(Double.isNaN(DoubleValue.get(TextValue.valueOf("1"), NaN)));
    }

    @Test
    public void testToString()
    {
        for (int i = -LONG_VALUE_POOL_SIZE; i <= LONG_VALUE_POOL_SIZE; i++)
        {
            assertEquals(Double.toString(i), new DoubleValue(i).textValue());
        }

        assertEquals("Infinity", DoubleValue.valueOf(POSITIVE_INFINITY)
                .textValue());
        assertEquals("-Infinity", DoubleValue.valueOf(NEGATIVE_INFINITY)
                .textValue());
        assertEquals("NaN", DoubleValue.valueOf(NaN)
                .textValue());
        assertEquals("99.0", DoubleValue.valueOf(99.0)
                .textValue());
        assertEquals("99.0", DoubleValue.valueOf(99.00000000)
                .textValue());
        assertEquals("99.0001", DoubleValue.valueOf(99.0001)
                .textValue());
    }
}








