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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.field.DoubleValue;

/**
 * Tests {@link DoubleValue}
 * 
 * @author Ramon Servadei
 */
public class DoubleValueTest
{

    @Test
    public void testEquals()
    {
        assertEquals(new DoubleValue(1.2), new DoubleValue(1.2));
        assertFalse(new DoubleValue(1.2).equals(new DoubleValue(1.21)));
    }

    @Test
    public void testGetType()
    {
        assertEquals(TypeEnum.DOUBLE, new DoubleValue(1.2).getType());
    }

    @Test
    public void testInitialisedWithNaN()
    {
        assertEquals(Double.NaN, new DoubleValue().doubleValue(), 0.0001);
    }

    @Test
    public void testGet()
    {
        assertEquals(1.0, DoubleValue.get(DoubleValue.valueOf(1), -1), 0.01);
        assertTrue(Double.isNaN(DoubleValue.get(null, Double.NaN)));
        assertTrue(Double.isNaN(DoubleValue.get(LongValue.valueOf(1), Double.NaN)));
        assertTrue(Double.isNaN(DoubleValue.get(TextValue.valueOf("1"), Double.NaN)));
    }
}
