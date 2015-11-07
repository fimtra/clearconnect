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
import static org.junit.Assert.assertSame;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IValue.TypeEnum;

/**
 * Tests for the {@link LongValue}
 * 
 * @author Ramon Servadei
 */
public class LongValueTest
{
    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testEquals()
    {
        assertEquals(LongValue.valueOf(1), LongValue.valueOf(1));
        assertFalse(LongValue.valueOf(1).equals(LongValue.valueOf(11)));
    }

    @Test
    public void testGetType()
    {
        assertEquals(TypeEnum.LONG, LongValue.valueOf(1).getType());
    }

    @Test
    public void testCache()
    {
        for (int i = LongValue.poolBottom; i < LongValue.poolTop; i++)
        {
            assertEquals(i, LongValue.valueOf(i).longValue());
            assertSame(LongValue.valueOf(i), LongValue.valueOf(i));
        }
    }

    @Test
    public void testBeyondCache()
    {
        for (int i = LongValue.poolBottom - 1; i < LongValue.poolTop + 1; i++)
        {
            assertEquals(i, LongValue.valueOf(i).longValue());
        }
    }

    @Test
    public void testGet()
    {
        assertEquals(1, LongValue.get(LongValue.valueOf(1), -1));
        assertEquals(-1, LongValue.get(DoubleValue.valueOf(1), -1));
        assertEquals(-1, LongValue.get(TextValue.valueOf("1"), -1));
        assertEquals(-1, LongValue.get(null, -1));
    }
}
