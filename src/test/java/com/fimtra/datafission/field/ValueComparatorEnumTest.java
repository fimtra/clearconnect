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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.field.BlobValue;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.datafission.field.ValueComparatorEnum;

/**
 * Tests for the {@link ValueComparatorEnum}
 * 
 * @author Ramon Servadei
 */
public class ValueComparatorEnumTest
{

    TextValue[] textValues = { TextValue.valueOf("0"), TextValue.valueOf("1"), TextValue.valueOf("2"),
        TextValue.valueOf("3"), };
    DoubleValue[] doubleValues = { DoubleValue.valueOf(0), DoubleValue.valueOf(1), DoubleValue.valueOf(2),
        DoubleValue.valueOf(3), };
    LongValue[] longValues =
        { LongValue.valueOf(0), LongValue.valueOf(1), LongValue.valueOf(2), LongValue.valueOf(3), };
    BlobValue[] blobValues = { BlobValue.toBlob("0"), BlobValue.toBlob("1"), BlobValue.toBlob("2"),
        BlobValue.toBlob("3"), };

    TextValue[] textValues2 = { TextValue.valueOf("0"), TextValue.valueOf("1"), TextValue.valueOf("2"),
        TextValue.valueOf("3"), };
    DoubleValue[] doubleValues2 = { DoubleValue.valueOf(0), DoubleValue.valueOf(1), DoubleValue.valueOf(2),
        DoubleValue.valueOf(3), };
    LongValue[] longValues2 =
        { LongValue.valueOf(0), LongValue.valueOf(1), LongValue.valueOf(2), LongValue.valueOf(3), };
    BlobValue[] blobValues2 = { BlobValue.toBlob("0"), BlobValue.toBlob("1"), BlobValue.toBlob("2"),
        BlobValue.toBlob("3"), };

    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testEQS()
    {
        final ValueComparatorEnum op = ValueComparatorEnum.EQS;

        assertTrue(op.evaluate(this.textValues[0], this.textValues2[0]));
        assertTrue(op.evaluate(this.doubleValues[0], this.doubleValues2[0]));
        assertTrue(op.evaluate(this.longValues[0], this.longValues2[0]));

        // test same type, different values
        assertFalse(op.evaluate(this.textValues[0], this.textValues2[1]));
        assertFalse(op.evaluate(this.doubleValues[0], this.doubleValues2[1]));
        assertFalse(op.evaluate(this.longValues[0], this.longValues2[1]));

        // test different types
        testDifferentTypes(op, 0, 0);
    }

    @Test
    public void testNEQ()
    {
        final ValueComparatorEnum op = ValueComparatorEnum.NEQ;

        // test same type, different values
        assertTrue(op.evaluate(this.textValues[0], this.textValues2[1]));
        assertTrue(op.evaluate(this.doubleValues[0], this.doubleValues2[1]));
        assertTrue(op.evaluate(this.longValues[0], this.longValues2[1]));

        // test same type, same values
        assertFalse(op.evaluate(this.textValues[0], this.textValues2[0]));
        assertFalse(op.evaluate(this.doubleValues[0], this.doubleValues2[0]));
        assertFalse(op.evaluate(this.longValues[0], this.longValues2[0]));

        // test different types - NEQ will be true for all these
        assertTrue(op.evaluate(null, this.textValues[0]));
        assertTrue(op.evaluate(null, this.doubleValues[0]));
        assertTrue(op.evaluate(null, this.longValues[0]));
        assertTrue(op.evaluate(null, this.blobValues[0]));
        assertTrue(op.evaluate(this.doubleValues[0], null));
        assertTrue(op.evaluate(this.longValues[0], null));
        assertTrue(op.evaluate(this.blobValues[0], null));
        assertTrue(op.evaluate(this.textValues[0], null));
        assertTrue(op.evaluate(this.doubleValues[0], this.textValues[0]));
        assertTrue(op.evaluate(this.longValues[0], this.textValues[0]));
        assertTrue(op.evaluate(this.blobValues[0], this.textValues[0]));
        assertTrue(op.evaluate(this.textValues[0], this.doubleValues[0]));
        assertTrue(op.evaluate(this.longValues[0], this.doubleValues[0]));
        assertTrue(op.evaluate(this.blobValues[0], this.doubleValues[0]));
        assertTrue(op.evaluate(this.textValues[0], this.longValues[0]));
        assertTrue(op.evaluate(this.doubleValues[0], this.longValues[0]));
        assertTrue(op.evaluate(this.blobValues[0], this.longValues[0]));
        assertTrue(op.evaluate(this.textValues[0], this.blobValues[0]));
        assertTrue(op.evaluate(this.doubleValues[0], this.blobValues[0]));
        assertTrue(op.evaluate(this.longValues[0], this.blobValues[0]));
    }

    @Test
    public void testGT()
    {
        final ValueComparatorEnum op = ValueComparatorEnum.GT;

        final int i1 = 1;
        final int i2 = 0;
        assertTrue(op.evaluate(this.textValues[i1], this.textValues2[i2]));
        assertTrue(op.evaluate(this.doubleValues[i1], this.doubleValues2[i2]));
        assertTrue(op.evaluate(this.longValues[i1], this.longValues2[i2]));

        assertFalse(op.evaluate(this.textValues[i1], this.textValues2[i1]));
        assertFalse(op.evaluate(this.doubleValues[i1], this.doubleValues2[i1]));
        assertFalse(op.evaluate(this.longValues[i1], this.longValues2[i1]));

        // test same type, different values
        assertFalse(op.evaluate(this.textValues[i2], this.textValues2[i1]));
        assertFalse(op.evaluate(this.doubleValues[i2], this.doubleValues2[i1]));
        assertFalse(op.evaluate(this.longValues[i2], this.longValues2[i1]));

        // test different types
        testDifferentTypes(op, i2, i1);
    }

    @Test
    public void testLT()
    {
        final ValueComparatorEnum op = ValueComparatorEnum.LT;

        final int i1 = 0;
        final int i2 = 1;
        assertTrue(op.evaluate(this.textValues[i1], this.textValues2[i2]));
        assertTrue(op.evaluate(this.doubleValues[i1], this.doubleValues2[i2]));
        assertTrue(op.evaluate(this.longValues[i1], this.longValues2[i2]));

        assertFalse(op.evaluate(this.textValues[i1], this.textValues2[i1]));
        assertFalse(op.evaluate(this.doubleValues[i1], this.doubleValues2[i1]));
        assertFalse(op.evaluate(this.longValues[i1], this.longValues2[i1]));

        // test same type, different values
        assertFalse(op.evaluate(this.textValues[i2], this.textValues2[i1]));
        assertFalse(op.evaluate(this.doubleValues[i2], this.doubleValues2[i1]));
        assertFalse(op.evaluate(this.longValues[i2], this.longValues2[i1]));

        // test different types
        testDifferentTypes(op, i1, i2);
    }

    @Test
    public void testGTE()
    {
        final ValueComparatorEnum op = ValueComparatorEnum.GTE;

        final int i1 = 1;
        final int i2 = 0;
        assertTrue(op.evaluate(this.textValues[i1], this.textValues2[i2]));
        assertTrue(op.evaluate(this.doubleValues[i1], this.doubleValues2[i2]));
        assertTrue(op.evaluate(this.longValues[i1], this.longValues2[i2]));

        assertTrue(op.evaluate(this.textValues[i1], this.textValues2[i1]));
        assertTrue(op.evaluate(this.doubleValues[i1], this.doubleValues2[i1]));
        assertTrue(op.evaluate(this.longValues[i1], this.longValues2[i1]));

        // test same type, different values
        assertFalse(op.evaluate(this.textValues[i2], this.textValues2[i1]));
        assertFalse(op.evaluate(this.doubleValues[i2], this.doubleValues2[i1]));
        assertFalse(op.evaluate(this.longValues[i2], this.longValues2[i1]));

        // test different types
        testDifferentTypes(op, i2, i1);
    }

    @Test
    public void testLTE()
    {
        final ValueComparatorEnum op = ValueComparatorEnum.LTE;

        final int i1 = 0;
        final int i2 = 1;
        assertTrue(op.evaluate(this.textValues[i1], this.textValues2[i2]));
        assertTrue(op.evaluate(this.doubleValues[i1], this.doubleValues2[i2]));
        assertTrue(op.evaluate(this.longValues[i1], this.longValues2[i2]));

        assertTrue(op.evaluate(this.textValues[i1], this.textValues2[i1]));
        assertTrue(op.evaluate(this.doubleValues[i1], this.doubleValues2[i1]));
        assertTrue(op.evaluate(this.longValues[i1], this.longValues2[i1]));

        // test same type, different values
        assertFalse(op.evaluate(this.textValues[i2], this.textValues2[i1]));
        assertFalse(op.evaluate(this.doubleValues[i2], this.doubleValues2[i1]));
        assertFalse(op.evaluate(this.longValues[i2], this.longValues2[i1]));

        // test different types
        testDifferentTypes(op, i2, i1);
    }

    private void testDifferentTypes(ValueComparatorEnum op, int i1, int i2)
    {
        assertFalse(op.evaluate(null, this.textValues[i2]));
        assertFalse(op.evaluate(null, this.doubleValues[i2]));
        assertFalse(op.evaluate(null, this.longValues[i2]));
        assertFalse(op.evaluate(null, this.blobValues[i2]));
        assertFalse(op.evaluate(this.doubleValues[i1], null));
        assertFalse(op.evaluate(this.longValues[i1], null));
        assertFalse(op.evaluate(this.blobValues[i1], null));
        assertFalse(op.evaluate(this.textValues[i1], null));

        assertFalse(op.evaluate(this.doubleValues[i1], this.textValues[i2]));
        assertFalse(op.evaluate(this.longValues[i1], this.textValues[i2]));
        assertFalse(op.evaluate(this.blobValues[i1], this.textValues[i2]));
        assertFalse(op.evaluate(this.textValues[i1], this.doubleValues[i2]));
        assertFalse(op.evaluate(this.longValues[i1], this.doubleValues[i2]));
        assertFalse(op.evaluate(this.blobValues[i1], this.doubleValues[i2]));
        assertFalse(op.evaluate(this.textValues[i1], this.longValues[i2]));
        assertFalse(op.evaluate(this.doubleValues[i1], this.longValues[i2]));
        assertFalse(op.evaluate(this.blobValues[i1], this.longValues[i2]));
        assertFalse(op.evaluate(this.textValues[i1], this.blobValues[i2]));
        assertFalse(op.evaluate(this.doubleValues[i1], this.blobValues[i2]));
        assertFalse(op.evaluate(this.longValues[i1], this.blobValues[i2]));
    }

}
