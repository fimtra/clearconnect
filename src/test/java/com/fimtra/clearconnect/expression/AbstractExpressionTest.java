/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
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
package com.fimtra.clearconnect.expression;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.clearconnect.expression.AbstractExpression;
import com.fimtra.clearconnect.expression.AndExpression;
import com.fimtra.clearconnect.expression.DataSignatureExpression;
import com.fimtra.clearconnect.expression.FalseExpression;
import com.fimtra.clearconnect.expression.IExpression;
import com.fimtra.clearconnect.expression.OrExpression;
import com.fimtra.clearconnect.expression.TrueExpression;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.datafission.field.ValueComparatorEnum;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link AbstractExpression}
 * 
 * @author Ramon Servadei
 */
public class AbstractExpressionTest
{

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testNoArgToFromWireString()
    {
        IExpression and = new AndExpression();
        final String wireString = and.toWireString();
        final IExpression fromWireString = AbstractExpression.fromWireString(wireString);
        System.err.println(fromWireString);
        assertEquals(and, fromWireString);
        assertTrue(and.evaluate());
    }

    @Test
    public void testToFromWireString()
    {
        IExpression and = new AndExpression(new TrueExpression(), new FalseExpression());
        final String wireString = and.toWireString();
        final IExpression fromWireString = AbstractExpression.fromWireString(wireString);
        System.err.println(fromWireString);
        assertEquals(and, fromWireString);
        assertEquals(and.toString(), fromWireString.toString());
        assertFalse(and.evaluate());
    }

    @Test
    public void testDataSignatureExpressionToFromWireString()
    {
        IExpression and =
            new AndExpression(new DataSignatureExpression("field1 with space", ValueComparatorEnum.EQS,
                LongValue.valueOf(1)), new OrExpression(new DataSignatureExpression("field2", ValueComparatorEnum.GT,
                LongValue.valueOf(2)), new DataSignatureExpression("field2 with space", ValueComparatorEnum.LT,
                TextValue.valueOf("with space"))), new DataSignatureExpression("field2 with space",
                ValueComparatorEnum.GT, LongValue.valueOf(2)));
        final String wireString = and.toWireString();
        final IExpression fromWireString = AbstractExpression.fromWireString(wireString);
        System.err.println(fromWireString);
        assertEquals(and, fromWireString);
        assertEquals(and.toString(), fromWireString.toString());
        assertFalse(and.evaluate());
    }

    @Test
    public void testDataSignatureExpressionWithSpecialCharsToFromWireString()
    {
        IExpression and =
            new AndExpression(new DataSignatureExpression("field1 with space and ( and ) <end>",
                ValueComparatorEnum.EQS, LongValue.valueOf(1)), new OrExpression(new DataSignatureExpression("field2",
                ValueComparatorEnum.GT, LongValue.valueOf(2)), new DataSignatureExpression(
                "field2 with space ) and ) <end>", ValueComparatorEnum.LT,
                TextValue.valueOf("with space and ( and ) <end>"))), new DataSignatureExpression(
                "field2 with space ) and ( <end>", ValueComparatorEnum.GT, LongValue.valueOf(2)));
        final String wireString = and.toWireString();
        final IExpression fromWireString = AbstractExpression.fromWireString(wireString);
        System.err.println(fromWireString);
        assertEquals(and, fromWireString);
        assertEquals(and.toString(), fromWireString.toString());
        assertFalse(and.evaluate());
    }
}
