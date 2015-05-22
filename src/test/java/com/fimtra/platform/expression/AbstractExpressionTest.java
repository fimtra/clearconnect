/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.expression;

import org.junit.Before;
import org.junit.Test;

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
