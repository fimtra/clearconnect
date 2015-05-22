/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.expression;

/**
 * The factory for creating operator expressions, generally used by the expression framework
 * internals
 * 
 * @author Ramon Servadei
 */
final class ExpressionOperatorFactory
{
    enum ExpressionOperatorEnum
    {
        FALSE, TRUE, AND, OR, DATA_SIGNATURE
    }

    static IExpression create(ExpressionOperatorEnum operator, String operandsString)
    {
        try
        {
            switch(operator)
            {
                case FALSE:
                    return new FalseExpression(AbstractExpression.getOperands(operandsString));
                case TRUE:
                    return new TrueExpression(AbstractExpression.getOperands(operandsString));
                case AND:
                    return new AndExpression(AbstractExpression.getOperands(operandsString));
                case OR:
                    return new OrExpression(AbstractExpression.getOperands(operandsString));
                case DATA_SIGNATURE:
                    return DataSignatureExpression.fromWireString(operandsString);
                default :
                    throw new UnsupportedOperationException("No implementation for operator: " + operator);
            }
        }
        catch (StringIndexOutOfBoundsException e)
        {
            throw new IllegalStateException("Could not handle operator=" + operator + " operands=" + operandsString, e);
        }
    }
}
