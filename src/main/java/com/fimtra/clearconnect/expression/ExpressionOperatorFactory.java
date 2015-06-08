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
