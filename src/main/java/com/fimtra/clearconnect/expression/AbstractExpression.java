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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fimtra.clearconnect.expression.ExpressionOperatorFactory.ExpressionOperatorEnum;
import com.fimtra.util.is;

/**
 * Base-class for {@link IExpression} instances
 * 
 * @author Ramon Servadei
 */
public abstract class AbstractExpression implements IExpression
{
    static final String OPERANDS_START = "(";
    static final String OPERANDS_END = ")";

    public static IExpression fromWireString(String wireForm)
    {
        final int operandsStartIndex = wireForm.indexOf(OPERANDS_START);
        final String operator = wireForm.substring(0, operandsStartIndex);
        final String operands = wireForm.substring(operandsStartIndex + 1, wireForm.length() - 1);
        return ExpressionOperatorFactory.create(ExpressionOperatorEnum.valueOf(operator), operands);
    }

    static IExpression[] getOperands(String operands)
    {
        // this could be "AND(EXPR(<expr2>)EXPR(<expr3>))DataRadarExpression(<expr1>)OR(...)"
        List<IExpression> expressions = new ArrayList<IExpression>();
        int ptr = 0;
        int operandsStartIndex;
        String operator;
        int endOfOperandsIndex;
        String operandsForOperator;
        if (operands.length() > 0)
        {
            do
            {
                operandsStartIndex = operands.indexOf(OPERANDS_START, ptr);
                operator = operands.substring(ptr, operandsStartIndex);
                endOfOperandsIndex = findOperandsEnd(operands, operandsStartIndex);
                operandsForOperator = operands.substring(operandsStartIndex + 1, endOfOperandsIndex);
                expressions.add(ExpressionOperatorFactory.create(ExpressionOperatorEnum.valueOf(operator),
                    operandsForOperator));
                ptr = endOfOperandsIndex + 1;
            }
            while (ptr < operands.length());
        }
        return expressions.toArray(new IExpression[expressions.size()]);
    }

    private static int findOperandsEnd(String string, int openBraceStart)
    {
        final char[] charArray = string.toCharArray();
        int count = 0;
        for (int i = openBraceStart; i < charArray.length; i++)
        {
            switch(charArray[i])
            {
                case '(':
                    count++;
                    break;
                case ')':
                    count--;
                    if (count == 0)
                    {
                        return i;
                    }
            }
        }
        return -1;
    }

    private final ExpressionOperatorEnum operator;
    final IExpression[] operands;

    AbstractExpression(ExpressionOperatorEnum operator, IExpression[] operands)
    {
        super();
        this.operator = operator;
        this.operands = operands;
    }

    @Override
    public final ExpressionOperatorEnum getOperatorEnum()
    {
        return this.operator;
    }

    @Override
    public final String toString()
    {
        return toWireString();
    }

    @Override
    public final IExpression[] getOperands()
    {
        return this.operands;
    }

    @Override
    public final String toWireString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(getOperatorEnum()).append(OPERANDS_START);
        final IExpression[] operands = getOperands();
        for (int i = 0; i < operands.length; i++)
        {
            IExpression expression = operands[i];
            sb.append(expression.toWireString());
        }
        sb.append(OPERANDS_END);
        return sb.toString();
    }

    @Override
    public final int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.operands);
        result = prime * result + ((this.operator == null) ? 0 : this.operator.hashCode());
        return result;
    }

    @Override
    public final boolean equals(Object obj)
    {
        if (is.same(this, obj))
        {
            return true;
        }
        if (is.differentClass(this, obj))
        {
            return false;
        }
        final AbstractExpression other = (AbstractExpression) obj;
        return is.eq(this.operator, other.operator) && is.eq(this.operands, other.operands);
    }
}
