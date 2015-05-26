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
package com.fimtra.platform.expression;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.AbstractValue;
import com.fimtra.datafission.field.ValueComparatorEnum;
import com.fimtra.platform.expression.ExpressionOperatorFactory.ExpressionOperatorEnum;
import com.fimtra.util.is;

/**
 * The expression for a data signature that matches against a single field in an {@link IRecord},
 * e.g. "field1 less-than 10" would be a data signature specification.
 * 
 * @author Ramon Servadei
 */
public final class DataSignatureExpression implements IExpression
{
    private static final String OPEN_BRACE = AbstractExpression.OPERANDS_START;
    private static final String OPEN_BRACE_TOKEN = "{OPEN_BRACE}";
    private static final String CLOSE_BRACE = AbstractExpression.OPERANDS_END;
    private static final String CLOSE_BRACE_TOKEN = "{CLOSE_BRACE}";

    private static final String SPACE = " ";
    private static final String SPACE_TOKEN = "{SPACE}";
    private static final IExpression[] EMPTY = new IExpression[0];

    static DataSignatureExpression fromWireString(String wireForm)
    {
        final String[] tokens = wireForm.split(SPACE);
        if (tokens.length != 3)
        {
            throw new IllegalStateException("There should be 3 tokens for a specification, got: '" + wireForm
                + "' tokens=" + tokens.length);
        }
        return new DataSignatureExpression(unescape(tokens[0]), ValueComparatorEnum.valueOf(unescape(tokens[1])),
            AbstractValue.constructFromStringValue(unescape(tokens[2])));
    }

    static String toWireString(DataSignatureExpression expr)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(ExpressionOperatorEnum.DATA_SIGNATURE).append(AbstractExpression.OPERANDS_START).append(
            escape(expr.fieldName)).append(SPACE).append(escape(expr.operator.toString())).append(SPACE).append(
            escape(expr.value.toString())).append(AbstractExpression.OPERANDS_END);
        return sb.toString();
    }

    private static String escape(String s)
    {
        return s.replace(SPACE, SPACE_TOKEN).replace(OPEN_BRACE, OPEN_BRACE_TOKEN).replace(CLOSE_BRACE,
            CLOSE_BRACE_TOKEN);
    }

    private static String unescape(String s)
    {
        return s.replace(SPACE_TOKEN, SPACE).replace(OPEN_BRACE_TOKEN, OPEN_BRACE).replace(CLOSE_BRACE_TOKEN,
            CLOSE_BRACE);
    }

    private final String fieldName;
    private final ValueComparatorEnum operator;
    private final IValue value;
    private boolean result;

    public DataSignatureExpression(String fieldName, ValueComparatorEnum operator, IValue value)
    {
        super();
        this.fieldName = fieldName;
        this.operator = operator;
        this.value = value;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.fieldName == null) ? 0 : this.fieldName.hashCode());
        result = prime * result + ((this.operator == null) ? 0 : this.operator.hashCode());
        result = prime * result + ((this.value == null) ? 0 : this.value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (is.same(this, obj))
        {
            return true;
        }
        if (is.differentClass(this, obj))
        {
            return false;
        }
        final DataSignatureExpression other = (DataSignatureExpression) obj;
        return is.eq(this.fieldName, other.fieldName) && is.eq(this.operator, other.operator)
            && is.eq(this.value, other.value);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " [" + this.fieldName + " " + this.operator + " " + this.value + "]";
    }

    @Override
    public ExpressionOperatorEnum getOperatorEnum()
    {
        return ExpressionOperatorEnum.DATA_SIGNATURE;
    }

    @Override
    public boolean evaluate()
    {
        return this.result;
    }

    @Override
    public String toWireString()
    {
        return toWireString(this);
    }

    @Override
    public IExpression[] getOperands()
    {
        return EMPTY;
    }

    public String getFieldName()
    {
        return this.fieldName;
    }

    public ValueComparatorEnum getOperator()
    {
        return this.operator;
    }

    public IValue getValue()
    {
        return this.value;
    }

    public void prepareResult(IValue test)
    {
        this.result = this.operator.evaluate(test, this.value);
    }

    public void reset()
    {
        this.result = false;
    }
}
