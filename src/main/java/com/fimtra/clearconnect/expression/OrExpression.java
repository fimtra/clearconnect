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

import com.fimtra.clearconnect.expression.ExpressionOperatorFactory.ExpressionOperatorEnum;

/**
 * Performs OR logic on the evaluation of the operands.
 * 
 * @author Ramon Servadei
 */
public final class OrExpression extends AbstractExpression
{
    public OrExpression(IExpression... operands)
    {
        super(ExpressionOperatorEnum.OR, operands);
    }

    @Override
    public boolean evaluate()
    {
        boolean result = true;
        for (int i = 0; i < this.operands.length; i++)
        {
            result |= this.operands[i].evaluate();
        }
        return result;
    }
}
