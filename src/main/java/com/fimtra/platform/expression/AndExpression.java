/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.expression;

import com.fimtra.platform.expression.ExpressionOperatorFactory.ExpressionOperatorEnum;

/**
 * Performs AND logic on the evaluation of the operands.
 * 
 * @author Ramon Servadei
 */
public final class AndExpression extends AbstractExpression
{
    public AndExpression(IExpression... operands)
    {
        super(ExpressionOperatorEnum.AND, operands);
    }

    @Override
    public boolean evaluate()
    {
        boolean result = true;
        for (int i = 0; i < this.operands.length; i++)
        {
            result &= this.operands[i].evaluate();
            // short-circuit for AND logic
            if (!result)
            {
                return result;
            }
        }
        return result;
    }
}
