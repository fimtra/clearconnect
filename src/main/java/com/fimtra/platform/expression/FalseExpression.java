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
 * Always FALSE expression
 * 
 * @author Ramon Servadei
 */
public final class FalseExpression extends AbstractExpression
{
    public FalseExpression(IExpression... operands)
    {
        super(ExpressionOperatorEnum.FALSE, operands);
    }

    @Override
    public boolean evaluate()
    {
        return false;
    }
}
