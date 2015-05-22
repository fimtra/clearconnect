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
 * Always TRUE expression
 * 
 * @author Ramon Servadei
 */
public final class TrueExpression extends AbstractExpression
{
    public TrueExpression(IExpression... operands)
    {
        super(ExpressionOperatorEnum.TRUE, operands);
    }

    @Override
    public boolean evaluate()
    {
        return true;
    }
}
