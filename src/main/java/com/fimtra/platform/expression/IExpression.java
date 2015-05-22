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
 * An expression has an operator and operands and evaluates to either <code>true</code> or
 * <code>false</code>
 * 
 * @author Ramon Servadei
 */
public interface IExpression
{
    /**
     * @return <code>true</code> if this expression evaluates to true
     */
    boolean evaluate();

    /**
     * @return the operator for this expression
     */
    ExpressionOperatorEnum getOperatorEnum();

    /**
     * @return the operands for this expression
     */
    IExpression[] getOperands();

    /**
     * @return the wire-form string for the expression
     * @see ExpressionOperatorFactory
     */
    String toWireString();
}
