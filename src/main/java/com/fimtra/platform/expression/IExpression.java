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
