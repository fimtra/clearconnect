/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform;

import com.fimtra.platform.expression.IExpression;

/**
 * Encapsulates searching for data with specific signatures on the platform.
 * 
 * @author Ramon Servadei
 */
public interface IDataRadar
{
    IExpression getDataRadarSignatureExpression();
}
