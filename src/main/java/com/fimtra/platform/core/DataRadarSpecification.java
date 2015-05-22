/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.core;

import com.fimtra.platform.IDataRadar;
import com.fimtra.platform.expression.AbstractExpression;
import com.fimtra.platform.expression.DataSignatureExpression;
import com.fimtra.platform.expression.IExpression;
import com.fimtra.util.is;

/**
 * This is the specification for a data-radar. The specification is composed of
 * {@link DataSignatureExpression} objects. The radar specification expresses the logical
 * relationship between the data-signature expressions.
 * <p>
 * Equal by the internal {@link IExpression} only.
 * 
 * @see IDataRadar
 * @author Ramon Servadei
 */
public final class DataRadarSpecification
{
    private static final String DELIM = ":";
    private static final String DELIM_TOKEN = "{delim}";

    public static DataRadarSpecification fromWireString(String wireForm)
    {
        final String[] tokens = wireForm.split(DELIM);
        return new DataRadarSpecification(unescape(tokens[0]), AbstractExpression.fromWireString(unescape(tokens[1])));
    }

    static String toWireString(DataRadarSpecification spec)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(escape(spec.name)).append(DELIM).append(escape(spec.dataRadarSignatureExpression.toWireString()));
        return sb.toString();
    }

    private static String escape(String s)
    {
        return s.replace(DELIM, DELIM_TOKEN);
    }

    private static String unescape(String s)
    {
        return s.replace(DELIM_TOKEN, DELIM);
    }

    private final String name;
    final IExpression dataRadarSignatureExpression;

    public DataRadarSpecification(String name, IExpression dataRadarSignatureExpression)
    {
        super();
        this.name = name;
        this.dataRadarSignatureExpression = dataRadarSignatureExpression;
    }

    /**
     * @return the name of the data radar specification
     */
    public final String getName()
    {
        return this.name;
    }

    /**
     * @return the wire-form string for this instance
     */
    public String toWireString()
    {
        return DataRadarSpecification.toWireString(this);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " [" + toWireString() + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result =
            prime * result
                + ((this.dataRadarSignatureExpression == null) ? 0 : this.dataRadarSignatureExpression.hashCode());
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
        final DataRadarSpecification other = (DataRadarSpecification) obj;
        return is.eq(this.dataRadarSignatureExpression, other.dataRadarSignatureExpression);
    }

    public IExpression getDataRadarSignatureExpression()
    {
        return this.dataRadarSignatureExpression;
    }
}
