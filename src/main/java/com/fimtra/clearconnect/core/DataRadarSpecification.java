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
package com.fimtra.clearconnect.core;

import com.fimtra.clearconnect.IDataRadar;
import com.fimtra.clearconnect.expression.AbstractExpression;
import com.fimtra.clearconnect.expression.DataSignatureExpression;
import com.fimtra.clearconnect.expression.IExpression;
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
