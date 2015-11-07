/*
 * Copyright (c) 2014 Ramon Servadei 
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
package com.fimtra.util;

import java.io.Serializable;

/**
 * Holds two objects as a single reference. Computes the hashcode on construction.
 * 
 * @author Ramon Servadei
 * @param <T1>
 * @param <T2>
 */
public final class Pair<T1 extends Serializable, T2 extends Serializable> implements Serializable
{
    private static final long serialVersionUID = 1L;

    final T1 first;
    final T2 second;
    final int hashCode;

    public Pair(T1 first, T2 second)
    {
        super();
        this.first = first;
        this.second = second;
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.first == null) ? 0 : this.first.hashCode());
        result = prime * result + ((this.second == null) ? 0 : this.second.hashCode());
        this.hashCode = result;
    }

    @Override
    public int hashCode()
    {
        return this.hashCode;
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
        @SuppressWarnings("rawtypes")
        Pair other = (Pair) obj;
        return is.eq(this.first, other.first) && is.eq(this.second, other.second);
    }

    @Override
    public String toString()
    {
        return "[" + this.first + ", " + this.second + "]";
    }

    public T1 getFirst()
    {
        return this.first;
    }

    public T2 getSecond()
    {
        return this.second;
    }

}