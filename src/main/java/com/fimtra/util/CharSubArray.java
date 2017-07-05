/*
 * Copyright (c) 2017 Ramon Servadei
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

/**
 * Utility to hold the offset and len of a sub-array within a main char[].
 * 
 * @author Ramon Servadei
 */
public final class CharSubArray
{
    final char[] ref;
    final int start;
    final int end;
    int hashcode;

    public CharSubArray(char[] carray, int offset, int len)
    {
        this.ref = carray;
        this.start = offset;
        this.end = offset + len;

        // compute hashcode upfront
        int h = this.hashcode;
        if (this.end > this.start)
        {
            for (int i = this.start; i < this.end; i++)
            {
                h = 31 * h + this.ref[i];
            }
            this.hashcode = h;
        }
    }

    @Override
    public int hashCode()
    {
        return this.hashcode;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }

        final CharSubArray other = (CharSubArray) obj;

        if ((this.end - this.start) != (other.end - other.start))
        {
            return false;
        }

        int j = 0;
        for (int i = this.start; i < this.end; i++)
        {
            if (this.ref[i] != other.ref[other.start + j++])
            {
                return false;
            }
        }
        return true;
    }

}