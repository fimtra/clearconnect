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
final class CharSubArray
{
    final char[] ref;
    final int start;
    final int end;
    final int hashcode;

    CharSubArray(char[] carray, int offset, int len)
    {
        this.ref = carray;
        this.start = offset;
        this.end = offset + len;

        // compute hashcode upfront based on revese order of chars
        int h = 0;
        if (len > 6)
        {
            int i = end;
            // use LAST 6 chars
            h = this.ref[--i];
            h = 31 * h + this.ref[--i];
            h = 31 * h + this.ref[--i];
            h = 31 * h + this.ref[--i];
            h = 31 * h + this.ref[--i];
            h = 31 * h + this.ref[--i];
        }
        else
        {
            for (int i = end - 1; i >= offset; i--)
            {
                h = 31 * h + this.ref[i];
            }
        }

        this.hashcode = h;
    }

    @Override
    public int hashCode()
    {
        return this.hashcode;
    }

    @Override
    public boolean equals(Object obj)
    {
        // this is only called in the context of a Map key lookup
        // and CharSubArray is only used in a CharSubArrayKeyedPool
        // a map key lookup will check for hashcode first, then object reference then call equals
        // so we skip null, hashcode and object reference checks
        // and we assume the object is a CharSubArray
        // (also obj will never be null as CharSubArrayKeyedPool only uses CharSubArray instances)
        final CharSubArray other = (CharSubArray) obj;

        if ((this.end - this.start) != other.end)
        {
            return false;
        }

        int j = 0;
        // note: obj is ALWAYS a key in a map constructed with a clean array so obj.start is always 0
        // see CharSubArrayKeyedPool.get
        for (int i = this.start; i < this.end; i++)
        {
            if (this.ref[i] != other.ref[j++])
            {
                return false;
            }
        }
        return true;
    }

}