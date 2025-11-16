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

import java.util.Arrays;

/**
 * Utilities for working with arrays
 *
 * @author Ramon Servadei
 */
public abstract class ArrayUtils
{
    private ArrayUtils()
    {
    }

    /**
     * Checks if the passed in array contains the object <b>instance</b>. This performs the identity
     * check <code>==</code> to determine the result.
     *
     * @return <code>true</code> if the array contains the instance
     */
    public static <T> boolean containsInstance(T[] arr, T instance)
    {
        for (int i = 0; i < arr.length; i++)
        {
            if (arr[i] == instance)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Increase the array size by one and append the long.
     *
     * @return the expanded array with the appended long
     */
    public static long[] add(long l, long[] arr)
    {
        final long[] expandedArr = Arrays.copyOf(arr, arr.length + 1);
        expandedArr[expandedArr.length - 1] = l;
        return expandedArr;
    }
}
