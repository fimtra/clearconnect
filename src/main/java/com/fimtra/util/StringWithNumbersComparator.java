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

import java.util.Comparator;

/**
 * Handles correct comparison when strings have numbers.
 * <p>
 * E.g. given <tt>XYZ_9, XYZ_10, XYZ_1, XYZ_0</tt> the traditional (lexical) string ordering is:
 * <tt>
 * <ul>
 * <li>XYZ_0
 * <li>XYZ_1
 * <li>XYZ_10
 * <li>XYZ_9
 * </ul>
 * </tt> This comparator sorts as: <tt>
 * <ul>
 * <li>XYZ_0
 * <li>XYZ_1
 * <li>XYZ_9
 * <li>XYZ_10
 * </ul>
 * </tt>
 * 
 * @author Ramon Servadei
 */
public final class StringWithNumbersComparator implements Comparator<String>
{
    @Override
    public int compare(String o1, String o2)
    {
        if (o1 == null)
        {
            if (o2 == null)
            {
                return 0;
            }
            else
            {
                return -1;
            }
        }
        else
        {
            if (o2 == null)
            {
                return 1;
            }
        }

        if (o1.length() == o2.length())
        {
            return o1.compareTo(o2);
        }

        int o1Length = o1.length();
        int o2Length = o2.length();
        int val;
        if (o1Length > o2Length)
        {
            for (int i = 0; i < o2Length; i++)
            {
                val = o1.charAt(i) - o2.charAt(i);
                if (val != 0)
                {
                    // check for o2=XYZ_9 vs o1=XYZ_10
                    if (Character.isDigit(o1.charAt(i + 1)) && Character.isDigit(o1.charAt(i))
                        && Character.isDigit(o2.charAt(i)))
                    {
                        return 1;
                    }
                    return val;
                }
                else
                {
                    // check o1=10XYZ vs o2=1XYZ
                    if (Character.isDigit(o1.charAt(i + 1)))
                    {
                        return 1;
                    }
                }
            }
            return 1;
        }
        else
        {
            for (int i = 0; i < o1Length; i++)
            {
                val = o1.charAt(i) - o2.charAt(i);
                if (val != 0)
                {
                    // check for o1=XYZ_9 vs o2=XYZ_10
                    if (Character.isDigit(o2.charAt(i + 1)) && Character.isDigit(o2.charAt(i))
                        && Character.isDigit(o1.charAt(i)))
                    {
                        return -1;
                    }
                    return val;
                }
                else
                {
                    // check o2=10XYZ vs o1=1XYZ
                    if (Character.isDigit(o2.charAt(i + 1)))
                    {
                        return -1;
                    }
                }
            }
            return -1;
        }
    }
}