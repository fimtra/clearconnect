/*
 * Copyright (c) 2015 Ramon Servadei, Fimtra
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

import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import com.fimtra.util.UtilProperties.Values;

/**
 * Utility methods for working with collections
 * 
 * @author Ramon Servadei
 */
public abstract class CollectionUtils
{
    private CollectionUtils()
    {
    }

    /**
     * @return a Deque implementation
     * @see Values#USE_LOW_GC_LINKEDLIST
     */
    public static final <T> Deque<T> newDeque()
    {
        return UtilProperties.Values.USE_LOW_GC_LINKEDLIST ? new LowGcLinkedList<T>() : new LinkedList<T>();
    }

    /**
     * @return an unmodifiable Set view of the comma separated items (each item is trimmed before
     *         adding)
     * @deprecated Use {@link #newSetFromString(String,String)} instead
     */
    public static final Set<String> newSetFromString(String commaSeparatedList)
    {
        return newSetFromString(commaSeparatedList, ",");
    }

    /**
     * @param tokenSeparator
     *            the token that separates the items
     * @return an unmodifiable Set view of the token separated items (each item is trimmed before
     *         adding)
     */
    public static final Set<String> newSetFromString(String tokenSeparatedList, String tokenSeparator)
    {
        if (tokenSeparatedList == null)
        {
            return Collections.emptySet();
        }
        final String[] split = tokenSeparatedList.split(tokenSeparator);
        final Set<String> set = new HashSet<String>(split.length);
        for (int i = 0; i < split.length; i++)
        {
            set.add(split[i].trim());
        }
        return Collections.unmodifiableSet(set);
    }
}
