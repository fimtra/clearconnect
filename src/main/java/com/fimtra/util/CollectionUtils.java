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

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fimtra.util.UtilProperties.Values;

/**
 * Utility methods for working with collections
 * 
 * @author Ramon Servadei
 */
public abstract class CollectionUtils
{
    private static final float LOAD_FACTOR = 1 / .75f;

    /**
     * An unmodifiable set of Map.Entry objects that are themselves unmodifiable (
     * {@link Entry#setValue(Object)} will throw {@link UnsupportedOperationException})
     * 
     * @author Ramon Servadei
     */
    static final class UnmodifiableEntrySet<K, V> extends AbstractSet<Map.Entry<K, V>>
    {
        private final static class UnmodifiableEntry<K, V> implements Entry<K, V>
        {
            final Entry<K, V> backingEntry;

            UnmodifiableEntry(java.util.Map.Entry<K, V> backingEntry)
            {
                super();
                this.backingEntry = backingEntry;
            }

            @Override
            public K getKey()
            {
                return this.backingEntry.getKey();
            }

            @Override
            public V getValue()
            {
                return this.backingEntry.getValue();
            }

            @Override
            public V setValue(V value)
            {
                throw new UnsupportedOperationException();
            }
        }

        final Set<Entry<K, V>> entrySet;

        UnmodifiableEntrySet(Set<Entry<K, V>> entrySet)
        {
            this.entrySet = Collections.unmodifiableSet(entrySet);
        }

        @Override
        public Iterator<java.util.Map.Entry<K, V>> iterator()
        {
            return new Iterator<Map.Entry<K, V>>()
            {
                final Iterator<Map.Entry<K, V>> backingIterator = UnmodifiableEntrySet.this.entrySet.iterator();

                @Override
                public boolean hasNext()
                {
                    return this.backingIterator.hasNext();
                }

                @Override
                public java.util.Map.Entry<K, V> next()
                {
                    return new UnmodifiableEntry<K, V>(this.backingIterator.next());
                }

                @Override
                public void remove()
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public int size()
        {
            return this.entrySet.size();
        }
    }

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

    /**
     * @return an unmodifiable {@link Set} with unmodifiable {@link Entry} objects for the passed in
     *         entrySet
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <K, V> Set<java.util.Map.Entry<K, V>> unmodifiableEntrySet(
        final Set<java.util.Map.Entry<K, V>> entrySet)
    {
        return new UnmodifiableEntrySet(entrySet);
    }

    /**
     * Creates a new {@link HashSet} wrapping the collection
     * 
     * @param c
     *            the collection to wrap
     * @return a {@link HashSet}
     */
    public static <T> Set<T> newHashSet(Collection<T> c)
    {
        final Set<T> s = new HashSet<T>(Math.max((int) (c.size() * LOAD_FACTOR) + 1, 2));
        s.addAll(c);
        return s;
    }

    /**
     * Creates a synchronized map initialised with the given size
     * 
     * @param size
     *            the size
     * @return a synchronized {@link HashMap}
     */
    public static <K, V> Map<K, V> newMap(int size)
    {
        return Collections.synchronizedMap(new HashMap<K, V>(size));
    }

    /**
     * Creates a synchronized map initialised with the given map
     * 
     * @param data
     *            the map data to use for initialisation
     * @return a synchronized {@link HashMap}
     */
    public static <K, V> Map<K, V> newMap(Map<K, V> data)
    {
        return Collections.synchronizedMap(new HashMap<K, V>(data));
    }
}
