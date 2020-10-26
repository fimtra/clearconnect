/*
 * Copyright (c) 2013 Ramon Servadei
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
package com.fimtra.datafission.core;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.DataFissionProperties.Values;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.Context.NoopAtomicChangeManager;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.util.CollectionUtils;
import com.fimtra.util.ObjectPool;
import com.fimtra.util.is;

/**
 * The standard implementation. This does not allow <code>null</code> keys. Records are equal by
 * value of their internal data map entries.
 *
 * @author Ramon Servadei
 * @see IRecord The IRecord interface for further behaviour documentation
 */
final class Record implements IRecord, Cloneable
{
    static String toString(String contextName, String recordName, long sequence, Map<String, IValue> data,
            Map<String, Map<String, IValue>> subMaps)
    {
        String subMapString;
        if (subMaps.size() > Values.MAX_MAP_FIELDS_TO_PRINT)
        {
            subMapString = "{Too big to print, size=" + subMaps.size() + "}";
        }
        else
        {
            final StringBuilder sb = new StringBuilder(subMaps.size() * 100);
            boolean first = true;
            sb.append("{");
            for (Entry<String, Map<String, IValue>> entry : subMaps.entrySet())
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    sb.append(", ");
                }
                sb.append(entry.getKey()).append("=").append(entry.getValue());
            }
            sb.append("}");
            subMapString = sb.toString();
        }

        final String dataString = ContextUtils.mapToString(data);
        final StringBuilder sb = new StringBuilder(
                dataString.length() + subMapString.length() + contextName.length() + recordName.length()
                        + 30);
        sb.append(contextName).append("|").append(recordName).append("|").append(sequence).append("|").append(
                dataString).append("|subMaps").append(subMapString);
        return sb.toString();
    }

    static Record snapshot(IRecord template)
    {
        if (template instanceof Record)
        {
            return ((Record) template).clone();
        }
        else
        {
            return ((ImmutableRecord) template).backingRecord.clone();
        }
    }

    private static final Map<String, Map<String, IValue>> EMPTY_SUBMAP = CollectionUtils.newMap(2);
    /**
     * A pool for the keys. Keys across records stand a VERY good chance of being repeated many
     * times so this is a valuable memory optimisation.
     */
    static final ObjectPool<String> keysPool =
            new ObjectPool<>("record-keys", DataFissionProperties.Values.KEYS_POOL_MAX);

    final AtomicLong sequence;
    final String name;
    final IAtomicChangeManager context;
    Map<String, IValue> data;
    Map<String, Map<String, IValue>> subMaps;

    Record(String name, Map<String, IValue> data, IAtomicChangeManager context)
    {
        super();
        this.name = keysPool.intern(name);
        this.data = CollectionUtils.newMap(data);
        this.subMaps = EMPTY_SUBMAP;
        this.context = context;
        this.sequence = new AtomicLong(0);
    }

    /**
     * Clone constructor for providing pre-sized subMaps to prevent re-hashing
     */
    Record(String name, Map<String, IValue> data, IAtomicChangeManager context,
            Map<String, Map<String, IValue>> subMaps)
    {
        super();
        this.name = keysPool.intern(name);
        this.data = CollectionUtils.newMap(data);
        this.subMaps = subMaps;
        this.context = context;
        this.sequence = new AtomicLong(0);
    }

    @Override
    public IRecord getImmutableInstance()
    {
        return new ImmutableRecord(this);
    }

    @Override
    public void clear()
    {
        synchronized (this)
        {
            for (Entry<String, IValue> stringIValueEntry : new HashMap<>(this.data).entrySet())
            {
                remove(stringIValueEntry.getKey());
            }

            for (Entry<String, Map<String, IValue>> stringMapEntry : this.subMaps.entrySet())
            {
                stringMapEntry.getValue().clear();
            }
            this.data = CollectionUtils.newMap(4);
            this.subMaps = EMPTY_SUBMAP;
        }
    }

    @Override
    public boolean containsKey(Object key)
    {
        synchronized (this)
        {
            return this.data.containsKey(key);
        }
    }

    @Override
    public boolean containsValue(Object value)
    {
        synchronized (this)
        {
            return this.data.containsValue(value);
        }
    }

    @Override
    public Set<Entry<String, IValue>> entrySet()
    {
        final int size = this.data.size();
        final Set<Entry<String, IValue>> entrySet = new HashSet<Entry<String, IValue>>(size)
        {
            @Override
            public Iterator<Entry<String, IValue>> iterator()
            {
                return new EntrySetIterator(super.iterator(), Record.this);
            }
        };

        synchronized (this)
        {
            entrySet.addAll(this.data.entrySet());
        }

        return entrySet;
    }

    @Override
    public boolean equals(Object o)
    {
        if (is.same(o, this))
        {
            return true;
        }
        if (!(o instanceof IRecord))
        {
            return false;
        }
        final Record other;
        if (o instanceof ImmutableRecord)
        {
            other = ((ImmutableRecord) o).backingRecord;
        }
        else
        {
            other = (Record) o;
        }
        synchronized (this)
        {
            synchronized (other)
            {
                return is.eq(this.name, other.name) && is.eq(this.context.getName(), other.context.getName())
                        && is.eq(this.data, other.data) && is.eq(this.subMaps, other.subMaps);
            }
        }
    }

    @Override
    public IValue get(Object key)
    {
        synchronized (this)
        {
            return this.data.get(key);
        }
    }

    @Override
    public int hashCode()
    {
        return this.name.hashCode();
    }

    @Override
    public boolean isEmpty()
    {
        synchronized (this)
        {
            return this.data.isEmpty();
        }
    }

    @Override
    public Set<String> keySet()
    {
        final Set<String> keySet = new HashSet<String>(this.data.size())
        {
            @Override
            public Iterator<String> iterator()
            {
                return new KeySetIterator(super.iterator(), Record.this);
            }
        };

        synchronized (this)
        {
            keySet.addAll(this.data.keySet());
        }

        return keySet;
    }

    @Override
    public IValue put(String key, IValue value)
    {
        if (key == null)
        {
            throw new NullPointerException("null keys are not allowed");
        }
        synchronized (this)
        {
            final String internKey = keysPool.intern(key);
            final IValue previous;
            if (value != null)
            {
                previous = this.data.put(internKey, value);
                // if there is no change, we perform no update
                if (this.context != null)
                {
                    if (previous == null || !previous.equals(value))
                    {
                        this.context.addEntryUpdatedToAtomicChange(this, internKey, value, previous);
                    }
                }
            }
            else
            {
                // a null is treated as if it removes the key
                previous = this.data.remove(internKey);
                if (this.context != null)
                {
                    if (previous != null)
                    {
                        this.context.addEntryRemovedToAtomicChange(this, internKey, previous);
                    }
                }
            }
            return previous;
        }
    }

    /**
     * Convenience put method for a long value
     *
     * @see #put(String, IValue)
     */
    @Override
    public IValue put(String key, long value)
    {
        return put(key, LongValue.valueOf(value));
    }

    /**
     * Convenience put method for a double value
     *
     * @see #put(String, IValue)
     */
    @Override
    public IValue put(String key, double value)
    {
        return put(key, DoubleValue.valueOf(value));
    }

    /**
     * Convenience put method for a String value
     *
     * @see #put(String, IValue)
     */
    @Override
    public IValue put(String key, String value)
    {
        return put(key, TextValue.valueOf(value));
    }

    @Override
    public void putAll(Map<? extends String, ? extends IValue> t)
    {
        synchronized (this)
        {
            String key;
            IValue value;
            String internKey;

            if (this.context instanceof NoopAtomicChangeManager)
            {
                for (Entry<? extends String, ? extends IValue> entry : t.entrySet())
                {
                    key = entry.getKey();
                    value = entry.getValue();
                    internKey = keysPool.intern(key);

                    if (value != null)
                    {
                        this.data.put(internKey, value);
                    }
                    else
                    {
                        // a null is treated as if it removes the key
                        this.data.remove(internKey);
                    }
                }
            }
            else
            {
                final ThreadLocalBulkChanges changes = ThreadLocalBulkChanges.get().initialise(t.size());
                IValue previous;
                int putPtr = 0;
                int removePtr = 0;
                for (Entry<? extends String, ? extends IValue> entry : t.entrySet())
                {
                    key = entry.getKey();
                    value = entry.getValue();
                    internKey = keysPool.intern(key);

                    if (value != null)
                    {
                        previous = this.data.put(internKey, value);
                        // if there is no change, we perform no update
                        if (this.context != null)
                        {
                            if (previous == null || !previous.equals(value))
                            {
                                changes.putKeys[putPtr] = internKey;
                                changes.putValues[putPtr][0] = value;
                                changes.putValues[putPtr][1] = previous;
                                putPtr++;
                            }
                        }
                    }
                    else
                    {
                        // a null is treated as if it removes the key
                        previous = this.data.remove(internKey);
                        if (this.context != null)
                        {
                            if (previous != null)
                            {
                                changes.removedKeys[removePtr] = internKey;
                                changes.removedValues[removePtr] = previous;
                                removePtr++;
                            }
                        }
                    }
                }
                changes.putSize = putPtr;
                changes.removedSize = removePtr;
                this.context.addBulkChangesToAtomicChange(this, changes);
            }
        }
    }

    @Override
    public IValue remove(Object key)
    {
        synchronized (this)
        {
            if (key instanceof String)
            {
                if (this.context instanceof NoopAtomicChangeManager)
                {
                    this.data.remove(key);
                }
                else
                {
                    final IValue value;
                    if ((value = this.data.remove(key)) != null)
                    {
                        addEntryRemovedToAtomicChange((String) key, value);
                        return value;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public int size()
    {
        synchronized (this)
        {
            return this.data.size();
        }
    }

    @Override
    public Collection<IValue> values()
    {
        final int size = this.data.size();
        final List<String> keys = new ArrayList<>(size);
        final List<IValue> values = new ArrayList<IValue>(size)
        {
            @Override
            public Iterator<IValue> iterator()
            {
                return new ValuesIterator(super.iterator(), keys, Record.this);
            }
        };

        synchronized (this)
        {
            this.data.forEach((key, value) -> {
                keys.add(key);
                values.add(value);
            });
        }

        return values;
    }

    @Override
    public String toString()
    {
        synchronized (this)
        {
            return toString(this.context.getName(), this.name, this.sequence.longValue(), this.data,
                    this.subMaps);
        }
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public String getContextName()
    {
        if (this.context != null)
        {
            return this.context.getName();
        }
        return "";
    }

    @Override
    public Map<String, IValue> getOrCreateSubMap(String key)
    {
        synchronized (this)
        {
            Map<String, IValue> submap = this.subMaps.get(key);
            if (submap == null)
            {
                if (this.subMaps == EMPTY_SUBMAP)
                {
                    this.subMaps = CollectionUtils.newMap(4);
                }
                final String internKey = keysPool.intern(key);
                submap = new SubMap(this, internKey);
                this.subMaps.put(internKey, submap);
            }
            return submap;
        }
    }

    @Override
    public Map<String, IValue> removeSubMap(String key)
    {
        synchronized (this)
        {
            final Map<String, IValue> subMap = this.subMaps.remove(key);
            if (subMap == null)
            {
                return ContextUtils.EMPTY_MAP;
            }
            final Map<String, IValue> previous = new HashMap<>(subMap);
            // this call adds the remove to the atomic change
            subMap.clear();
            return previous;
        }
    }

    @Override
    public Set<String> getSubMapKeys()
    {
        synchronized (this)
        {
            return CollectionUtils.newHashSet(this.subMaps.keySet());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IValue> T get(String key)
    {
        synchronized (this)
        {
            return (T) this.data.get(key);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void resolveFromStream(Reader reader) throws IOException
    {
        final HashMap<String, IValue> flattenedMap = new HashMap<>();
        ContextUtils.resolveRecordMapFromStream(reader, flattenedMap);
        final Map<?, ?>[] demergeMaps = ContextUtils.demergeMaps(flattenedMap);
        synchronized (this)
        {
            // trigger an atomic change by adding to self
            putAll((Map<String, IValue>) demergeMaps[0]);
            Map<String, Map<String, IValue>> subMaps = (Map<String, Map<String, IValue>>) demergeMaps[1];

            String key;
            Map<String, IValue> value;
            for (Entry<String, Map<String, IValue>> entry : subMaps.entrySet())
            {
                key = entry.getKey();
                value = entry.getValue();
                getOrCreateSubMap(key).putAll(value);
            }
        }
    }

    @Override
    public void serializeToStream(Writer writer) throws IOException
    {
        synchronized (this)
        {
            ContextUtils.serializeRecordMapToStream(writer, asFlattenedMap());
        }
    }

    @Override
    public Object getWriteLock()
    {
        return this;
    }

    /**
     * Create a deep-copy of the record (a 'snapshot')
     */
    @Override
    protected Record clone()
    {
        // this is a deep copy of the record internal maps
        synchronized (this)
        {
            final Record cloneRecord;
            if (this.subMaps.size() == 0)
            {
                cloneRecord = new Record(this.name, CollectionUtils.newMap(this.data), this.context);
            }
            else
            {
                final Map<String, Map<String, IValue>> cloneSubMaps =
                        CollectionUtils.newMap(this.subMaps.size());
                cloneRecord =
                        new Record(this.name, CollectionUtils.newMap(this.data), this.context, cloneSubMaps);
                Map.Entry<String, Map<String, IValue>> entry;
                for (Entry<String, Map<String, IValue>> stringMapEntry : this.subMaps.entrySet())
                {
                    entry = stringMapEntry;
                    cloneSubMaps.put(entry.getKey(), ((SubMap) entry.getValue()).clone(cloneRecord));
                }
            }
            cloneRecord.sequence.set(this.sequence.get());
            return cloneRecord;
        }
    }

    @Override
    public Map<String, IValue> asFlattenedMap()
    {
        synchronized (this)
        {
            return ContextUtils.mergeMaps(this.data, this.subMaps);
        }
    }

    @Override
    public long getSequence()
    {
        return this.sequence.longValue();
    }

    void addEntryRemovedToAtomicChange(String key, final IValue value)
    {
        if (this.context != null)
        {
            this.context.addEntryRemovedToAtomicChange(this, key, value);
        }
    }

    void addSubMapEntryUpdatedToAtomicChange(String subMapKey, String key, final IValue current,
            IValue previous)
    {
        if (this.context != null)
        {
            this.context.addSubMapEntryUpdatedToAtomicChange(this, subMapKey, key, current, previous);
        }
    }

    void addSubMapEntryRemovedToAtomicChange(String subMapKey, String key, final IValue value)
    {
        if (this.context != null)
        {
            this.context.addSubMapEntryRemovedToAtomicChange(this, subMapKey, key, value);
        }
    }

    void setSequence(long sequence)
    {
        this.sequence.set(sequence);
    }

    private static abstract class AbstractIterator<T> implements Iterator<T>
    {
        private final Iterator<T> iterator;
        private final Map<String, IValue> target;

        private AbstractIterator(Iterator<T> iterator, Map<String, IValue> target)
        {
            this.iterator = iterator;
            this.target = target;
        }

        @Override
        public final boolean hasNext()
        {
            return this.iterator.hasNext();
        }

        @Override
        public final T next()
        {
            final T next = this.iterator.next();
            doNext(next);
            return next;
        }

        @Override
        public final void remove()
        {
            this.target.remove(getKey());
            this.iterator.remove();
        }

        abstract void doNext(T next);

        abstract String getKey();
    }

    static final class KeySetIterator extends AbstractIterator<String>
    {
        private String current;

        KeySetIterator(Iterator<String> iterator, Map<String, IValue> target)
        {
            super(iterator, target);
        }

        @Override
        void doNext(String next)
        {
            this.current = next;
        }

        @Override
        String getKey()
        {
            return this.current;
        }
    }

    static final class ValuesIterator extends AbstractIterator<IValue>
    {
        private final List<String> keys;
        private int i = -1;

        ValuesIterator(Iterator<IValue> iterator, List<String> keys, Map<String, IValue> target)
        {
            super(iterator, target);
            this.keys = keys;
        }

        @Override
        void doNext(IValue next)
        {
            this.i++;
        }

        @Override
        String getKey()
        {
            return this.keys.get(this.i);
        }
    }

    static final class EntrySetIterator extends AbstractIterator<Entry<String, IValue>>
    {
        private Entry<String, IValue> current;

        EntrySetIterator(Iterator<Entry<String, IValue>> iterator, Map<String, IValue> target)
        {
            super(iterator, target);
        }

        @Override
        void doNext(Entry<String, IValue> next)
        {
            this.current = next;
        }

        @Override
        String getKey()
        {
            return this.current.getKey();
        }
    }
}

/**
 * A sub-map view onto the record's internal map. The sub-map key-values are held in the record's
 * map with each key prefixed with a string.
 *
 * @author Ramon Servadei
 */
final class SubMap implements Map<String, IValue>
{
    private static final String SUB_MAP_KEY_PREFIX = "{";
    private static final String SUB_MAP_KEY_SUFFIX = "}.";
    static final char CHAR_SUB_MAP_KEY_PREFIX = SUB_MAP_KEY_PREFIX.toCharArray()[0];

    /**
     * Format of each key in a submap is:
     *
     * <pre>
     * {subMapKey}.keyOfEntryInSubMap
     * </pre>
     * <p>
     * this returns the <code>subMapKey</code> argument wrapped as <tt>{subMapKey}.</tt>
     */
    static String encodeSubMapKey(String subMapKey)
    {
        return SUB_MAP_KEY_PREFIX + subMapKey + SUB_MAP_KEY_SUFFIX;
    }

    /**
     * Format of each key in a submap is:
     *
     * <pre>
     * {subMapKey}.keyOfEntryInSubMap
     * </pre>
     * <p>
     * This method returns an array holding the <code>subMapKey</code> and
     * <code>keyOfEntryInSubMap</code> parts.
     *
     * @param recordKey one of the keys in a record
     * @return <code>new String[]{subMapKey, keyOfEntryInSubMap}</code> OR <code>null</code> if the
     * record key is not for a sub-map
     */
    static String[] decodeSubMapKeys(String recordKey)
    {
        if (recordKey.charAt(0) == CHAR_SUB_MAP_KEY_PREFIX)
        {
            final int indexOf = recordKey.indexOf(SUB_MAP_KEY_SUFFIX);
            if (indexOf == -1)
            {
                return null;
            }
            return new String[] { recordKey.substring(1, indexOf),
                    recordKey.substring(indexOf + SUB_MAP_KEY_SUFFIX.length()) };
        }
        return null;
    }

    final Record record;
    final String subMapKey;
    final Map<String, IValue> subMap;

    SubMap(Record record, String subMapKey)
    {
        this.subMapKey = subMapKey;
        this.record = record;
        this.subMap = CollectionUtils.newMap(4);
    }

    private SubMap(Record record, String subMapKey, Map<String, IValue> subMap)
    {
        this.record = record;
        this.subMapKey = subMapKey;
        this.subMap = subMap;
    }

    SubMap clone(Record cloneRecord)
    {
        synchronized (this.record)
        {
            return new SubMap(cloneRecord, this.subMapKey, CollectionUtils.newMap(this.subMap));
        }
    }

    @Override
    public String toString()
    {
        synchronized (this.record)
        {
            return ContextUtils.mapToString(this.subMap);
        }
    }

    @Override
    public int size()
    {
        synchronized (this.record)
        {
            return this.subMap.size();
        }
    }

    @Override
    public boolean isEmpty()
    {
        synchronized (this.record)
        {
            return this.subMap.isEmpty();
        }
    }

    @Override
    public boolean containsKey(Object key)
    {
        synchronized (this.record)
        {
            return this.subMap.containsKey(key);
        }
    }

    @Override
    public boolean containsValue(Object value)
    {
        synchronized (this.record)
        {
            return this.subMap.containsValue(value);
        }
    }

    @Override
    public IValue get(Object key)
    {
        synchronized (this.record)
        {
            return this.subMap.get(key);
        }
    }

    @Override
    public IValue put(String key, IValue value)
    {
        synchronized (this.record)
        {
            final String internKey = Record.keysPool.intern(key);
            final IValue previous;
            if (value != null)
            {
                previous = this.subMap.put(internKey, value);
                if (previous == null || !previous.equals(value))
                {
                    this.record.addSubMapEntryUpdatedToAtomicChange(this.subMapKey, internKey, value,
                            previous);
                }
            }
            else
            {
                previous = this.subMap.remove(internKey);
                this.record.addSubMapEntryRemovedToAtomicChange(this.subMapKey, internKey, previous);
            }
            return previous;
        }
    }

    @Override
    public IValue remove(Object key)
    {
        synchronized (this.record)
        {
            if (this.record.context instanceof NoopAtomicChangeManager)
            {
                return this.subMap.remove(key);
            }
            else
            {
                final IValue previous = this.subMap.remove(key);
                this.record.addSubMapEntryRemovedToAtomicChange(this.subMapKey, key.toString(), previous);
                return previous;
            }
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends IValue> m)
    {
        synchronized (this.record)
        {
            String key;
            IValue value;
            String internKey;

            if (this.record.context instanceof NoopAtomicChangeManager)
            {
                for (Entry<? extends String, ? extends IValue> entry : m.entrySet())
                {
                    key = entry.getKey();
                    value = entry.getValue();
                    internKey = Record.keysPool.intern(key);

                    if (value != null)
                    {
                        this.subMap.put(internKey, value);
                    }
                    else
                    {
                        // a null is treated as if it removes the key
                        this.subMap.remove(internKey);
                    }
                }
            }
            else
            {
                final ThreadLocalBulkChanges changes = ThreadLocalBulkChanges.get().initialise(m.size());
                IValue previous;
                int putPtr = 0;
                int removePtr = 0;
                for (Entry<? extends String, ? extends IValue> entry : m.entrySet())
                {
                    key = entry.getKey();
                    value = entry.getValue();
                    internKey = Record.keysPool.intern(key);

                    if (value != null)
                    {
                        previous = this.subMap.put(internKey, value);
                        if (previous == null || !previous.equals(value))
                        {
                            changes.putKeys[putPtr] = internKey;
                            changes.putValues[putPtr][0] = value;
                            changes.putValues[putPtr][1] = previous;
                            putPtr++;
                        }
                    }
                    else
                    {
                        previous = this.subMap.remove(internKey);
                        changes.removedKeys[removePtr] = internKey;
                        changes.removedValues[removePtr] = previous;
                        removePtr++;

                    }
                }
                changes.putSize = putPtr;
                changes.removedSize = removePtr;
                this.record.context.addBulkSubMapChangesToAtomicChange(this.record, this.subMapKey, changes);
            }
        }
    }

    @Override
    public void clear()
    {
        synchronized (this.record)
        {
            Set<Map.Entry<String, IValue>> entrySet = entrySet();
            for (Iterator<Map.Entry<String, IValue>> iterator = entrySet.iterator(); iterator.hasNext(); )
            {
                iterator.next();
                iterator.remove();
            }
        }
    }

    @Override
    public Set<String> keySet()
    {
        final Set<String> keySet = new HashSet<String>(this.subMap.size())
        {
            @Override
            public Iterator<String> iterator()
            {
                return new Record.KeySetIterator(super.iterator(), SubMap.this);
            }
        };

        synchronized (SubMap.this.record)
        {
            keySet.addAll(this.subMap.keySet());
        }

        return keySet;
    }

    @Override
    public Collection<IValue> values()
    {
        final int size = this.subMap.size();
        final List<String> keys = new ArrayList<>(size);
        final List<IValue> values = new ArrayList<IValue>(size)
        {
            @Override
            public Iterator<IValue> iterator()
            {
                return new Record.ValuesIterator(super.iterator(), keys, SubMap.this);
            }
        };

        synchronized (SubMap.this.record)
        {
            this.subMap.forEach((key, value) -> {
                keys.add(key);
                values.add(value);
            });
        }

        return values;
    }

    @Override
    public Set<Map.Entry<String, IValue>> entrySet()
    {
        final Set<Entry<String, IValue>> entrySet = new HashSet<Entry<String, IValue>>(this.subMap.size())
        {
            @Override
            public Iterator<Entry<String, IValue>> iterator()
            {
                return new Record.EntrySetIterator(super.iterator(), SubMap.this);
            }
        };

        synchronized (SubMap.this.record)
        {
            entrySet.addAll(this.subMap.entrySet());
        }

        return entrySet;
    }

    @Override
    public boolean equals(Object o)
    {
        if (is.same(o, this))
        {
            return true;
        }
        if (is.differentClass(this, o))
        {
            return false;
        }
        SubMap other = (SubMap) o;
        synchronized (this.record)
        {
            synchronized (other.record)
            {
                return is.eq(this.subMapKey, other.subMapKey) && is.eq(this.record.getName(),
                        other.record.getName()) && is.eq(this.subMap, other.subMap);
            }
        }
    }

    @Override
    public int hashCode()
    {
        return this.subMapKey.hashCode();
    }
}