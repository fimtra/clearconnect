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
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.DataFissionProperties.Values;
import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IPublisherContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.util.ObjectPool;
import com.fimtra.util.is;

/**
 * A {@link Map} implementation that holds <code>String=IValue</code> entries and can (indirectly)
 * notify observers with atomic changes to the record. The record is created by an
 * {@link IPublisherContext} instance.
 * <p>
 * Observers are added using the {@link IObserverContext#addObserver(IRecordListener, String...)}
 * method. The notification is done by calling {@link IPublisherContext#publishAtomicChange(String)}
 * . This will notify the observers with all the changes to the record since the last call to this
 * method. This provides a mechanism to notify with atomic changes.
 * <p>
 * A change in a field only occurs if the value associated with the key changes. Updating a value
 * with an object that is equal to the previous value according to its {@link Object#equals(Object)}
 * method does not count as a change. Therefore the equals method for the value objects must be a
 * correct implementation for changes to be detected.
 * <p>
 * <b>A note on null keys and values;</b> this implementation allows null values to be stored
 * against a key but does not allow null keys.
 * <p>
 * Records are equal by value of their internal data map entries.
 * <p>
 * <b>This is a thread safe implementation BUT the same iteration semantics as per {@link Map} apply
 * for consistent read/writes in a multi-threaded access context.</b>
 * 
 * @see IRecord The IRecord interface for further behaviour documentation
 * @author Ramon Servadei
 */
final class Record implements IRecord, Cloneable
{
    private static final float LOAD_FACTOR = .75f;

    static <K, V> ConcurrentHashMap<K, V> newConcurrentHashMap(int size)
    {
        ConcurrentHashMap<K, V> map = new ConcurrentHashMap<K, V>(size, LOAD_FACTOR, Values.MAX_RECORD_CONCURRENCY);
        return map;
    }

    static <K, V> ConcurrentHashMap<K, V> newConcurrentHashMap(Map<K, V> data)
    {
        ConcurrentHashMap<K, V> map =
            new ConcurrentHashMap<K, V>(data.size(), LOAD_FACTOR, Values.MAX_RECORD_CONCURRENCY);
        map.putAll(data);
        return map;
    }

    static String toString(String contextName, String recordName, long sequence, Map<String, IValue> data,
        Map<String, Map<String, IValue>> subMaps)
    {
        String subMapString = "";
        if (subMaps.size() > Values.MAX_MAP_FIELDS_TO_PRINT)
        {
            subMapString = "{Too big to print, size=" + subMaps.size() + "}";
        }
        else
        {
            final StringBuilder sb = new StringBuilder(subMaps.size() * 100);
            Map.Entry<String, Map<String, IValue>> entry = null;
            boolean first = true;
            sb.append("{");
            for (Iterator<Map.Entry<String, Map<String, IValue>>> it = subMaps.entrySet().iterator(); it.hasNext();)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    sb.append(", ");
                }
                entry = it.next();
                sb.append(entry.getKey()).append("=").append(entry.getValue());
            }
            sb.append("}");
            subMapString = sb.toString();
        }

        final String dataString = ContextUtils.mapToString(data);
        final StringBuilder sb =
            new StringBuilder(dataString.length() + subMapString.length() + contextName.length() + recordName.length()
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

    private static final ConcurrentMap<String, Map<String, IValue>> EMPTY_SUBMAP = newConcurrentHashMap(1);
    /**
     * A pool for the keys. Keys across records stand a VERY good chance of being repeated many
     * times so this is a valuable memory optimisation.
     */
    static final ObjectPool<String> keysPool = new ObjectPool<String>("record-keys",
        DataFissionProperties.Values.KEYS_POOL_MAX);

    final AtomicLong sequence;
    final String name;
    final IAtomicChangeManager context;
    final ConcurrentMap<String, IValue> data;
    ConcurrentMap<String, Map<String, IValue>> subMaps;
    final Lock readLock;
    final Lock writeLock;

    Record(String name, Map<String, IValue> data, IAtomicChangeManager context)
    {
        super();
        this.name = keysPool.intern(name);
        this.data = newConcurrentHashMap(data);
        this.subMaps = EMPTY_SUBMAP;
        this.context = context;
        final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        this.readLock = reentrantReadWriteLock.readLock();
        this.writeLock = reentrantReadWriteLock.writeLock();
        this.sequence = new AtomicLong(0);
    }

    /**
     * Clone constructor for providing pre-sized subMaps to prevent re-hashing
     */
    Record(String name, Map<String, IValue> data, IAtomicChangeManager context,
        ConcurrentMap<String, Map<String, IValue>> subMaps)
    {
        super();
        this.name = keysPool.intern(name);
        this.data = newConcurrentHashMap(data);
        this.subMaps = subMaps;
        this.context = context;
        final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        this.readLock = reentrantReadWriteLock.readLock();
        this.writeLock = reentrantReadWriteLock.writeLock();
        this.sequence = new AtomicLong(0);
    }

    @Override
    public IRecord getImmutableInstance()
    {
        this.readLock.lock();
        try
        {
            return new ImmutableRecord(this);
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @Override
    public void clear()
    {
        this.writeLock.lock();
        try
        {
            for (Iterator<Map.Entry<String, IValue>> it = this.data.entrySet().iterator(); it.hasNext();)
            {
                remove(it.next().getKey());
            }

            for (Iterator<Map.Entry<String, Map<String, IValue>>> it = this.subMaps.entrySet().iterator(); it.hasNext();)
            {
                it.next().getValue().clear();
            }
        }
        finally
        {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean containsKey(Object key)
    {
        this.readLock.lock();
        try
        {
            return this.data.containsKey(key);
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @Override
    public boolean containsValue(Object value)
    {
        this.readLock.lock();
        try
        {
            return this.data.containsValue(value);
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @Override
    public Set<Entry<String, IValue>> entrySet()
    {
        this.readLock.lock();
        try
        {
            return new AbstractSet<Entry<String, IValue>>()
            {
                final Set<java.util.Map.Entry<String, IValue>> backingEntrySet =
                    Record.this.getSnapshotOfBackingEntrySet();

                @Override
                public Iterator<Entry<String, IValue>> iterator()
                {
                    return new EntrySetIterator(Record.this, this.backingEntrySet.iterator(), Record.this);
                }

                @Override
                public int size()
                {
                    return this.backingEntrySet.size();
                }
            };
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @Override
    public boolean equals(Object o)
    {
        this.readLock.lock();
        try
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
            return is.eq(this.name, other.name) && is.eq(this.context.getName(), other.context.getName())
                && is.eq(this.data, other.data) && is.eq(this.subMaps, other.subMaps);
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @Override
    public IValue get(Object key)
    {
        this.readLock.lock();
        try
        {
            return this.data.get(key);
        }
        finally
        {
            this.readLock.unlock();
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
        this.readLock.lock();
        try
        {
            return this.data.isEmpty();
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @Override
    public Set<String> keySet()
    {
        this.readLock.lock();
        try
        {
            return new AbstractSet<String>()
            {
                final Set<java.util.Map.Entry<String, IValue>> backingEntrySet =
                    Record.this.getSnapshotOfBackingEntrySet();

                @Override
                public Iterator<String> iterator()
                {
                    return new KeySetIterator(Record.this, this.backingEntrySet.iterator(), Record.this);
                }

                @Override
                public int size()
                {
                    return this.backingEntrySet.size();
                }
            };
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @Override
    public IValue put(String key, IValue value)
    {
        if (key == null)
        {
            throw new NullPointerException("null keys are not allowed");
        }
        this.writeLock.lock();
        try
        {
            return corePut_callWithWriteLock(key, value);
        }
        finally
        {
            this.writeLock.unlock();
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
        return put(key, new DoubleValue(value));
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

    @SuppressWarnings("unchecked")
    @Override
    public void putAll(Map<? extends String, ? extends IValue> t)
    {
        this.writeLock.lock();
        try
        {
            Map.Entry<String, IValue> entry = null;
            String key = null;
            IValue value = null;
            for (Iterator<?> it = t.entrySet().iterator(); it.hasNext();)
            {
                entry = (Map.Entry<String, IValue>) it.next();
                key = entry.getKey();
                value = entry.getValue();
                corePut_callWithWriteLock(key, value);
            }
        }
        finally
        {
            this.writeLock.unlock();
        }
    }

    @Override
    public IValue remove(Object key)
    {
        if (key instanceof String)
        {
            this.writeLock.lock();
            try
            {
                if (this.data.containsKey(key))
                {
                    return coreRemove_callWithWriteLock(key);
                }
            }
            finally
            {
                this.writeLock.unlock();
            }
        }
        return null;
    }

    @Override
    public int size()
    {
        this.readLock.lock();
        try
        {
            return this.data.size();
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @Override
    public Collection<IValue> values()
    {
        this.readLock.lock();
        try
        {
            return new AbstractCollection<IValue>()
            {
                final Set<java.util.Map.Entry<String, IValue>> backingEntrySet =
                    Record.this.getSnapshotOfBackingEntrySet();

                @Override
                public Iterator<IValue> iterator()
                {
                    return new ValuesIterator(Record.this, this.backingEntrySet.iterator(), Record.this);
                }

                @Override
                public int size()
                {
                    return this.backingEntrySet.size();
                }
            };
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @Override
    public String toString()
    {
        this.readLock.lock();
        try
        {
            return toString(this.context.getName(), this.name, this.sequence.longValue(), this.data, this.subMaps);
        }
        finally
        {
            this.readLock.unlock();
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
        final String internKey = keysPool.intern(key);
        this.readLock.lock();
        try
        {
            if (this.subMaps != EMPTY_SUBMAP)
            {
                Map<String, IValue> submap = this.subMaps.get(internKey);
                if (submap != null)
                {
                    return submap;
                }
            }
        }
        finally
        {
            this.readLock.unlock();
        }
        this.writeLock.lock();
        try
        {
            if (this.subMaps == EMPTY_SUBMAP)
            {
                this.subMaps = newConcurrentHashMap(1);
            }
            final SubMap newSubMap = new SubMap(this, internKey);
            // put if absent to handle multiple writers running concurrently but blocked by the lock
            // this ensures only the first added one is used
            final Map<String, IValue> previous = this.subMaps.putIfAbsent(internKey, newSubMap);
            return previous == null ? newSubMap : previous;
        }
        finally
        {
            this.writeLock.unlock();
        }
    }

    @Override
    public Map<String, IValue> removeSubMap(String key)
    {
        final Map<String, IValue> subMap;
        this.writeLock.lock();
        try
        {
            subMap = this.subMaps.remove(key);
            if (subMap == null)
            {
                return ContextUtils.EMPTY_MAP;
            }
            Map<String, IValue> previous = new HashMap<String, IValue>(subMap);
            // this call adds the remove to the atomic change
            subMap.clear();
            return previous;
        }
        finally
        {
            this.writeLock.unlock();
        }
    }

    @Override
    public Set<String> getSubMapKeys()
    {
        this.readLock.lock();
        try
        {
            return this.subMaps.keySet();
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IValue> T get(String key)
    {
        this.readLock.lock();
        try
        {
            return (T) this.data.get(key);
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void resolveFromStream(Reader reader) throws IOException
    {
        final HashMap<String, IValue> flattenedMap = new HashMap<String, IValue>();
        ContextUtils.resolveRecordMapFromStream(reader, flattenedMap);
        final Map<?, ?>[] demergeMaps = ContextUtils.demergeMaps(flattenedMap);
        this.writeLock.lock();
        try
        {
            // trigger an atomic change by adding to self
            putAll((Map<String, IValue>) demergeMaps[0]);
            Map<String, Map<String, IValue>> subMaps = (Map<String, Map<String, IValue>>) demergeMaps[1];

            Map.Entry<String, Map<String, IValue>> entry = null;
            String key = null;
            Map<String, IValue> value = null;
            for (Iterator<Map.Entry<String, Map<String, IValue>>> it = subMaps.entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                key = entry.getKey();
                value = entry.getValue();
                getOrCreateSubMap(key).putAll(value);
            }
        }
        finally
        {
            this.writeLock.unlock();
        }
    }

    @Override
    public void serializeToStream(Writer writer) throws IOException
    {
        this.readLock.lock();
        try
        {
            ContextUtils.serializeRecordMapToStream(writer, asFlattenedMap());
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @Override
    public Lock getWriteLock()
    {
        return this.writeLock;
    }

    /**
     * Create a deep-copy of the record (a 'snapshot')
     */
    @Override
    protected Record clone()
    {
        // this is a deep copy of the record internal maps
        this.readLock.lock();
        try
        {
            final Record cloneRecord;
            if (this.subMaps.size() == 0)
            {
                cloneRecord = new Record(this.name, newConcurrentHashMap(this.data), this.context);
            }
            else
            {
                final ConcurrentMap<String, Map<String, IValue>> cloneSubMaps =
                    newConcurrentHashMap(this.subMaps.size());
                cloneRecord = new Record(this.name, newConcurrentHashMap(this.data), this.context, cloneSubMaps);
                Map.Entry<String, Map<String, IValue>> entry = null;
                for (final Iterator<Map.Entry<String, Map<String, IValue>>> it = this.subMaps.entrySet().iterator(); it.hasNext();)
                {
                    entry = it.next();
                    cloneSubMaps.put(entry.getKey(), ((SubMap) entry.getValue()).clone(cloneRecord));
                }
            }
            cloneRecord.sequence.set(this.sequence.get());
            return cloneRecord;
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    @Override
    public Map<String, IValue> asFlattenedMap()
    {
        this.readLock.lock();
        try
        {
            return ContextUtils.mergeMaps(this.data, this.subMaps);
        }
        finally
        {
            this.readLock.unlock();
        }

    }

    @Override
    public long getSequence()
    {
        return this.sequence.longValue();
    }

    IValue corePut_callWithWriteLock(String key, IValue value)
    {
        final String internKey = keysPool.intern(key);
        // concurrent maps don't allow nulls
        if (value != null)
        {
            final IValue previous = this.data.put(internKey, value);
            // if there is no change, we perform no update
            if (this.context != null)
            {
                if (previous == null || !previous.equals(value))
                {
                    this.context.addEntryUpdatedToAtomicChange(this, internKey, value, previous);
                }
            }
            return previous;
        }
        else
        {
            // a null is treated as if it removes the key
            final IValue previous = this.data.remove(internKey);
            if (this.context != null)
            {
                if (previous != null)
                {
                    this.context.addEntryRemovedToAtomicChange(this, internKey, previous);
                }
            }
            return previous;
        }
    }

    IValue coreRemove_callWithWriteLock(Object key)
    {
        final IValue value = this.data.remove(key);
        addEntryRemovedToAtomicChange((String) key, value);
        return value;
    }

    Set<Entry<String, IValue>> getSnapshotOfBackingEntrySet()
    {
        this.readLock.lock();
        try
        {
            return new HashSet<Entry<String, IValue>>(this.data.entrySet());
        }
        finally
        {
            this.readLock.unlock();
        }
    }

    void addEntryRemovedToAtomicChange(String key, final IValue value)
    {
        if (this.context != null)
        {
            this.context.addEntryRemovedToAtomicChange(this, key, value);
        }
    }

    void addSubMapEntryUpdatedToAtomicChange(String subMapKey, String key, final IValue current, IValue previous)
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
     * 
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
     * 
     * This method returns an array holding the <code>subMapKey</code> and
     * <code>keyOfEntryInSubMap</code> parts.
     * 
     * @param recordKey
     *            one of the keys in a record
     * @return <code>new String[]{subMapKey, keyOfEntryInSubMap}</code> OR <code>null</code> if the
     *         record key is not for a sub-map
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

    final String prefix;
    final Record record;
    final String subMapKey;
    final ConcurrentMap<String, IValue> subMap;

    SubMap(Record record, String subMapKey)
    {
        this.subMapKey = subMapKey;
        this.record = record;
        this.prefix = SubMap.encodeSubMapKey(subMapKey);
        this.subMap = Record.newConcurrentHashMap(2);
        Map.Entry<String, IValue> entry;
        String key;
        IValue value;
        for (Iterator<Map.Entry<String, IValue>> it = this.record.data.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            key = entry.getKey();
            value = entry.getValue();
            if (key.startsWith(this.prefix, 0))
            {
                this.subMap.put(key.substring(this.prefix.length()), value);
            }
        }
    }

    private SubMap(Record record, String subMapKey, String prefix, ConcurrentMap<String, IValue> subMap)
    {
        this.record = record;
        this.subMapKey = subMapKey;
        this.prefix = prefix;
        this.subMap = subMap;
    }

    SubMap clone(Record cloneRecord)
    {
        final ConcurrentHashMap<String, IValue> clonedSubMap = Record.newConcurrentHashMap(this.subMap.size());
        clonedSubMap.putAll(this.subMap);
        return new SubMap(cloneRecord, this.subMapKey, this.prefix, clonedSubMap);
    }

    @Override
    public String toString()
    {
        return ContextUtils.mapToString(this.subMap);
    }

    Iterator<Map.Entry<String, IValue>> subMapIterator(final Iterator<Map.Entry<String, IValue>> subMapIterator)
    {
        return new Iterator<Map.Entry<String, IValue>>()
        {
            Map.Entry<String, IValue> current;

            @Override
            public boolean hasNext()
            {
                boolean hasNext = subMapIterator.hasNext();
                if (!hasNext)
                {
                    this.current = null;
                }
                return hasNext;
            }

            @Override
            public Map.Entry<String, IValue> next()
            {
                this.current = subMapIterator.next();
                return this.current;
            }

            @Override
            public void remove()
            {
                subMapIterator.remove();
            }
        };
    }

    @Override
    public int size()
    {
        this.record.readLock.lock();
        try
        {
            return this.subMap.size();
        }
        finally
        {
            this.record.readLock.unlock();
        }
    }

    @Override
    public boolean isEmpty()
    {
        this.record.readLock.lock();
        try
        {
            return this.subMap.isEmpty();
        }
        finally
        {
            this.record.readLock.unlock();
        }
    }

    @Override
    public boolean containsKey(Object key)
    {
        this.record.readLock.lock();
        try
        {
            return this.subMap.containsKey(key);
        }
        finally
        {
            this.record.readLock.unlock();
        }
    }

    @Override
    public boolean containsValue(Object value)
    {
        this.record.readLock.lock();
        try
        {
            return this.subMap.containsValue(value);
        }
        finally
        {
            this.record.readLock.unlock();
        }
    }

    @Override
    public IValue get(Object key)
    {
        this.record.readLock.lock();
        try
        {
            return this.subMap.get(key);
        }
        finally
        {
            this.record.readLock.unlock();
        }
    }

    @Override
    public IValue put(String key, IValue value)
    {
        this.record.writeLock.lock();
        try
        {
            return corePut_callWithWriteLock(key, value);
        }
        finally
        {
            this.record.writeLock.unlock();
        }
    }

    IValue corePut_callWithWriteLock(String key, IValue value)
    {
        final String internKey = Record.keysPool.intern(key);
        // concurrent maps don't allow nulls
        if (value != null)
        {
            final IValue previous = this.subMap.put(internKey, value);
            if (previous == null || !previous.equals(value))
            {
                this.record.addSubMapEntryUpdatedToAtomicChange(this.subMapKey, internKey, value, previous);
            }
            return previous;
        }
        else
        {
            final IValue previous = this.subMap.remove(internKey);
            this.record.addSubMapEntryRemovedToAtomicChange(this.subMapKey, internKey, previous);
            return previous;
        }
    }

    @Override
    public IValue remove(Object key)
    {
        this.record.writeLock.lock();
        try
        {
            final IValue previous = this.subMap.remove(key);
            this.record.addSubMapEntryRemovedToAtomicChange(this.subMapKey, key.toString(), previous);
            return previous;
        }
        finally
        {
            this.record.writeLock.unlock();
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends IValue> m)
    {
        this.record.writeLock.lock();
        try
        {
            Map.Entry<?, ?> entry = null;
            String key = null;
            IValue value = null;
            for (Iterator<?> it = m.entrySet().iterator(); it.hasNext();)
            {
                entry = (Map.Entry<?, ?>) it.next();
                key = (String) entry.getKey();
                value = (IValue) entry.getValue();
                corePut_callWithWriteLock(key, value);
            }
        }
        finally
        {
            this.record.writeLock.unlock();
        }
    }

    @Override
    public void clear()
    {
        this.record.writeLock.lock();
        try
        {
            Set<Map.Entry<String, IValue>> entrySet = entrySet();
            for (Iterator<Map.Entry<String, IValue>> iterator = entrySet.iterator(); iterator.hasNext();)
            {
                iterator.next();
                iterator.remove();
            }
        }
        finally
        {
            this.record.writeLock.unlock();
        }
    }

    @Override
    public Set<String> keySet()
    {
        return new AbstractSet<String>()
        {
            final Set<java.util.Map.Entry<String, IValue>> backingEntrySet = getSnapshotOfBackingEntrySet();

            @Override
            public Iterator<String> iterator()
            {
                return new KeySetIterator(SubMap.this.record, subMapIterator(this.backingEntrySet.iterator()),
                    SubMap.this)
                {
                    @Override
                    void remove_callWithWriteLock(String key, IValue value)
                    {
                        this.record.addSubMapEntryRemovedToAtomicChange(SubMap.this.subMapKey, key, value);
                    }
                };
            }

            @Override
            public int size()
            {
                return this.backingEntrySet.size();
            }
        };
    }

    @Override
    public Collection<IValue> values()
    {
        return new AbstractCollection<IValue>()
        {
            final Set<java.util.Map.Entry<String, IValue>> backingEntrySet = getSnapshotOfBackingEntrySet();

            @Override
            public Iterator<IValue> iterator()
            {
                return new ValuesIterator(SubMap.this.record, subMapIterator(this.backingEntrySet.iterator()),
                    SubMap.this)
                {
                    @Override
                    void remove_callWithWriteLock(String key, IValue value)
                    {
                        this.record.addSubMapEntryRemovedToAtomicChange(SubMap.this.subMapKey, key, value);
                    }
                };
            }

            @Override
            public int size()
            {
                return this.backingEntrySet.size();
            }
        };
    }

    @Override
    public Set<Map.Entry<String, IValue>> entrySet()
    {
        return new AbstractSet<Entry<String, IValue>>()
        {
            final Set<java.util.Map.Entry<String, IValue>> backingEntrySet = getSnapshotOfBackingEntrySet();

            @Override
            public Iterator<Entry<String, IValue>> iterator()
            {
                return new EntrySetIterator(SubMap.this.record, subMapIterator(this.backingEntrySet.iterator()),
                    SubMap.this)
                {
                    @Override
                    void remove_callWithWriteLock(String key, IValue value)
                    {
                        this.record.addSubMapEntryRemovedToAtomicChange(SubMap.this.subMapKey, key, value);
                    }
                };
            }

            @Override
            public int size()
            {
                return this.backingEntrySet.size();
            }
        };
    }

    @Override
    public boolean equals(Object o)
    {
        this.record.readLock.lock();
        try
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
            return is.eq(this.prefix, other.prefix) && is.eq(this.record.getName(), other.record.getName())
                && is.eq(this.subMap, other.subMap);
        }
        finally
        {
            this.record.readLock.unlock();
        }
    }

    @Override
    public int hashCode()
    {
        return this.prefix.hashCode();
    }

    Set<Map.Entry<String, IValue>> getSnapshotOfBackingEntrySet()
    {
        this.record.readLock.lock();
        try
        {
            return new HashSet<Map.Entry<String, IValue>>(this.subMap.entrySet());
        }
        finally
        {
            this.record.readLock.unlock();
        }
    }
}

/**
 * A base class for an iterator that can be returned from the {@link Collection} objects returned
 * from the methods {@link Record#keySet()}, {@link Record#values()} and {@link Record#entrySet()}.
 * This iterator will call the {@link Record#addEntryRemovedToAtomicChange(String, IValue)} method
 * when items from the underlying {@link Map} of the record are removed by this iterator.
 * 
 * @author Ramon Servadei
 * @param <IteratorType>
 *            the type the {@link Iterator} returns
 */
abstract class AbstractNotifyingIterator<IteratorType> implements Iterator<IteratorType>
{
    /** The record this entry set iterator operates on */
    final Record record;
    final Map<String, IValue> target;

    /**
     * The iterator returned from the {@link #record} - this is a SNAPSHOT of the entries at the
     * point when the iterator was created
     */
    private final Iterator<Entry<String, IValue>> snapshotEntryIterator;

    /** Tracks the current iteration item */
    private Entry<String, IValue> current;

    AbstractNotifyingIterator(Record record, Iterator<Entry<String, IValue>> entryIterator, Map<String, IValue> target)
    {
        this.record = record;
        this.snapshotEntryIterator = entryIterator;
        this.target = target;
    }

    @Override
    public boolean hasNext()
    {
        boolean hasNext = this.snapshotEntryIterator.hasNext();
        if (!hasNext)
        {
            this.current = null;
        }
        return hasNext;
    }

    @Override
    public IteratorType next()
    {
        final Entry<String, IValue> next = this.snapshotEntryIterator.next();
        this.current = next;
        return getValueForNext(this.current);
    }

    @Override
    public void remove()
    {
        this.record.getWriteLock().lock();
        try
        {
            this.snapshotEntryIterator.remove();
            this.target.remove(this.current.getKey());
            remove_callWithWriteLock(this.current.getKey(), this.current.getValue());
        }
        finally
        {
            this.record.getWriteLock().unlock();
        }
    }

    abstract void remove_callWithWriteLock(String key, IValue value);

    /**
     * Called from the {@link #next()} method to return the correct value from the current record
     * entry.
     * 
     * @param currentEntry
     *            the current record entry the iterator is pointing to after the delegate iterator's
     *            call to {@link #next()}
     * @return the correct value from the current record entry
     */
    abstract IteratorType getValueForNext(Entry<String, IValue> currentEntry);
}

/**
 * Iterates over the {@link Record#keySet()} collection.
 * 
 * @author Ramon Servadei
 */
class KeySetIterator extends AbstractNotifyingIterator<String>
{
    KeySetIterator(Record record, Iterator<Entry<String, IValue>> entryIterator, Map<String, IValue> target)
    {
        super(record, entryIterator, target);
    }

    @Override
    String getValueForNext(Entry<String, IValue> currentEntry)
    {
        return currentEntry.getKey();
    }

    @Override
    void remove_callWithWriteLock(String key, IValue value)
    {
        this.record.addEntryRemovedToAtomicChange(key, value);
    }
}

/**
 * Iterates over the {@link Record#entrySet()} collection.
 * 
 * @author Ramon Servadei
 */
class EntrySetIterator extends AbstractNotifyingIterator<Entry<String, IValue>>
{

    EntrySetIterator(Record record, Iterator<Entry<String, IValue>> entryIterator, Map<String, IValue> target)
    {
        super(record, entryIterator, target);
    }

    @Override
    Entry<String, IValue> getValueForNext(Entry<String, IValue> currentEntry)
    {
        return currentEntry;
    }

    @Override
    void remove_callWithWriteLock(String key, IValue value)
    {
        this.record.addEntryRemovedToAtomicChange(key, value);
    }
}

/**
 * Iterates over the {@link Record#values()} collection.
 * 
 * @author Ramon Servadei
 */
class ValuesIterator extends AbstractNotifyingIterator<IValue>
{

    ValuesIterator(Record record, Iterator<Entry<String, IValue>> entryIterator, Map<String, IValue> target)
    {
        super(record, entryIterator, target);
    }

    @Override
    IValue getValueForNext(Entry<String, IValue> currentEntry)
    {
        return currentEntry.getValue();
    }

    @Override
    void remove_callWithWriteLock(String key, IValue value)
    {
        this.record.addEntryRemovedToAtomicChange(key, value);
    }
}