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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue;
import com.fimtra.util.CollectionUtils;

/**
 * An immutable {@link IRecord}.
 * <p>
 * Immutable here means the contents of the record cannot be changed by this object. <b>However, the
 * contents of the record can change if the immutable record is created with a LIVE record backing
 * it. Changes made to the live instance will be seen by this immutable instance.</b>
 * <p>
 * An immutable record ignores the read-write lock of its backing record and accesses the record's
 * underlying {@link ConcurrentHashMap} directly. This provides faster access to the fields. This
 * should perfectly fine for most usecases as the immutable record is (generally) only used in the
 * {@link IRecordListener#onChange(IRecord, com.fimtra.datafission.IRecordChange)} callback method
 * as the image argument.
 * 
 * @author Ramon Servadei
 */
public class ImmutableRecord implements IRecord
{
    /**
     * Create a snapshot of the {@link IRecord} as the source for a new {@link ImmutableRecord}
     * instance.
     * 
     * @deprecated Use {@link ImmutableSnapshotRecord#create(IRecord)} instead
     */
    @Deprecated
    public static ImmutableSnapshotRecord snapshot(IRecord template)
    {
        return ImmutableSnapshotRecord.create(template);
    }

    final Record backingRecord;

    /**
     * Construct an immutable record backed by a record. Changes made to the backing record are
     * visible via the immutable instance.
     */
    ImmutableRecord(Record template)
    {
        this.backingRecord = template;
    }

    @Override
    public int size()
    {
        return this.backingRecord.data.size();
    }

    @Override
    public boolean isEmpty()
    {
        return this.backingRecord.data.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return this.backingRecord.data.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        return this.backingRecord.data.containsValue(value);
    }

    @Override
    public IValue get(Object key)
    {
        return this.backingRecord.data.get(key);
    }

    @Override
    public IValue put(String key, IValue value)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.backingRecord.getContextName()
            + ":" + this.backingRecord.getName());
    }

    @Override
    public IValue remove(Object key)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.backingRecord.getContextName()
            + ":" + this.backingRecord.getName());
    }

    @Override
    public void putAll(Map<? extends String, ? extends IValue> m)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.backingRecord.getContextName()
            + ":" + this.backingRecord.getName());
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.backingRecord.getContextName()
            + ":" + this.backingRecord.getName());
    }

    @Override
    public Set<String> keySet()
    {
        return Collections.unmodifiableSet(this.backingRecord.data.keySet());
    }

    @Override
    public Collection<IValue> values()
    {
        return Collections.unmodifiableCollection(this.backingRecord.data.values());
    }

    @Override
    public Set<java.util.Map.Entry<String, IValue>> entrySet()
    {
        return CollectionUtils.unmodifiableEntrySet(this.backingRecord.data.entrySet());
    }

    @Override
    public boolean equals(Object o)
    {
        return this.backingRecord.equals(o);
    }

    @Override
    public int hashCode()
    {
        return this.backingRecord.hashCode();
    }

    @Override
    public String getName()
    {
        return this.backingRecord.getName();
    }

    @Override
    public String getContextName()
    {
        return this.backingRecord.getContextName();
    }

    @Override
    public IValue put(String key, long value)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.backingRecord.getContextName()
            + ":" + this.backingRecord.getName());
    }

    @Override
    public IValue put(String key, double value)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.backingRecord.getContextName()
            + ":" + this.backingRecord.getName());
    }

    @Override
    public IValue put(String key, String value)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.backingRecord.getContextName()
            + ":" + this.backingRecord.getName());
    }

    @Override
    public Set<String> getSubMapKeys()
    {
        return Collections.unmodifiableSet(this.backingRecord.subMaps.keySet());
    }

    @Override
    public Map<String, IValue> getOrCreateSubMap(String subMapKey)
    {
        if (this.backingRecord.subMaps.keySet().contains(subMapKey))
        {
            return new ImmutableSubmap((SubMap) this.backingRecord.subMaps.get(subMapKey));
        }
        return ContextUtils.EMPTY_MAP;
    }

    @Override
    public Map<String, IValue> removeSubMap(String subMapKey)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.backingRecord.getContextName()
            + ":" + this.backingRecord.getName());
    }

    @Override
    public String toString()
    {
        return "(Immutable)" + this.backingRecord.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IValue> T get(String key)
    {
        return (T) this.backingRecord.data.get(key);
    }

    @Override
    public void resolveFromStream(Reader reader) throws IOException
    {
        throw new UnsupportedOperationException("Cannot resolve immutable record from a stream "
            + this.backingRecord.getContextName() + ":" + this.backingRecord.getName());
    }

    @Override
    public void serializeToStream(Writer writer) throws IOException
    {
        ContextUtils.serializeRecordMapToStream(writer, asFlattenedMap());
    }

    @Override
    public Lock getWriteLock()
    {
        return this.backingRecord.getWriteLock();
    }

    @Override
    public IRecord getImmutableInstance()
    {
        return this;
    }

    @Override
    public Map<String, IValue> asFlattenedMap()
    {
        return ContextUtils.mergeMaps(this.backingRecord.data, this.backingRecord.subMaps);
    }

    @Override
    public long getSequence()
    {
        return this.backingRecord.getSequence();
    }
}

/**
 * Immutable wrapper for a {@link SubMap}.
 * <p>
 * This returns direct versions of {@link #values()}, {@link #keySet()}, {@link #entrySet()} from
 * the backing {@link SubMap}
 * 
 * @author Ramon Servadei
 */
class ImmutableSubmap implements Map<String, IValue>
{
    final SubMap delegate;

    ImmutableSubmap(SubMap delegate)
    {
        super();
        this.delegate = delegate;
    }

    @Override
    public int size()
    {
        return this.delegate.subMap.size();
    }

    @Override
    public boolean isEmpty()
    {
        return this.delegate.subMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return this.delegate.subMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        return this.delegate.subMap.containsValue(value);
    }

    @Override
    public IValue get(Object key)
    {
        return this.delegate.subMap.get(key);
    }

    @Override
    public IValue put(String key, IValue value)
    {
        throw new UnsupportedOperationException("Cannot call on immutable subMap " + this.delegate.subMapKey + " of "
            + this.delegate.record.getContextName() + ":" + this.delegate.record.getName());
    }

    @Override
    public IValue remove(Object key)
    {
        throw new UnsupportedOperationException("Cannot call on immutable subMap " + this.delegate.subMapKey + " of "
            + this.delegate.record.getContextName() + ":" + this.delegate.record.getName());
    }

    @Override
    public void putAll(Map<? extends String, ? extends IValue> m)
    {
        throw new UnsupportedOperationException("Cannot call on immutable subMap " + this.delegate.subMapKey + " of "
            + this.delegate.record.getContextName() + ":" + this.delegate.record.getName());
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException("Cannot call on immutable subMap " + this.delegate.subMapKey + " of "
            + this.delegate.record.getContextName() + ":" + this.delegate.record.getName());
    }

    @Override
    public Set<String> keySet()
    {
        return Collections.unmodifiableSet(this.delegate.subMap.keySet());
    }

    @Override
    public Collection<IValue> values()
    {
        return Collections.unmodifiableCollection(this.delegate.subMap.values());
    }

    @Override
    public Set<java.util.Map.Entry<String, IValue>> entrySet()
    {
        return CollectionUtils.unmodifiableEntrySet(this.delegate.subMap.entrySet());
    }

    @Override
    public boolean equals(Object o)
    {
        return this.delegate.equals(o);
    }

    @Override
    public int hashCode()
    {
        return this.delegate.hashCode();
    }
}
