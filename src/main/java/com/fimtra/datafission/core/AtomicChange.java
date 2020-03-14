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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.thimble.ISequentialRunnable;
import com.fimtra.util.is;

/**
 * Represents an atomic change for a single named record.
 * 
 * @author Ramon Servadei
 */
public final class AtomicChange implements IRecordChange, ISequentialRunnable
{
    private static final Long SEQ_INIT = Long.valueOf(-1);

    static final Map<String, IValue> EMPTY_MAP = Collections.unmodifiableMap(ContextUtils.EMPTY_MAP);

    final static IRecordChange NULL_CHANGE = new IRecordChange()
    {
        @Override
        public boolean isEmpty()
        {
            return true;
        }

        @Override
        public Set<String> getSubMapKeys()
        {
            return ContextUtils.EMPTY_STRING_SET;
        }

        @Override
        public IRecordChange getSubMapAtomicChange(String subMapKey)
        {
            return NULL_CHANGE;
        }

        @Override
        public Map<String, IValue> getRemovedEntries()
        {
            return EMPTY_MAP;
        }

        @Override
        public Map<String, IValue> getPutEntries()
        {
            return EMPTY_MAP;
        }

        @Override
        public Map<String, IValue> getOverwrittenEntries()
        {
            return EMPTY_MAP;
        }

        @Override
        public String getName()
        {
            return "null";
        }

        @Override
        public void applyTo(Map<String, IValue> target)
        {
        }

        @Override
        public void applyCompleteAtomicChangeToRecord(IRecord record)
        {
        }

        @Override
        public void coalesce(List<IRecordChange> subsequentChanges)
        {
        }

        @Override
        public void setScope(char scope)
        {
        }

        @Override
        public void setSequence(long sequence)
        {
        }

        @Override
        public char getScope()
        {
            return DELTA_SCOPE_CHAR;
        }

        @Override
        public long getSequence()
        {
            return -1;
        }

        @Override
        public int getSize()
        {
            return 0;
        }
    };

    static <K, V> Map<K, V> newMap()
    {
        return new HashMap<>();
    }

    final String name;
    AtomicReference<Character> scope = new AtomicReference<>(DELTA_SCOPE);
    AtomicReference<Long> sequence = new AtomicReference<>(SEQ_INIT);

    Map<String, IValue> putEntries;
    Map<String, IValue> overwrittenEntries;
    Map<String, IValue> removedEntries;
    Set<String> subMapKeys;
    Map<String, AtomicChange> subMapAtomicChanges;

    // members needed for the ISequentialRunnable use
    Context context;
    CountDownLatch latch;

    /**
     * Construct the atomic change to represent the record.
     * <p>
     * Note: the change takes the sequence of the record and is image scope.
     */
    public AtomicChange(IRecord image)
    {
        this(image.getName());
        internalGetPutEntries().putAll(image);
        for (String subMapKey : image.getSubMapKeys())
        {
            internalGetSubMapAtomicChange(subMapKey).internalGetPutEntries().putAll(image.getOrCreateSubMap(subMapKey));
        }
        this.scope.set(IMAGE_SCOPE);
        this.sequence.set(Long.valueOf(image.getSequence()));
    }

    public AtomicChange(String name, Map<String, IValue> putEntries, Map<String, IValue> overwrittenEntries,
        Map<String, IValue> removedEntries)
    {
        super();
        this.name = name;
        this.putEntries = putEntries;
        this.overwrittenEntries = overwrittenEntries;
        this.removedEntries = removedEntries;
    }

    AtomicChange(String name)
    {
        this(name, null, null, null);
    }

    // ==== methods used to support use as the ISequentialRunnable

    void preparePublish(CountDownLatch latch, Context context)
    {
        this.latch = latch;
        this.context = context;
    }

    @Override
    public void run()
    {
        try
        {
            this.context.doPublishChange(this.name, this, this.sequence.get().longValue());
        }
        finally
        {
            this.context.throttle.eventFinish();
            this.latch.countDown();
        }
    }

    @Override
    public Object context()
    {
        return this.name;
    }

    // ==== END methods used to support use as the ISequentialRunnable

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public Map<String, IValue> getPutEntries()
    {
        if (this.putEntries == null)
        {
            return EMPTY_MAP;
        }
        return Collections.unmodifiableMap(internalGetPutEntries());
    }

    @Override
    public Map<String, IValue> getOverwrittenEntries()
    {
        if (this.overwrittenEntries == null)
        {
            return EMPTY_MAP;
        }
        return Collections.unmodifiableMap(internalGetOverwrittenEntries());
    }

    @Override
    public Map<String, IValue> getRemovedEntries()
    {
        if (this.removedEntries == null)
        {
            return EMPTY_MAP;
        }
        return Collections.unmodifiableMap(internalGetRemovedEntries());
    }

    @Override
    public boolean isEmpty()
    {
        boolean dataEmpty = noOverwrittenEntries() && noPutEntries() && noRemovedEntries();
        if (dataEmpty && this.subMapAtomicChanges != null)
        {
            for (Iterator<Map.Entry<String, AtomicChange>> it = this.subMapAtomicChanges.entrySet().iterator(); it.hasNext()
                    && dataEmpty;)
            {
                dataEmpty &= it.next().getValue().isEmpty();
            }
        }
        return dataEmpty;
    }

    @Override
    public int getSize()
    {
        int size = this.putEntries == null ? 0 : this.putEntries.size();
        size += this.overwrittenEntries == null ? 0 : this.overwrittenEntries.size();
        size += this.removedEntries == null ? 0 : this.removedEntries.size();

        if (this.subMapAtomicChanges != null)
        {
            AtomicChange value = null;
            for (Iterator<Map.Entry<String, AtomicChange>> it =
                this.subMapAtomicChanges.entrySet().iterator(); it.hasNext();)
            {
                value = it.next().getValue();
                size += value.putEntries == null ? 0 : value.putEntries.size();
                size += value.overwrittenEntries == null ? 0 : value.overwrittenEntries.size();
                size += value.removedEntries == null ? 0 : value.removedEntries.size();
            }
        }
        return size;
    }

    @Override
    public String toString()
    {
        return "AtomicChange [name="
            + this.name
            + ", "
            + this.scope
            + this.sequence
            + (noPutEntries() ? "" : ", putEntries=" + ContextUtils.mapToString(this.putEntries))
            + (noOverwrittenEntries() ? "" : ", overwrittenEntries="
                + ContextUtils.mapToString(this.overwrittenEntries))
            + (noRemovedEntries() ? "" : ", removedEntries=" + ContextUtils.mapToString(this.removedEntries))
            + (this.subMapAtomicChanges == null ? "" : " subMapAtomicChanges=" + this.subMapAtomicChanges) + "]";
    }

    @Override
    public void coalesce(List<IRecordChange> subsequentChanges)
    {
        final Map<String, IValue> putEntries = newMap();
        final Map<String, IValue> overwrittenEntries = newMap();
        final Map<String, IValue> removedEntries = newMap();
        final Set<String> ultimatelyRemovedKeys = new HashSet<>();
        final Set<String> ultimatelyAddedKeys = new HashSet<>();
        Map<String, IValue> newPutEntries;
        Map<String, IValue> newOverwrittenEntries;
        Map<String, IValue> newRemovedEntries;

        // sub-map vars
        Set<String> subMapKeysToMerge;
        Iterator<String> subMapKeysToMergeIterator;
        Map<String, List<IRecordChange>> subMapChangesToMerge = null;

        List<IRecordChange> subMapChangesList;
        String subMapKey;

        // add self AT THE BEGINNING to the changes so we merge on top of ourself
        subsequentChanges.add(0, this);

        boolean isImage = false;
        boolean newPutEntriesSizeGreaterThan0;
        boolean newRemovedEntriesSizeGreaterThan0;
        // process the changes in order, building up an aggregated atomic change
        IRecordChange subsequentChange;
        for (int i = 0; i < subsequentChanges.size(); i++)
        {
            subsequentChange = subsequentChanges.get(i);

            if (subsequentChange == NULL_CHANGE || subsequentChange == null)
            {
                continue;
            }

            if (!isImage)
            {
                isImage = subsequentChange.getScope() == IRecordChange.IMAGE_SCOPE_CHAR;
            }

            if (subsequentChange instanceof AtomicChange)
            {
                newPutEntries = ((AtomicChange) subsequentChange).internalGetPutEntries();
                newOverwrittenEntries = ((AtomicChange) subsequentChange).internalGetOverwrittenEntries();
                newRemovedEntries = ((AtomicChange) subsequentChange).internalGetRemovedEntries();
            }
            else
            {
                newPutEntries = subsequentChange.getPutEntries();
                newOverwrittenEntries = subsequentChange.getOverwrittenEntries();
                newRemovedEntries = subsequentChange.getRemovedEntries();
            }

            newPutEntriesSizeGreaterThan0 = newPutEntries.size() > 0;
            newRemovedEntriesSizeGreaterThan0 = newRemovedEntries.size() > 0;

            // NOTE: it is not possible to optimise this by grouping by the put/remove size > 0
            // checks - the order of adding/removing must be maintained to ensure the
            // ultimatelyAdded/Removed keys are correct
            if (newPutEntriesSizeGreaterThan0)
            {
                putEntries.putAll(newPutEntries);
            }
            if (newOverwrittenEntries.size() > 0)
            {
                overwrittenEntries.putAll(newOverwrittenEntries);
            }
            if (newRemovedEntriesSizeGreaterThan0)
            {
                removedEntries.putAll(newRemovedEntries);
            }

            if (newPutEntriesSizeGreaterThan0)
            {
                ultimatelyAddedKeys.addAll(newPutEntries.keySet());
            }
            if (newRemovedEntriesSizeGreaterThan0)
            {
                ultimatelyAddedKeys.removeAll(newRemovedEntries.keySet());
            }

            if (newRemovedEntriesSizeGreaterThan0)
            {
                ultimatelyRemovedKeys.addAll(newRemovedEntries.keySet());
            }
            if (newPutEntriesSizeGreaterThan0)
            {
                ultimatelyRemovedKeys.removeAll(newPutEntries.keySet());
            }

            // build up the map of the list of sub-map changes, keyed by sub-map key
            // this VASTLY improves performance of merging
            subMapKeysToMerge = subsequentChange.getSubMapKeys();
            if (subMapKeysToMerge.size() > 0)
            {
                if (subMapChangesToMerge == null)
                {
                    subMapChangesToMerge = newMap();
                }
                for (subMapKeysToMergeIterator = subMapKeysToMerge.iterator(); subMapKeysToMergeIterator.hasNext();)
                {
                    subMapKey = subMapKeysToMergeIterator.next();
                    subMapChangesList = subMapChangesToMerge.get(subMapKey);
                    if (subMapChangesList == null)
                    {
                        subMapChangesList = new ArrayList<>(1);
                        subMapChangesToMerge.put(subMapKey, subMapChangesList);
                    }
                    subMapChangesList.add(subsequentChange.getSubMapAtomicChange(subMapKey));
                }
            }
        }

        // determine what keys were ultimately added - remove them from the removedEntries
        if (ultimatelyRemovedKeys.size() > 0)
        {
            ultimatelyAddedKeys.removeAll(ultimatelyRemovedKeys);
            // remove any puts/overwritten that were ultimately removed
            for (String removedKey : ultimatelyRemovedKeys)
            {
                putEntries.remove(removedKey);
                overwrittenEntries.remove(removedKey);
            }
        }
        if (ultimatelyAddedKeys.size() > 0)
        {
            for (String addedKey : ultimatelyAddedKeys)
            {
                removedEntries.remove(addedKey);
            }
        }

        synchronized (this)
        {
            this.putEntries = putEntries;
            this.overwrittenEntries = overwrittenEntries;
            this.removedEntries = removedEntries;
        }

        setScope(isImage ? IRecordChange.IMAGE_SCOPE_CHAR : IRecordChange.DELTA_SCOPE_CHAR);

        // only need to set the sequence from the last one (they are in order)
        setSequence(subsequentChanges.get(subsequentChanges.size() - 1).getSequence());

        // now coalesce the sub-maps in each list per sub-map key
        if (subMapChangesToMerge != null && subMapChangesToMerge.size() > 0)
        {
            Map.Entry<String, List<IRecordChange>> entry = null;
            for (Iterator<Map.Entry<String, List<IRecordChange>>> it =
                subMapChangesToMerge.entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                internalGetSubMapAtomicChange(entry.getKey()).coalesce(entry.getValue());
            }
        }
    }

    @Override
    public void setScope(char scope)
    {
        this.scope.set(Character.valueOf(scope));
    }

    @Override
    public void setSequence(long sequence)
    {
        this.sequence.set(Long.valueOf(sequence));
    }

    @Override
    public char getScope()
    {
        return this.scope.get().charValue();
    }

    @Override
    public long getSequence()
    {
        return this.sequence.get().longValue();
    }

    void mergeBulkChanges(ThreadLocalBulkChanges changes)
    {
        final Map<String, IValue> internalPutEntries = internalGetPutEntries();
        final Map<String, IValue> internalOverwrittenEntries = internalGetOverwrittenEntries();
        final Map<String, IValue> internalRemovedEntries = internalGetRemovedEntries();

        for (int i = 0; i < changes.putSize; i++)
        {
            internalPutEntries.put(changes.putKeys[i], changes.putValues[i][0]);
            if (changes.putValues[i][1] != null)
            {
                internalOverwrittenEntries.put(changes.putKeys[i], changes.putValues[i][1]);
            }
            // VERY IMPORTANT: when adding a field, if the atomic change has not been completed,
            // a put MUST overrule any previous remove, otherwise the atomic change has a put +
            // remove which can cause problems if the put vs removes are applied in different
            // orders
            internalRemovedEntries.remove(changes.putKeys[i]);
        }

        // now do removes
        for (int i = 0; i < changes.removedSize; i++)
        {
            internalPutEntries.remove(changes.removedKeys[i]);
            internalOverwrittenEntries.remove(changes.removedKeys[i]);
            internalRemovedEntries.put(changes.removedKeys[i], changes.removedValues[i]);
        }
    }

    void mergeBulkSubMapChanges(String subMapKey, ThreadLocalBulkChanges changes)
    {
        internalGetSubMapAtomicChange(subMapKey).mergeBulkChanges(changes);
    }

    void mergeEntryUpdatedChange(String key, IValue current, IValue previous)
    {
        internalGetPutEntries().put(key, current);
        if (previous != null)
        {
            internalGetOverwrittenEntries().put(key, previous);
        }
        // VERY IMPORTANT: when adding a field, if the atomic change has not been completed, a put
        // MUST overrule any previous remove, otherwise the atomic change has a put + remove which
        // can cause problems if the put vs removes are applied in different orders
        internalGetRemovedEntries().remove(key);
    }

    void addEntry_onlyCallFromCodec(String key, IValue current)
    {
        this.putEntries.put(key, current);
    }

    void mergeEntryRemovedChange(String key, IValue value)
    {
        internalGetPutEntries().remove(key);
        internalGetOverwrittenEntries().remove(key);
        internalGetRemovedEntries().put(key, value);
    }

    void removeEntry_onlyCallFromCodec(String key, IValue value)
    {
        this.removedEntries.put(key, value);
    }

    void mergeSubMapEntryUpdatedChange(String subMapKey, String key, IValue current, IValue previous)
    {
        internalGetSubMapAtomicChange(subMapKey).mergeEntryUpdatedChange(key, current, previous);
    }

    void mergeSubMapEntryRemovedChange(String subMapKey, String key, IValue value)
    {
        internalGetSubMapAtomicChange(subMapKey).mergeEntryRemovedChange(key, value);
    }

    Map<String, IValue> internalGetPutEntries()
    {
        if (this.putEntries != null)
        {
            return this.putEntries;
        }
        synchronized (this)
        {
            if (this.putEntries == null)
            {
                this.putEntries = newMap();
            }
            return this.putEntries;
        }
    }

    Map<String, IValue> internalGetRemovedEntries()
    {
        if (this.removedEntries != null)
        {
            return this.removedEntries;
        }
        synchronized (this)
        {
            if (this.removedEntries == null)
            {
                this.removedEntries = newMap();
            }
            return this.removedEntries;
        }
    }

    Map<String, IValue> internalGetOverwrittenEntries()
    {
        if (this.overwrittenEntries != null)
        {
            return this.overwrittenEntries;
        }
        synchronized (this)
        {
            if (this.overwrittenEntries == null)
            {
                this.overwrittenEntries = newMap();
            }
            return this.overwrittenEntries;
        }
    }

    AtomicChange internalGetSubMapAtomicChange(String subMapKey)
    {
        synchronized (this)
        {
            if (this.subMapAtomicChanges == null)
            {
                this.subMapAtomicChanges = newMap();
                this.subMapKeys = Collections.unmodifiableSet(this.subMapAtomicChanges.keySet());
            }
            AtomicChange subMapAtomicChange = this.subMapAtomicChanges.get(subMapKey);
            if (subMapAtomicChange == null)
            {
                subMapAtomicChange = new AtomicChange(subMapKey);
                subMapAtomicChange.scope = this.scope;
                subMapAtomicChange.sequence = this.sequence;
                this.subMapAtomicChanges.put(subMapKey, subMapAtomicChange);
            }
            return subMapAtomicChange;
        }
    }

    @Override
    public Set<String> getSubMapKeys()
    {
        if (this.subMapAtomicChanges != null)
        {
            return this.subMapKeys;
        }
        else
        {
            return ContextUtils.EMPTY_STRING_SET;
        }
    }

    Set<String> internalGetSubMapKeys()
    {
        if (this.subMapAtomicChanges != null)
        {
            return this.subMapAtomicChanges.keySet();
        }
        else
        {
            return ContextUtils.EMPTY_STRING_SET;
        }
    }

    @Override
    public IRecordChange getSubMapAtomicChange(String subMapKey)
    {
        if (this.subMapAtomicChanges != null)
        {
            final IRecordChange subMapAtomicChange = this.subMapAtomicChanges.get(subMapKey);
            if (subMapAtomicChange != null)
            {
                return subMapAtomicChange;
            }
        }
        return NULL_CHANGE;
    }

    @Override
    public void applyTo(Map<String, IValue> target)
    {
        if (this.removedEntries != null)
        {
            for (String objectName : this.removedEntries.keySet())
            {
                target.remove(objectName);
            }
        }
        if (this.putEntries != null)
        {
            target.putAll(this.putEntries);
        }
    }

    @Override
    public void applyCompleteAtomicChangeToRecord(IRecord record)
    {
        synchronized (record.getWriteLock())
        {
            // user code should not be able to set sequences, hence the instance-of check
            if (record instanceof Record)
            {
                ((Record) record).setSequence(this.sequence.get().longValue());
            }

            applyTo(record);

            if (this.subMapAtomicChanges != null)
            {
                Map<String, IValue> subMap;
                for (String subMapKey : this.subMapAtomicChanges.keySet())
                {
                    subMap = record.getOrCreateSubMap(subMapKey);
                    getSubMapAtomicChange(subMapKey).applyTo(subMap);
                    if (subMap.size() == 0)
                    {
                        record.removeSubMap(subMapKey);
                    }
                }
            }
        }
    }

    private boolean noRemovedEntries()
    {
        return this.removedEntries == null || this.removedEntries.isEmpty();
    }

    private boolean noPutEntries()
    {
        return this.putEntries == null || this.putEntries.isEmpty();
    }

    private boolean noOverwrittenEntries()
    {
        return this.overwrittenEntries == null || this.overwrittenEntries.isEmpty();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.name == null) ? 0 : this.name.hashCode());
        result = prime * result + ((this.scope.get() == null) ? 0 : this.scope.get().hashCode());
        result = prime * result + ((this.sequence.get() == null) ? 0 : this.sequence.get().hashCode());
        return result;
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
        final AtomicChange other = (AtomicChange) obj;
        return is.eq(this.name, other.name) && is.eq(this.scope.get(), other.scope.get())
            && is.eq(this.sequence.get(), other.sequence.get()) && is.eq(this.putEntries, other.putEntries)
            && is.eq(this.removedEntries, other.removedEntries)
            && is.eq(this.subMapAtomicChanges, other.subMapAtomicChanges);
    }
}

/**
 * Utility to hold put and remove changes in bijectional arrays
 * 
 * @author Ramon Servadei
 */
final class ThreadLocalBulkChanges
{
    static final ThreadLocal<ThreadLocalBulkChanges> THREAD_LOCAL = new ThreadLocal<ThreadLocalBulkChanges>()
    {
        @SuppressWarnings("synthetic-access")
        @Override
        protected ThreadLocalBulkChanges initialValue()
        {
            return new ThreadLocalBulkChanges();
        }
    };

    static ThreadLocalBulkChanges get()
    {
        return THREAD_LOCAL.get();
    }

    String[] putKeys;
    IValue[][] putValues;
    String[] removedKeys;
    IValue[] removedValues;
    int putSize;
    int removedSize;

    private ThreadLocalBulkChanges()
    {
        this.putKeys = new String[4];
        this.putValues = new IValue[4][2];
        this.removedKeys = new String[4];
        this.removedValues = new IValue[4];
    }

    ThreadLocalBulkChanges initialise(int size)
    {
        if (this.putKeys.length < size)
        {
            this.putKeys = new String[size];
            this.putValues = new IValue[size][2];
            this.removedKeys = new String[size];
            this.removedValues = new IValue[size];
        }
        else
        {
            int i;
            for (i = 0; i < this.putSize; i++)
            {
                this.putKeys[i] = null;
                this.putValues[i][0] = null;
                this.putValues[i][1] = null;
            }
            for (i = 0; i < this.removedSize; i++)
            {
                this.removedKeys[i] = null;
                this.removedValues[i] = null;
            }
        }

        // marked to zero
        this.putSize = 0;
        this.removedSize = 0;

        return this;
    }
}