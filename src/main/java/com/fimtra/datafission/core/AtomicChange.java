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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.util.is;

/**
 * Represents an atomic change for a single named record.
 * 
 * @author Ramon Servadei
 */
public final class AtomicChange implements IRecordChange
{
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
            return DELTA_SCOPE.charValue();
        }

        @Override
        public long getSequence()
        {
            return -1;
        }
    };

    final String name;
    AtomicReference<Character> scope = new AtomicReference<Character>(DELTA_SCOPE);
    AtomicReference<Long> sequence = new AtomicReference<Long>(Long.valueOf(-1));

    Map<String, IValue> putEntries;
    Map<String, IValue> overwrittenEntries;
    Map<String, IValue> removedEntries;
    Map<String, AtomicChange> subMapAtomicChanges;

    final Lock lock;

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
        
        this.lock = new ReentrantLock();
    }

    AtomicChange(String name)
    {
        this(name, null, null, null);
    }

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
        final Map<String, IValue> putEntries = new HashMap<String, IValue>();
        final Map<String, IValue> overwrittenEntries = new HashMap<String, IValue>();
        final Map<String, IValue> removedEntries = new HashMap<String, IValue>();
        final Set<String> ultimatelyRemovedKeys = new HashSet<String>();
        final Set<String> ultimatelyAddedKeys = new HashSet<String>();
        Map<String, IValue> newPutEntries;
        Map<String, IValue> newOverwrittenEntries;
        Map<String, IValue> newRemovedEntries;

        // sub-map vars
        Set<String> subMapKeysToMerge;
        Iterator<String> subMapKeysToMergeIterator;
        final Map<String, List<IRecordChange>> subMapChangesToMerge = new HashMap<String, List<IRecordChange>>();
        List<IRecordChange> subMapChangesList;
        String subMapKey;

        // add self AT THE BEGINNING to the changes so we merge on top of ourself
        subsequentChanges.add(0, this);

        // process the changes in order, building up an aggregated atomic change
        IRecordChange subsequentChange;
        for (int i = 0; i < subsequentChanges.size(); i++)
        {
            subsequentChange = subsequentChanges.get(i);

            if (subsequentChange == NULL_CHANGE || subsequentChange == null)
            {
                continue;
            }

            newPutEntries = subsequentChange.getPutEntries();
            newOverwrittenEntries = subsequentChange.getOverwrittenEntries();
            newRemovedEntries = subsequentChange.getRemovedEntries();

            putEntries.putAll(newPutEntries);
            overwrittenEntries.putAll(newOverwrittenEntries);
            removedEntries.putAll(newRemovedEntries);

            ultimatelyAddedKeys.addAll(newPutEntries.keySet());
            ultimatelyAddedKeys.removeAll(newRemovedEntries.keySet());

            ultimatelyRemovedKeys.addAll(newRemovedEntries.keySet());
            ultimatelyRemovedKeys.removeAll(newPutEntries.keySet());

            // build up the map of the list of sub-map changes, keyed by sub-map key
            // this VASTLY improves performance of merging
            subMapKeysToMerge = subsequentChange.getSubMapKeys();
            if (subMapKeysToMerge.size() > 0)
            {
                for (subMapKeysToMergeIterator = subMapKeysToMerge.iterator(); subMapKeysToMergeIterator.hasNext();)
                {
                    subMapKey = subMapKeysToMergeIterator.next();
                    subMapChangesList = subMapChangesToMerge.get(subMapKey);
                    if (subMapChangesList == null)
                    {
                        subMapChangesList = new ArrayList<IRecordChange>(1);
                        subMapChangesToMerge.put(subMapKey, subMapChangesList);
                    }
                    subMapChangesList.add(subsequentChange.getSubMapAtomicChange(subMapKey));
                }
            }
        }

        // determine what keys were ultimately added - remove them from the removedEntries
        ultimatelyAddedKeys.removeAll(ultimatelyRemovedKeys);
        for (String addedKey : ultimatelyAddedKeys)
        {
            removedEntries.remove(addedKey);
        }

        // remove any puts/overwritten that were ultimately removed
        for (String removedKey : ultimatelyRemovedKeys)
        {
            putEntries.remove(removedKey);
            overwrittenEntries.remove(removedKey);
        }

        final Lock lock = getLock();
        lock.lock();
        try
        {
            this.putEntries = putEntries;
            this.overwrittenEntries = overwrittenEntries;
            this.removedEntries = removedEntries;
        }
        finally
        {
            lock.unlock();
        }

        // only need to set the scope/sequence from the last one (they are in order)
        final IRecordChange lastChange = subsequentChanges.get(subsequentChanges.size() - 1);
        setScope(lastChange.getScope());
        setSequence(lastChange.getSequence());

        // now coalesce the sub-maps in each list per sub-map key
        if (subMapChangesToMerge.size() > 0)
        {
            Map.Entry<String, List<IRecordChange>> entry = null;
            for (Iterator<Map.Entry<String, List<IRecordChange>>> it = subMapChangesToMerge.entrySet().iterator(); it.hasNext();)
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

    Lock getLock()
    {
        return this.lock;
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

    void mergeEntryRemovedChange(String key, IValue value)
    {
        internalGetPutEntries().remove(key);
        internalGetOverwrittenEntries().remove(key);
        internalGetRemovedEntries().put(key, value);
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
        final Lock lock = getLock();
        lock.lock();
        try
        {
            if (this.putEntries == null)
            {
                this.putEntries = new HashMap<String, IValue>(2);
            }
            return this.putEntries;
        }
        finally
        {
            lock.unlock();
        }
    }

    Map<String, IValue> internalGetRemovedEntries()
    {
        if (this.removedEntries != null)
        {
            return this.removedEntries;
        }
        final Lock lock = getLock();
        lock.lock();
        try
        {
            if (this.removedEntries == null)
            {
                this.removedEntries = new HashMap<String, IValue>(2);
            }
            return this.removedEntries;
        }
        finally
        {
            lock.unlock();
        }
    }

    Map<String, IValue> internalGetOverwrittenEntries()
    {
        if (this.overwrittenEntries != null)
        {
            return this.overwrittenEntries;
        }
        final Lock lock = getLock();
        lock.lock();
        try
        {
            if (this.overwrittenEntries == null)
            {
                this.overwrittenEntries = new HashMap<String, IValue>(2);
            }
            return this.overwrittenEntries;
        }
        finally
        {
            lock.unlock();
        }
    }

    AtomicChange internalGetSubMapAtomicChange(String subMapKey)
    {
        Lock lock = getLock();
        lock.lock();
        try
        {
            if (this.subMapAtomicChanges == null)
            {
                this.subMapAtomicChanges = new HashMap<String, AtomicChange>(1);
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
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public Set<String> getSubMapKeys()
    {
        if (this.subMapAtomicChanges != null)
        {
            return Collections.unmodifiableSet(new HashSet<String>(this.subMapAtomicChanges.keySet()));
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
        Set<String> removed = getRemovedEntries().keySet();
        for (String objectName : removed)
        {
            target.remove(objectName);
        }
        target.putAll(getPutEntries());
    }

    @Override
    public void applyCompleteAtomicChangeToRecord(IRecord record)
    {
        // user code should not be able to set sequences, hence the instance-of check
        if (record instanceof Record)
        {
            ((Record) record).setSequence(this.sequence.get().longValue());
        }
        applyTo(record);
        Map<String, IValue> subMap;
        for (String subMapKey : getSubMapKeys())
        {
            subMap = record.getOrCreateSubMap(subMapKey);
            getSubMapAtomicChange(subMapKey).applyTo(subMap);
            if (subMap.size() == 0)
            {
                record.removeSubMap(subMapKey);
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