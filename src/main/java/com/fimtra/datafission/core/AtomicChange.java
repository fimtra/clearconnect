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

import static com.fimtra.util.CollectionUtils.newMap;
import static com.fimtra.util.CollectionUtils.newSet;
import static com.fimtra.util.CollectionUtils.noopMap;

import java.util.ArrayList;
import java.util.Collections;
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
import com.fimtra.util.CharRef;
import com.fimtra.util.LongRef;
import com.fimtra.util.is;

/**
 * Represents an atomic change for a single named record.
 * 
 * @author Ramon Servadei
 */
public final class AtomicChange implements IRecordChange, ISequentialRunnable
{
    private static final long SEQ_INIT = -1L;

    private static final Map<String, IValue> EMPTY_MAP = Collections.unmodifiableMap(newMap(0));
    private static final Map<String, IValue> NOOP_MAP = noopMap();

    private static final IRecordChange NULL_CHANGE = new IRecordChange()
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

    final String name;
    final CharRef scope;
    final LongRef sequence;

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
        this(image.getName(), new CharRef(IMAGE_SCOPE_CHAR), new LongRef(image.getSequence()));
        synchronized (image.getWriteLock())
        {
            if (!image.isEmpty())
            {
                internalGetPutEntries().putAll(image);
            }
            for (String subMapKey : image.getSubMapKeys())
            {
                internalGetSubMapAtomicChange(subMapKey).internalGetPutEntries().putAll(
                        image.getOrCreateSubMap(subMapKey));
            }
        }
    }

    public AtomicChange(String name, Map<String, IValue> putEntries, Map<String, IValue> overwrittenEntries,
            Map<String, IValue> removedEntries)
    {
        this(name, putEntries, overwrittenEntries, removedEntries, new CharRef(DELTA_SCOPE_CHAR),
                new LongRef(SEQ_INIT));
    }

    AtomicChange(String name)
    {
        this(name, null, null, null);
    }

    AtomicChange(String name, CharRef scope, LongRef sequence)
    {
        this(name, null, null, null, scope, sequence);
    }

    private AtomicChange(String name, Map<String, IValue> putEntries, Map<String, IValue> overwrittenEntries,
            Map<String, IValue> removedEntries, CharRef scope, LongRef sequence)
    {
        super();
        this.name = name;
        this.putEntries = putEntries;
        this.overwrittenEntries = overwrittenEntries;
        this.removedEntries = removedEntries;
        this.scope = scope;
        this.sequence = sequence;
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
            this.context.doPublishChange(this);
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
        return Collections.unmodifiableMap(putEntries);
    }

    @Override
    public Map<String, IValue> getOverwrittenEntries()
    {
        if (this.overwrittenEntries == null)
        {
            return EMPTY_MAP;
        }
        return Collections.unmodifiableMap(overwrittenEntries);
    }

    @Override
    public Map<String, IValue> getRemovedEntries()
    {
        if (this.removedEntries == null)
        {
            return EMPTY_MAP;
        }
        return Collections.unmodifiableMap(removedEntries);
    }

    @Override
    public boolean isEmpty()
    {
        if (!(putEntries == null || putEntries.isEmpty()))
        {
            return false;
        }
        if (!(removedEntries == null || removedEntries.isEmpty()))
        {
            return false;
        }
        // check submaps
        if (this.subMapAtomicChanges != null)
        {
            for (Map.Entry<String, AtomicChange> entry : subMapAtomicChanges.entrySet())
            {
                if (!entry.getValue()
                        .isEmpty())
                {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int getSize()
    {
        int size = this.putEntries == null ? 0 : this.putEntries.size();
        size += this.removedEntries == null ? 0 : this.removedEntries.size();

        if (this.subMapAtomicChanges != null)
        {
            AtomicChange value;
            for (Map.Entry<String, AtomicChange> entry : this.subMapAtomicChanges.entrySet())
            {
                value = entry.getValue();
                size += value.putEntries == null ? 0 : value.putEntries.size();
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
        Map<String, IValue> putEntries = null;
        Map<String, IValue> overwrittenEntries = null;
        Map<String, IValue> removedEntries = null;
        Set<String> ultimatelyRemovedKeys = null;
        Set<String> ultimatelyAddedKeys = null;
        Map<String, IValue> newPutEntries;
        Map<String, IValue> newOverwrittenEntries;
        Map<String, IValue> newRemovedEntries;

        // sub-map vars
        Set<String> subMapKeysToMerge;
        Iterator<String> subMapKeysToMergeIterator;
        Map<String, List<IRecordChange>> subMapChangesToMerge = null;

        String subMapKey;

        // add self AT THE BEGINNING to the changes so we merge on top of ourself
        subsequentChanges.add(0, this);

        boolean isImage = false;
        boolean newPutEntriesExist;
        // process the changes in order, building up an aggregated atomic change
        for (IRecordChange subsequentChange : subsequentChanges)
        {
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
                // on-demand assignment of collections
                final AtomicChange atomicChange = (AtomicChange) subsequentChange;
                newPutEntries = atomicChange.putEntries == null ? NOOP_MAP : atomicChange.putEntries;
                newOverwrittenEntries =
                        atomicChange.overwrittenEntries == null ? NOOP_MAP : atomicChange.overwrittenEntries;
                newRemovedEntries =
                        atomicChange.removedEntries == null ? NOOP_MAP : atomicChange.removedEntries;
            }
            else
            {
                newPutEntries = subsequentChange.getPutEntries();
                newOverwrittenEntries = subsequentChange.getOverwrittenEntries();
                newRemovedEntries = subsequentChange.getRemovedEntries();
            }

            newPutEntriesExist = !newPutEntries.isEmpty();

            // NOTE: it is NOT possible to optimise this by grouping all the put/remove size > 0 checks as
            //       the order of adding/removing must be maintained to ensure the ultimatelyAdded/Removed
            //       keys are correct
            if (newPutEntriesExist)
            {
                // on-demand assignment of collections
                (putEntries == null ? (putEntries = newMap()) : putEntries).putAll(newPutEntries);
                (ultimatelyAddedKeys == null ? (ultimatelyAddedKeys = newSet()) : ultimatelyAddedKeys).addAll(
                        newPutEntries.keySet());

                // overwritten entries cannot exist if there is no put so we only check for new put entries
                if (!newOverwrittenEntries.isEmpty())
                {
                    (overwrittenEntries == null ? (overwrittenEntries = newMap()) : overwrittenEntries).putAll(
                            newOverwrittenEntries);
                }
            }
            // handle any newly removed entries
            if (!newRemovedEntries.isEmpty())
            {
                // on-demand assignment of collections
                (removedEntries == null ? (removedEntries = newMap()) : removedEntries).putAll(
                        newRemovedEntries);
                (ultimatelyRemovedKeys == null ? (ultimatelyRemovedKeys = newSet()) :
                        ultimatelyRemovedKeys).addAll(newRemovedEntries.keySet());

                // remove new removed entries from ultimatelyAddedKeys
                if (ultimatelyAddedKeys != null)
                {
                    ultimatelyAddedKeys.removeAll(newRemovedEntries.keySet());
                }
            }
            // remove new put entries from ultimatelyRemovedKeys - MUST do this AFTER checking new removes
            if (newPutEntriesExist)
            {
                if (ultimatelyRemovedKeys != null)
                {
                    ultimatelyRemovedKeys.removeAll(newPutEntries.keySet());
                }
            }

            // build up the map of the list of sub-map changes, keyed by sub-map key
            // this VASTLY improves performance of merging
            subMapKeysToMerge = subsequentChange.getSubMapKeys();
            if (!subMapKeysToMerge.isEmpty())
            {
                if (subMapChangesToMerge == null)
                {
                    subMapChangesToMerge = newMap();
                }
                for (subMapKeysToMergeIterator =
                             subMapKeysToMerge.iterator(); subMapKeysToMergeIterator.hasNext(); )
                {
                    subMapKey = subMapKeysToMergeIterator.next();
                    subMapChangesToMerge.computeIfAbsent(subMapKey, k -> new ArrayList<>(1)).add(
                            subsequentChange.getSubMapAtomicChange(subMapKey));
                }
            }
        }

        // determine what keys were ultimately added - remove them from the removedEntries
        if (ultimatelyAddedKeys != null )
        {
            if (ultimatelyRemovedKeys != null)
            {
                ultimatelyAddedKeys.removeAll(ultimatelyRemovedKeys);

                // remove any puts/overwritten that were ultimately removed
                for (String removedKey : ultimatelyRemovedKeys)
                {
                    putEntries.remove(removedKey);
                }
                if (overwrittenEntries != null)
                {
                    for (String removedKey : ultimatelyRemovedKeys)
                    {
                        overwrittenEntries.remove(removedKey);
                    }
                }
            }
            if (removedEntries != null)
            {
                for (String addedKey : ultimatelyAddedKeys)
                {
                    removedEntries.remove(addedKey);
                }
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
        if (subMapChangesToMerge != null && !subMapChangesToMerge.isEmpty())
        {
            for (Map.Entry<String, List<IRecordChange>> entry : subMapChangesToMerge.entrySet())
            {
                internalGetSubMapAtomicChange(entry.getKey()).coalesce(entry.getValue());
            }
        }
    }

    @Override
    public void setScope(char scope)
    {
        this.scope.set(scope);
    }

    @Override
    public void setSequence(long sequence)
    {
        this.sequence.set(sequence);
    }

    @Override
    public char getScope()
    {
        return this.scope.get();
    }

    @Override
    public long getSequence()
    {
        return this.sequence.get();
    }

    void mergeBulkChanges(ThreadLocalBulkChanges changes)
    {
        synchronized (this)
        {
            // on-demand assignment of collections
            final Map<String, IValue> _putEntries = noPutEntries() ?
                    (changes.putSize > 0 ? (putEntries = newMap()) : NOOP_MAP) : putEntries;
            final Map<String, IValue> _removedEntries = noRemovedEntries() ?
                    (changes.removedSize > 0 ? (removedEntries = newMap()) : NOOP_MAP) :
                    removedEntries;

            for (int i = 0; i < changes.putSize; i++)
            {
                _putEntries.put(changes.putKeys[i], changes.putValues[i][0]);
                if (changes.putValues[i][1] != null)
                {
                    if (overwrittenEntries == null)
                    {
                        overwrittenEntries = newMap();
                    }
                    overwrittenEntries.put(changes.putKeys[i], changes.putValues[i][1]);
                }
                // VERY IMPORTANT: when adding a field, if the atomic change has not been completed,
                // a put MUST overrule any previous remove, otherwise the atomic change has a put +
                // remove which can cause problems if the put vs removes are applied in different
                // orders
                _removedEntries.remove(changes.putKeys[i]);
            }

            // now do removes
            final Map<String, IValue> _overwrittenEntries =
                    noOverwrittenEntries() ? NOOP_MAP : overwrittenEntries;
            for (int i = 0; i < changes.removedSize; i++)
            {
                _putEntries.remove(changes.removedKeys[i]);
                _overwrittenEntries.remove(changes.removedKeys[i]);
                _removedEntries.put(changes.removedKeys[i], changes.removedValues[i]);
            }
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
        if (removedEntries != null)
        {
            removedEntries.remove(key);
        }
    }

    void mergeEntryRemovedChange(String key, IValue value)
    {
        if (putEntries != null)
        {
            putEntries.remove(key);
        }
        if (overwrittenEntries != null)
        {
            overwrittenEntries.remove(key);
        }
        // putting needs the map to exist!
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
        synchronized (this)
        {
            return (putEntries == null ? (putEntries = newMap()) : putEntries);
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
            return (removedEntries == null ? (removedEntries = newMap()) : removedEntries);
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
            return (overwrittenEntries == null ? (overwrittenEntries = newMap()) : overwrittenEntries);
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
            return this.subMapAtomicChanges.computeIfAbsent(subMapKey,
                    k -> new AtomicChange(k, this.scope, this.sequence));
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
                ((Record) record).setSequence(this.sequence.get());
            }

            applyTo(record);

            if (this.subMapAtomicChanges != null)
            {
                Map<String, IValue> subMap;
                for (String subMapKey : this.subMapAtomicChanges.keySet())
                {
                    subMap = record.getOrCreateSubMap(subMapKey);
                    getSubMapAtomicChange(subMapKey).applyTo(subMap);
                    if (subMap.isEmpty())
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
        result = prime * result + ((this.scope == null) ? 0 : this.scope.hashCode());
        result = prime * result + ((this.sequence == null) ? 0 : this.sequence.hashCode());
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
    static final ThreadLocal<ThreadLocalBulkChanges> THREAD_LOCAL =
            ThreadLocal.withInitial(ThreadLocalBulkChanges::new);

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