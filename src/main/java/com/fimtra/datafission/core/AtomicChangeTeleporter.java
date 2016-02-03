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
package com.fimtra.datafission.core;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;

/**
 * Splits a single {@link AtomicChange} into parts for sending and rebuilds the {@link AtomicChange}
 * from the parts at the other end.
 * 
 * @author Ramon Servadei
 */
final class AtomicChangeTeleporter
{
    private static final String PART_INDEX_PREFIX = new String(new char[] { 0xa, 0xb });
    private static final char PART_INDEX_DELIM = 0xc;

    static String getRecordName(String recordName)
    {
        final AtomicReference<String> name = new AtomicReference<String>();
        getNameAndPart(recordName, name, new AtomicInteger());
        if (name.get() == null)
        {
            return recordName;
        }
        return name.get();
    }

    private static void getNameAndPart(String chars, AtomicReference<String> name, AtomicInteger part)
    {
        if (chars.startsWith(PART_INDEX_PREFIX, 0))
        {
            final int index = chars.indexOf(PART_INDEX_DELIM, 2);
            if (index > -1)
            {
                name.set(chars.substring(index + 1));
                try
                {
                    part.set(Integer.parseInt(chars.substring(2, index)));
                }
                catch (NumberFormatException e)
                {
                    // this is not a parsable number, it must be a genuine record name that "looks"
                    // like a part
                    name.set(null);
                }
            }
        }
    }

    /**
     * Enum to help read/write entries from one {@link AtomicChange} to another.
     * 
     * @author Ramon Servadei
     */
    private static enum EntryEnum
    {
            PUT, OVERWRITTEN, REMOVED;

        Map<String, IValue> getEntriesToRead(AtomicChange atomicChange)
        {
            switch(this)
            {
                case PUT:
                    return atomicChange.getPutEntries();
                case OVERWRITTEN:
                    return atomicChange.getOverwrittenEntries();
                case REMOVED:
                    return atomicChange.getRemovedEntries();
            }
            throw new UnsupportedOperationException("No support for " + this);
        }

        Map<String, IValue> getEntriesToWrite(AtomicChange atomicChange)
        {
            switch(this)
            {
                case PUT:
                    return atomicChange.internalGetPutEntries();
                case OVERWRITTEN:
                    return atomicChange.internalGetOverwrittenEntries();
                case REMOVED:
                    return atomicChange.internalGetRemovedEntries();
            }
            throw new UnsupportedOperationException("No support for " + this);
        }
    }

    /**
     * Merge the received part into the source
     */
    private static void merge(AtomicChange source, AtomicChange receivedPart)
    {
        source.scope = receivedPart.scope;
        source.sequence = receivedPart.sequence;

        mergeEntries(EntryEnum.PUT, source, receivedPart, null);
        mergeEntries(EntryEnum.OVERWRITTEN, source, receivedPart, null);
        mergeEntries(EntryEnum.REMOVED, source, receivedPart, null);
        Set<String> subMapKeys = receivedPart.getSubMapKeys();
        if (subMapKeys.size() > 0)
        {
            AtomicChange receivedSubMap;
            for (String key : subMapKeys)
            {
                receivedSubMap = receivedPart.internalGetSubMapAtomicChange(key);
                mergeEntries(EntryEnum.PUT, source, receivedSubMap, key);
                mergeEntries(EntryEnum.OVERWRITTEN, source, receivedSubMap, key);
                mergeEntries(EntryEnum.REMOVED, source, receivedSubMap, key);
            }
        }
    }

    /**
     * Helper for {@link #merge(AtomicChange, AtomicChange)}
     */
    private static void mergeEntries(EntryEnum type, AtomicChange source, AtomicChange receivedPart, String subMapKey)
    {
        if (subMapKey == null)
        {
            Map<String, IValue> entriesToCopy = type.getEntriesToRead(receivedPart);
            if (entriesToCopy.size() > 0)
            {
                type.getEntriesToWrite(source).putAll(entriesToCopy);
            }
        }
        else
        {
            Map<String, IValue> entriesToCopy = type.getEntriesToRead(receivedPart);
            if (entriesToCopy.size() > 0)
            {
                type.getEntriesToWrite(source.internalGetSubMapAtomicChange(subMapKey)).putAll(entriesToCopy);
            }
        }
    }

    /**
     * Count the number of changes in the {@link AtomicChange}
     */
    static int getFieldChangeCount(IRecordChange change, Set<String> subMapKeys)
    {
        int recordLevelChangeCount =
            change.getPutEntries().size() + change.getOverwrittenEntries().size() + change.getRemovedEntries().size();
        if (subMapKeys.size() > 0)
        {
            for (String key : subMapKeys)
            {
                recordLevelChangeCount +=
                    getFieldChangeCount(change.getSubMapAtomicChange(key), ContextUtils.EMPTY_STRING_SET);
            }
        }
        return recordLevelChangeCount;
    }

    /**
     * Write all the entries of the specific type from the source into one of the parts. Effectively
     * splitting the source entries across the parts.
     * 
     * @param type
     *            the type of the entry to process
     * @param name
     *            the name of the atomic change the entries are for
     * @param source
     *            the source that is being split
     * @param parts
     *            the parts of the atomic change
     * @param argPartsIndex
     *            the current index in the parts[]
     * @param counter
     *            the counter incremented for each change written
     * @param maxChangesPerPart
     *            the maximum changes allowed per part in the part[]
     * @param subMapKey
     *            optional, if not-null then the source is a submap of the main atomic change
     * @param totalChanges
     *            the total expected changes
     * @return the index in the parts[] for the next call to this method
     */
    private static int writeEntries(final EntryEnum type, final String name, final AtomicChange source,
        final AtomicChange[] parts, final int argPartsIndex, final AtomicInteger counter, final int maxChangesPerPart,
        final String subMapKey, int totalChanges)
    {
        int partsIndex = argPartsIndex;

        // NOTE: doing the mod here is less expensive than doing it each time within the loop!
        int loopCount = counter.get() % maxChangesPerPart;

        Map<String, IValue> targetEntries = null;
        if (subMapKey != null)
        {
            targetEntries = type.getEntriesToWrite(parts[partsIndex].internalGetSubMapAtomicChange(subMapKey));
        }
        else
        {
            targetEntries = type.getEntriesToWrite(parts[partsIndex]);
        }

        Map.Entry<String, IValue> entry = null;
        for (Iterator<Map.Entry<String, IValue>> it =
            type.getEntriesToRead(source).entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            targetEntries.put(entry.getKey(), entry.getValue());
            loopCount++;

            // work out if we have written enough changes and should start a new part
            if (counter.incrementAndGet() < totalChanges && loopCount == maxChangesPerPart)
            {
                loopCount = 0;
                partsIndex++;
                parts[partsIndex] =
                    new AtomicChange(PART_INDEX_PREFIX + (parts.length - partsIndex) + PART_INDEX_DELIM + name);
                parts[partsIndex].scope = source.scope;
                parts[partsIndex].sequence = source.sequence;

                if (subMapKey != null)
                {
                    targetEntries = type.getEntriesToWrite(parts[partsIndex].internalGetSubMapAtomicChange(subMapKey));
                }
                else
                {
                    targetEntries = type.getEntriesToWrite(parts[partsIndex]);
                }
            }
        }
        return partsIndex;
    }

    final int maxChangesPerPart;
    final ConcurrentMap<String, AtomicChange> receivedParts;

    AtomicChangeTeleporter(int maxChangesPerPart)
    {
        this.maxChangesPerPart = maxChangesPerPart;
        this.receivedParts = new ConcurrentHashMap<String, AtomicChange>();
    }

    /**
     * Split the change into parts
     * 
     * @param change
     *            the change to split
     * @return an array holding the parts for the change
     */
    AtomicChange[] split(AtomicChange change)
    {
        final Set<String> subMapKeys = change.getSubMapKeys();
        final int totalChangeCount = getFieldChangeCount(change, subMapKeys);
        if (totalChangeCount == 0 || totalChangeCount < this.maxChangesPerPart)
        {
            return new AtomicChange[] { change };
        }

        final String name = change.getName();
        final AtomicChange[] parts =
            new AtomicChange[(int) Math.ceil((double) totalChangeCount / this.maxChangesPerPart)];
        int partsIndex = 0;
        final AtomicInteger changeCounter = new AtomicInteger();

        // populate the first element
        parts[partsIndex] = new AtomicChange(PART_INDEX_PREFIX + (parts.length - partsIndex) + PART_INDEX_DELIM + name);
        parts[partsIndex].scope = change.scope;
        parts[partsIndex].sequence = change.sequence;

        partsIndex = writeEntries(EntryEnum.PUT, name, change, parts, partsIndex, changeCounter, this.maxChangesPerPart,
            null, totalChangeCount);
        partsIndex = writeEntries(EntryEnum.OVERWRITTEN, name, change, parts, partsIndex, changeCounter,
            this.maxChangesPerPart, null, totalChangeCount);
        partsIndex = writeEntries(EntryEnum.REMOVED, name, change, parts, partsIndex, changeCounter,
            this.maxChangesPerPart, null, totalChangeCount);

        // now do the submaps
        if (subMapKeys.size() > 0)
        {
            AtomicChange subMapChange;
            for (String key : subMapKeys)
            {
                subMapChange = change.internalGetSubMapAtomicChange(key);
                partsIndex = writeEntries(EntryEnum.PUT, name, subMapChange, parts, partsIndex, changeCounter,
                    this.maxChangesPerPart, key, totalChangeCount);
                partsIndex = writeEntries(EntryEnum.OVERWRITTEN, name, subMapChange, parts, partsIndex, changeCounter,
                    this.maxChangesPerPart, key, totalChangeCount);
                partsIndex = writeEntries(EntryEnum.REMOVED, name, subMapChange, parts, partsIndex, changeCounter,
                    this.maxChangesPerPart, key, totalChangeCount);
            }
        }
        return parts;
    }

    /**
     * @param receivedPart
     *            a received part of an {@link AtomicChange}
     * @return <code>null</code> if the received part was not the final part otherwise the completed
     *         {@link AtomicChange} from all its received parts
     */
    AtomicChange combine(AtomicChange receivedPart)
    {
        System.err.println(receivedPart.getName());
        // todo we need a "sequence" number to match all the parts with - if 2 atomic changes are
        // split into parts and are interleaved, we cannot recombine properly..
        final AtomicReference<String> nameRef = new AtomicReference<String>();
        final AtomicInteger part = new AtomicInteger(Integer.MAX_VALUE);
        getNameAndPart(receivedPart.getName(), nameRef, part);

        final String name = nameRef.get();
        // there was no part so assume its a whole change
        if (name == null)
        {
            return receivedPart;
        }

        AtomicChange atomicChange = new AtomicChange(name);
        final AtomicChange putIfAbsent = this.receivedParts.putIfAbsent(name, atomicChange);
        if (putIfAbsent != null)
        {
            atomicChange = putIfAbsent;
        }
        merge(atomicChange, receivedPart);

        if (part.get() == 1)
        {
            this.receivedParts.remove(name);
            return atomicChange;
        }
        else
        {
            return null;
        }
    }
}