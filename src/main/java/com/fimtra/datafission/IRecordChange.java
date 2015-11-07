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
package com.fimtra.datafission;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents an atomic change for a single named record. There are 3 sets of changes in an atomic
 * change that encapsulate what happened in the change. Each change is held in a separate map:
 * <ul>
 * <li>PUT - these are fields that are added or changed:
 * <ul>
 * <li>If the field is added (i.e. it is new), the value appears in this map and it will <b>NOT</b>
 * have an entry in the overwritten map.
 * <li>If the field is updated, the new value for the field appears in this map and the previous
 * value appears in the overwritten map.
 * </ul>
 * <li>OVERWRITTEN - these are the fields in the record that have changed, this map will hold the
 * field name and the <b>previous</b> value associated with the field
 * <li>REMOVED - these are fields that have been removed from the record, this map will hold the
 * removed field name and value associated with the removed field. <br>
 * <b>A field that is removed will NEVER appear in the PUT or OVERWRITTEN maps.</b>
 * </ul>
 * 
 * @author Ramon Servadei
 */
public interface IRecordChange
{
    Character DELTA_SCOPE = Character.valueOf('d');
    Character IMAGE_SCOPE = Character.valueOf('i');

    /**
     * @return the name of the record that the change is for
     */
    String getName();

    /**
     * @return the entries that were added or updated in this atomic change. If the field is added
     *         (i.e. it is new), the value appears in this map and it will <b>NOT</b> have an entry
     *         in the {@link #getOverwrittenEntries()} map.<br>
     *         If the field is updated, the new value for the field appears in this map and the
     *         previous value appears in the {@link #getOverwrittenEntries()} map.
     */
    Map<String, IValue> getPutEntries();

    /**
     * @return the entries that were overwritten in this atomic change.
     * @see #getPutEntries
     */
    Map<String, IValue> getOverwrittenEntries();

    /**
     * @return the entries that were removed in this atomic change
     */
    Map<String, IValue> getRemovedEntries();

    /** @return <code>true</code> if the change is empty (there are no changes) */
    boolean isEmpty();

    /**
     * Get the keys of any sub-maps that have an atomic change.
     * 
     * @see #getSubMapAtomicChange(String)
     * @return the set of sub-map keys for sub-maps that have a change as part of this atomic change
     */
    Set<String> getSubMapKeys();

    /**
     * Get the atomic change for the sub-map
     * 
     * @param subMapKey
     *            the key of the sub-map
     * @return the atomic change for the sub-map, <code>null</code> if there is no change for this
     *         sub-map in this atomic change
     */
    IRecordChange getSubMapAtomicChange(String subMapKey);

    /**
     * Apply the atomic change (excluding sub-map atomic changes) to the target map
     */
    void applyTo(Map<String, IValue> target);

    /**
     * Apply the complete atomic change (including sub-map atomic changes) to the target record
     */
    void applyCompleteAtomicChangeToRecord(IRecord record);

    /**
     * Merge the changes in the list, in order, with this change. The merging assumes the list is
     * ordered from oldest to earliest change AND that <code>this</code> instance is the oldest
     * change.
     * 
     * @param subsequentChanges
     *            atomic changes occurring after <code>this</code> instance.
     */
    void coalesce(List<IRecordChange> subsequentChanges);

    /**
     * Set the scope of the atomic change, by default scope is 'delta'
     * 
     * @see #DELTA_SCOPE
     * @see #IMAGE_SCOPE
     */
    void setScope(char scope);

    /**
     * Set the sequence number for the change. The sequence number is aligned to the record the
     * change is for
     */
    void setSequence(long sequence);

    /**
     * @return a character to indicate the scope of the change - either image or delta
     * @see #DELTA_SCOPE
     * @see #IMAGE_SCOPE
     */
    char getScope();

    /**
     * @return the sequence number of this change for the record
     */
    long getSequence();

}