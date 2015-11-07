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

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import com.fimtra.datafission.core.ImmutableSnapshotRecord;

/**
 * A {@link Map} implementation that holds <code>String=IValue</code> entries.
 * <p>
 * A change in a field only occurs if the value associated with the key changes. Updating a value
 * with an object that is equal to the previous value according to its {@link Object#equals(Object)}
 * method does not count as a change. Therefore the equals method for the value objects must be a
 * correct implementation for changes to be detected.
 * <p>
 * <b>Storing a <code>null</code> value against a key is the same as removing the key.</b>
 * <h2>Internal maps</h2> A record can hold maps associated with a key in the record - these are
 * called 'sub-maps'. A sub-map can be used in the same way as using a standard {@link Map}. This is
 * a convenience feature of a record to solve the common requirement for having a map-of-maps (more
 * than a depth of 2 for nested maps is uncommon). See {@link #getOrCreateSubMap(String)} and
 * {@link #getSubMapKeys()}.
 * 
 * @author Ramon Servadei
 */
public interface IRecord extends Map<String, IValue>
{
    /**
     * Get the record's write lock.
     * <p>
     * The record's context will lock this lock when executing
     * {@link IPublisherContext#publishAtomicChange(IRecord)}
     * 
     * @return the record write lock
     */
    Lock getWriteLock();

    /**
     * @return the name of the record
     */
    String getName();

    /**
     * @return the name of the context this record belongs in
     */
    String getContextName();

    /**
     * Convenience put method for a long value
     * 
     * @see #put(String, IValue)
     */
    IValue put(String key, long value);

    /**
     * Convenience put method for a double value
     * 
     * @see #put(String, IValue)
     */
    IValue put(String key, double value);

    /**
     * Convenience put method for a String value
     * 
     * @see #put(String, IValue)
     */
    IValue put(String key, String value);

    /**
     * Convenience version of {@link #get(Object)}, this accepts the {@link String} key and returns
     * a value compatible with the assigned reference
     * 
     * @param key
     *            the key in the record for the field
     * @return the value for the key, cast to the correct reference. <code>null</code> if there is
     *         no value for this key
     */
    <T extends IValue> T get(String key);

    /**
     * This method allows all the sub-maps held within a record to be discovered by their keys.
     * Calling {@link #getOrCreateSubMap(String)} with each key returned from this method will
     * retrieve the sub-map for the associated key.
     * 
     * @return the set of keys for each internal sub-map held in the record
     */
    Set<String> getSubMapKeys();

    /**
     * Retrieve a map from the record (a sub-map of the record) or create it if it does not exist.
     * This allows maps to be stored in the record against a key. This method initially creates a
     * blank sub-map and as the sub-map is altered, the record will track the changes.
     * <p>
     * <b>Note:</b> this method automatically creates sub-maps, so to test if a sub-map exists
     * before calling this method, call {@link #getSubMapKeys()} to see if the sub-map key exists.
     * 
     * @param subMapKey
     *            the key to lookup the sub-map from within the record
     * @return the sub-map held against this key or an empty map that can be populated
     */
    Map<String, IValue> getOrCreateSubMap(String subMapKey);

    /**
     * Remove a sub-map that was stored against the key. After this method completes, the sub-map is
     * removed from the record.
     * 
     * @param subMapKey
     *            the key to lookup the sub-map from within the record
     * @return the sub-map that was held in the record
     */
    Map<String, IValue> removeSubMap(String subMapKey);

    /**
     * Resolve the state of this record from the stream attached to the reader
     * 
     * @param reader
     *            the reader for the stream of the serialised state of this record
     * @throws IOException
     */
    void resolveFromStream(Reader reader) throws IOException;

    /**
     * Write the state of this record to the stream attached to the writer
     * 
     * @param writer
     *            the writer for the stream to hold the serialised state of this record
     * @throws IOException
     */
    void serializeToStream(Writer writer) throws IOException;

    /**
     * Get a flattened view of the record and internal sub-maps. The returned map reflects the state
     * of the record at the time of calling this method. The map will not show subsequent changes to
     * the record.
     * 
     * @return all the key-values pairs of the record and sub-maps in a single map
     */
    Map<String, IValue> asFlattenedMap();

    /**
     * Get a read-only version of this record. Changes to the underlying record will STILL be seen
     * by the immutable instance. The contents cannot be changed directly via the returned instance.
     * <p>
     * To get a snapshot of a record use {@link ImmutableSnapshotRecord#create(IRecord)}.
     * 
     * @return an immutable instance that will reflect all current and future changes to this record
     */
    IRecord getImmutableInstance();

    /**
     * Get the change sequence number for this record. Each published change of a record increments
     * the sequence number. This is, in effect, a version number for the record.
     * 
     * @return the sequence number for this record
     */
    long getSequence();
}
