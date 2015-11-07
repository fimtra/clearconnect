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

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * A publisher context is the home for records for a specific context. The definition of context is
 * application specific. A publisher context creates records. Changes to the records in a context
 * can be distributed to one or more {@link IRecordListener} instances.
 * 
 * @author Ramon Servadei
 */
public interface IPublisherContext extends IObserverContext
{
    /**
     * Convenience method to get an existing record or create it if it does not exist
     * 
     * @see #createRecord(String)
     * @see #getRecord(String)
     * @param name
     *            the record name
     * @return the record
     */
    IRecord getOrCreateRecord(String name);

    /**
     * @see #createRecord(String, Map)
     */
    IRecord createRecord(String name);

    /**
     * Create a new record with the given name using the specified record implementation
     * 
     * @param name
     *            the name for the record
     * @param initialData
     *            the initial data that the record will use
     * @return the record for the name
     * @throws IllegalStateException
     *             if an instance with the same name already exists in this context
     */
    IRecord createRecord(String name, Map<String, IValue> initialData);

    /**
     * Remove a record from this context. All {@link IRecordListener} objects will stop receiving
     * changes occurring to this record after this method completes.
     * 
     * @param name
     *            the name of the record to remove
     * @return the record that has been removed
     */
    IRecord removeRecord(String name);

    /**
     * Notify all {@link IRecordListener} objects registered as observers of the named record with
     * the atomic change that has occurred to the record. The atomic change will contain all changes
     * to the record that have occurred since the last call to this method for the record.
     * <p>
     * <b>This method may be asynchronous.</b>
     * 
     * @return a {@link CountDownLatch} that is triggered when the change has been published to all
     *         observers. If there is no change to publish or there are no observers, the latch is
     *         still triggered.
     */
    CountDownLatch publishAtomicChange(String name);

    /**
     * Notify all {@link IRecordListener} objects registered as observers of the record with the
     * atomic change that has occurred to the record. The atomic change will contain all changes to
     * the record that have occurred since the last call to this method for the record.
     * <p>
     * <b>This method may be asynchronous.</b>
     * 
     * @return a {@link CountDownLatch} that is triggered when the change has been published to all
     *         observers. If there is no change to publish or there are no observers, the latch is
     *         still triggered.
     */
    CountDownLatch publishAtomicChange(IRecord record);

    /**
     * Create the RPC in this context. The name of the RPC is added to the
     * {@link ISystemRecordNames#CONTEXT_RPCS} record.
     * 
     * @param rpc
     *            the RPC to create
     * @throws IllegalArgumentException
     *             if an RPC with the same name already exists - use {@link #removeRpc(String)} to
     *             remove then add
     */
    void createRpc(IRpcInstance rpc);

    /**
     * Removes the RPC from this context.
     * 
     * @param rpcName
     *            the name of the RPC to remove
     */
    void removeRpc(String rpcName);

    /**
     * Add a validator
     * 
     * @param validator
     *            the validator to add
     * @return <code>true</code> if the validator was added, <code>false</code> if it was already
     *         there
     */
    boolean addValidator(IValidator validator);

    /**
     * Update a validator
     * 
     * @param validator
     *            the validator to update
     */
    void updateValidator(IValidator validator);

    /**
     * Remove a validator
     * 
     * @param validator
     *            the validator to remove
     * @return <code>true</code> if the validator was found and removed, <code>false</code> if it
     *         was not found
     */
    boolean removeValidator(IValidator validator);

    /**
     * Set the permission filter for this context. All subscriptions via
     * {@link #addObserver(String, IRecordListener, String...)} are passed through the filter to see
     * if the permission token is valid for the record(s) being subscribed for.
     * 
     * @param filter
     *            the filter to set, <code>null</code> to use the default "pass all" filter
     */
    void setPermissionFilter(IPermissionFilter filter);
}