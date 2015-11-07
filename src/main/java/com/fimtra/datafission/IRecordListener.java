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

/**
 * Receives atomic changes that occur to a record.
 * <p>
 * <b>Implementations need to be thread safe.<b>
 * 
 * @author Ramon Servadei
 */
public interface IRecordListener
{
    /**
     * Invoked when the {@link IPublisherContext#publishAtomicChange(String)} method is called. All
     * changes that have occurred to the named record since the last call to this method are
     * captured in the {@link IRecordChange} argument.
     * <p>
     * <b>This method is NEVER invoked concurrently for the same record.</b> However if the listener
     * is registered against multiple records then this method can execute concurrently across the
     * records that were registered (depends on thread scheduling).
     * <p>
     * The passed in 'image' reflects all atomic changes that have occurred to the source record,
     * including the current one. <b>Be aware that, due to concurrent application activities, the
     * image may include changes that are yet to be presented to the listener from a subsequent
     * atomic change update.</b>
     * 
     * @param imageValidInCallingThreadOnly
     *            an immutable and re-used instance that is an immutable version of the source
     *            record's image. This image will include the current atomic change. <b>This
     *            instance must not be cached and it must only be accessed in the thread that calls
     *            this method.</b>
     * @param atomicChange
     *            the atomic change that has occurred to the record - thread safe.
     */
    void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange);
}
