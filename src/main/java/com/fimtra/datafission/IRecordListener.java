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
     * Invoked when the {@link IPublisherContext#publishAtomicChange(IRecord)} method is called. All
     * changes that have occurred to the named record since the last call to this method are
     * captured in the {@link IRecordChange} argument.
     * <p>
     * <b>This method is NEVER invoked concurrently for the same record.</b> However if the listener
     * is registered against multiple records then this method can execute concurrently across the
     * records that were registered (depends on thread scheduling).
     * <p>
     * The passed in 'image' reflects all atomic changes that have occurred to the source record,
     * including the current one. It is a 'snapshot' of the state of the record when the publish
     * occurred. If you cache this image, be aware that its contents will be altered every time a
     * change to the underlying record is published.
     * 
     * @param image
     *            an immutable, thread-safe and re-used instance of the source record's image. The
     *            image contents will change each time the source record is published and will
     *            reflect the current state of the source record; be aware if you cache this image.
     * @param atomicChange
     *            the atomic change that has occurred to the record - thread safe.
     */
    void onChange(IRecord image, IRecordChange atomicChange);
}
