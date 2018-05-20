/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
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
package com.fimtra.clearconnect.event;

/**
 * A listener that provides notifications when records are added or removed from a platform service
 * component.
 * <h2>Threading</h2> <b>Callbacks must be thread-safe.</b> They will be executed by at least 2
 * threads, possibly concurrently:
 * <ul>
 * <li>The image-on-subscribe is handled by a dedicated image notifier thread (image thread).
 * <li>Normal updates are handled by a different thread (update thread).
 * </ul>
 * The image and update threads will be different and there is no guarantee that images will be
 * notified before real-time updates.
 * 
 * @author Ramon Servadei
 */
public interface IRecordAvailableListener extends IEventListener
{
    /**
     * Called when a record is added to a platform service component.
     * 
     * @param recordName
     *            the name of the record that was added to the platform service component.
     */
    void onRecordAvailable(String recordName);

    /**
     * Called when a record is removed from a platform service component.
     * 
     * @param recordName
     *            the name of the record that was removed from the platform service component.
     */
    void onRecordUnavailable(String recordName);
}