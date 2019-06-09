/*
 * Copyright (c) 2018 Ramon Servadei
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
 * A tagging interface for all event listeners.
 * <p>
 * <h2>Threading</h2> <b>Callbacks must be thread-safe.</b> They will be executed by 1 of 2 threads
 * and possibly concurrently:
 * <ul>
 * <li>The image-on-subscribe is handled by a dedicated image notifier thread (image thread).
 * <li>Normal updates are handled by a different thread (update thread).
 * </ul>
 * Data that has been updated before the initial image is received will be notified via the update
 * thread and excluded from the image. Thus there is no double notification of initial state.
 * 
 * @author Ramon Servadei
 */
public interface IEventListener
{

}
