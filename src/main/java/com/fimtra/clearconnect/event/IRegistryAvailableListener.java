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
 * Receives notifications when the agent is connected/disconnected to the platform registry.
 * 
 * @see IEventListener for threading
 * @author Ramon Servadei
 */
public interface IRegistryAvailableListener extends IEventListener
{
    /**
     * Called when the agent is connected to the registry
     */
    void onRegistryConnected();

    /**
     * Called when the agent is no longer connected to the registry
     */
    void onRegistryDisconnected();
}