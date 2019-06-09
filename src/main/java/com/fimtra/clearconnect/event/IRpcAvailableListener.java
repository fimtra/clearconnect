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

import com.fimtra.datafission.IRpcInstance;

/**
 * A listener that provides notifications when RPCs are available or unavailable
 * 
 * @see IEventListener for threading
 * @author Ramon Servadei
 */
public interface IRpcAvailableListener extends IEventListener
{
    /**
     * Called when a new RPC is available in the platform service component.
     * 
     * @param rpc
     *            the rpc that is available for calling
     */
    void onRpcAvailable(IRpcInstance rpc);

    /**
     * Called when an RPC is removed from the platform service component.
     * 
     * @param rpc
     *            the rpc that has been removed
     */
    void onRpcUnavailable(IRpcInstance rpc);
}