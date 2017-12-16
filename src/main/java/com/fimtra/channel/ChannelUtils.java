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
package com.fimtra.channel;

/**
 * Utilities for working with {@link ITransportChannel} instances
 * 
 * @author Ramon Servadei
 */
public abstract class ChannelUtils
{
    private ChannelUtils()
    {
    }

    private final static TransportTechnologyEnum TRANSPORT = TransportTechnologyEnum.getDefaultFromSystemProperty();

    /** Connection aliveness watchdog for all {@link ITransportChannel} instances */
    public final static ChannelWatchdog WATCHDOG = new ChannelWatchdog();

    public static final byte[] HEARTBEAT_SIGNAL = { 0x3 };

    /**
     * @see TransportTechnologyEnum#getNextAvailableServicePort()
     */
    public static int getNextAvailableServicePort()
    {
        return TRANSPORT.getNextAvailableServicePort();
    }
}
