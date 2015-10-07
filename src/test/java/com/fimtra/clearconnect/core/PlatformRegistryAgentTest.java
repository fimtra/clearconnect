/*
 * Copyright (c) 2015 Ramon Servadei 
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
package com.fimtra.clearconnect.core;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.clearconnect.IPlatformRegistryAgent.RegistryNotAvailableException;

/**
 * Tests for the {@link PlatformRegistryAgent}
 * 
 * @author Ramon Servadei
 */
public class PlatformRegistryAgentTest
{
    PlatformRegistryAgent candidate;
    PlatformRegistry registry;

    @Before
    public void setUp() throws Exception
    {
        this.registry = new PlatformRegistry("PRA-Test", "localhost", 54321);
    }

    @After
    public void tearDown() throws Exception
    {
        this.registry.destroy();
        this.candidate.destroy();
    }

    @Test
    public void test() throws RegistryNotAvailableException
    {
        this.candidate = new PlatformRegistryAgent("test", new EndPointAddress("localhost", 54322),new EndPointAddress("localhost", 54321));
        assertTrue(this.candidate.registryProxy.isConnected());
    }

}
