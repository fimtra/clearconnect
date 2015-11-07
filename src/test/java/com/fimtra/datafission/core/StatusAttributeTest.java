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
package com.fimtra.datafission.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.IStatusAttribute;
import com.fimtra.datafission.core.IStatusAttribute.Connection;

/**
 * Tests for the {@link IStatusAttribute.Utils}
 * 
 * @author Ramon Servadei
 */
public class StatusAttributeTest
{
    @Test
    public void testSetGetStatus()
    {
        Map<String, IValue> record = new HashMap<String, IValue>();
        IStatusAttribute.Utils.setStatus(Connection.DISCONNECTED, record);
        assertEquals(Connection.DISCONNECTED, IStatusAttribute.Utils.getStatus(Connection.class, record));
    }

    @Test
    public void testGetNullStatus()
    {
        Map<String, IValue> record = new HashMap<String, IValue>();
        assertNull(IStatusAttribute.Utils.getStatus(Connection.class, record));
    }
}
