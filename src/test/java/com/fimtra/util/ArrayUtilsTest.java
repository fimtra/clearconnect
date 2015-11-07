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
package com.fimtra.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.fimtra.util.ArrayUtils;

/**
 * Tests for {@link ArrayUtils}
 * 
 * @author Ramon Servadei
 */
public class ArrayUtilsTest
{
    @Test
    public void testContainsInstance()
    {
        String[] s = new String[] { "1", "2" };
        assertFalse(ArrayUtils.containsInstance(s, null));
        assertFalse(ArrayUtils.containsInstance(s, ""));
        // not the same instance
        assertFalse(ArrayUtils.containsInstance(s, new String("1")));
        
        assertTrue(ArrayUtils.containsInstance(s, "1"));
        assertTrue(ArrayUtils.containsInstance(s, "2"));
    }

}
