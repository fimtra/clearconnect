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
package com.fimtra.util;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link ObjectPool}
 * 
 * @author Ramon Servadei
 */
public class ObjectPoolTest
{
    ObjectPool<String> candidate;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new ObjectPool<String>("unit-test", 2);
    }

    @Test
    public void testIntern()
    {
        int i=0;
        String s1 = new String("" + ++i);
        String s2 = new String("" + ++i);
        String s3 = new String("" + ++i);
        
        assertSame(s1, candidate.intern(s1));
        assertSame(s1, candidate.intern(new String("1")));

        assertSame(s2, candidate.intern(s2));
        assertSame(s2, candidate.intern(new String("2")));

        // this will cause the pool to overflow so s1 will be wiped
        assertSame(s3, candidate.intern(s3));
        assertSame(s3, candidate.intern(new String("3")));
        
        String s1_2 = new String("1");
        assertNotSame(s1, candidate.intern(s1_2));
        assertSame(s1_2, candidate.intern(s1_2));
        
        assertSame(s2, candidate.intern(s2));
        
        assertNull(candidate.intern(null));
    }

}
