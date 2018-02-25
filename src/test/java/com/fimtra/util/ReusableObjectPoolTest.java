/*
 * Copyright (c) 2017 Ramon Servadei
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link ReusableObjectPool}
 * 
 * @author Ramon Servadei
 */
public class ReusableObjectPoolTest
{
    AbstractReusableObjectPool<Map> candidate;

    @Before
    public void setUp() throws Exception
    {
        candidate = new SingleThreadReusableObjectPool<Map>("test", new IReusableObjectBuilder<Map>()
        {
            @Override
            public Map newInstance()
            {
                return new HashMap();
            }
        }, new IReusableObjectFinalizer<Map>()
        {
            @Override
            public void reset(Map instance)
            {
                instance.clear();
            }
        }, 10);
    }
    
    @Test
    public void testSimpleGetOffer()
    {
        final Map map1 = candidate.get();
        final Map map2 = candidate.get();
        assertNotSame(map1, map2);
        map1.put("1", "1");
        map2.put("2", "2");
        
        candidate.offer(map1);
        final Map map1_1 = candidate.get();        
        assertSame(map1, map1_1);
        assertEquals(0, map1_1.size());
        
        candidate.offer(map1);
        candidate.offer(map2);
        
        final Map map2_1 = candidate.get();
        assertSame(map2, map2_1);
        assertEquals(0, map2_1.size());
    }

    @Test
    public void testGrowth()
    {
        int max = candidate.getSize();
        List<Map> maps = new ArrayList<Map>(max);
        for(int i = 0; i < max; i++)
        {
            maps.add(candidate.get());
        }
        for(int i = 0; i < max; i++)
        {
            candidate.offer(maps.get(i));
        }
        System.out.println(candidate.getSize());
    }

}
