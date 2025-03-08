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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link CharSubArray}
 * 
 * @author Ramon Servadei
 */
public class CharSubArrayTest
{

    @Test
    public void testHashCodeAndEquals()
    {
        CharSubArray candidate = new CharSubArray("world".toCharArray(), 0, 5);

        assertEquals(candidate.hashCode(), candidate.hashCode());
        assertEquals(candidate.hashCode(), new CharSubArray("hello-world!".toCharArray(), 6, 5).hashCode());
        assertEquals(candidate.hashCode(), new CharSubArray("bye-world!".toCharArray(), 4, 5).hashCode());
        assertEquals(candidate.hashCode(), new CharSubArray("world".toCharArray(), 0, 5).hashCode());

        assertNotEquals(candidate.hashCode(), (new CharSubArray("world!".toCharArray(), 1, 5)).hashCode());

        assertNotEquals(candidate, new CharSubArray("world!".toCharArray(), 1, 5));
        assertEquals(candidate, candidate);
        assertEquals(new CharSubArray("hello-world!".toCharArray(), 6, 5), candidate);
        assertEquals(new CharSubArray("bye-world!".toCharArray(), 4, 5), candidate);
        assertEquals(new CharSubArray("world".toCharArray(), 0, 5), candidate);

        assertNotEquals(new CharSubArray("world!".toCharArray(), 1, 5), candidate);
    }

    @Test
    public void testHashCodeAndEquals_7chars()
    {
        CharSubArray candidate = new CharSubArray("helloworld".toCharArray(), 0, 10);

        assertEquals(candidate.hashCode(), candidate.hashCode());
        assertEquals(candidate.hashCode(), new CharSubArray("..helloworld!".toCharArray(), 2, 10).hashCode());
        assertEquals(candidate.hashCode(), new CharSubArray("helloworld".toCharArray(), 0, 10).hashCode());

        assertNotEquals(candidate.hashCode(), (new CharSubArray("world!".toCharArray(), 1, 5)).hashCode());

        assertNotEquals(candidate, new CharSubArray("world!".toCharArray(), 1, 5));
        assertEquals(candidate, candidate);
        assertEquals(new CharSubArray("..helloworld!".toCharArray(), 2, 10), candidate);
        assertEquals(new CharSubArray("helloworld".toCharArray(), 0, 10), candidate);

        assertNotEquals((new CharSubArray("world!".toCharArray(), 1, 5)), candidate);
    }

    @Test
    public void testMapGet()
    {
        final Map<CharSubArray, String> map = new HashMap<>();

        final String helloworld = "helloworld";
        final CharSubArray originalKey = new CharSubArray(helloworld.toCharArray(), 0, 10);
        map.put(originalKey, helloworld);
        map.put(new CharSubArray("?helloworld".toCharArray(), 0, 10), "?helloworl");
        map.put(new CharSubArray("?helloworld".toCharArray(), 0, 11), "?helloworld");
        map.put(new CharSubArray("helloworld!".toCharArray(), 0, 11), "helloworld!");
        map.put(new CharSubArray("yelloworld".toCharArray(), 0, 10), "yelloworld");

        assertSame(helloworld, map.get(originalKey));
        assertSame(helloworld, map.get(new CharSubArray(helloworld.toCharArray(), 0, 10)));
        assertSame(helloworld, map.get(new CharSubArray("..helloworld!".toCharArray(), 2, 10)));
        assertNull(map.get(new CharSubArray("..helloworld!".toCharArray(), 1, 10)));
    }
}
