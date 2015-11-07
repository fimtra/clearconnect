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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.util.StringWithNumbersComparator;

/**
 * Tests for the {@link StringWithNumbersComparator}
 * 
 * @author Ramon Servadei
 */
public class StringWithNumbersComparatorTest
{
    StringWithNumbersComparator candidate;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new StringWithNumbersComparator();
    }

    @Test
    public void testComparator1()
    {
        List<String> strings = new ArrayList<String>();
        strings.add("XYZ_10");
        strings.add("XYZ_1");
        Collections.sort(strings, this.candidate);
        assertEquals("[XYZ_1, XYZ_10]", strings.toString());
    }

    @Test
    public void testComparator2()
    {
        List<String> strings = new ArrayList<String>();
        strings.add("XYZ_10");
        strings.add("XYZ_9");
        Collections.sort(strings, this.candidate);
        assertEquals("[XYZ_9, XYZ_10]", strings.toString());
    }
    
    @Test
    public void testComparator3()
    {
        List<String> strings = new ArrayList<String>();
        strings.add("XYZ");
        strings.add("XYZ_10");
        strings.add("");
        Collections.sort(strings, this.candidate);
        assertEquals("[, XYZ, XYZ_10]", strings.toString());
    }
    
    @Test
    public void testComparator4()
    {
        List<String> strings = new ArrayList<String>();
        strings.add("XYZ");
        strings.add("XYZ_15");
        strings.add("XYZ_5");
        strings.add("WXYZ");
        strings.add("XYZ_1");
        strings.add("XYZ_10");
        strings.add(null);
        Collections.sort(strings, this.candidate);
        assertEquals("[null, WXYZ, XYZ, XYZ_1, XYZ_5, XYZ_10, XYZ_15]", strings.toString());
    }
    
    @Test
    public void testComparator5()
    {
        List<String> strings = new ArrayList<String>();
        strings.add("");
        strings.add("10XYZ");
        strings.add(null);
        strings.add("5XYZ");
        strings.add("1XYZ");
        strings.add("");
        strings.add("8XYZ");
        strings.add("9XYZ");
        strings.add("");
        Collections.sort(strings, this.candidate);
        assertEquals("[null, , , , 1XYZ, 5XYZ, 8XYZ, 9XYZ, 10XYZ]", strings.toString());
    }

    @Test
    public void testComparator()
    {
        List<String> strings = new ArrayList<String>();
        strings.add("XYZ_0");
        strings.add("XYZ_1");
        strings.add("XYZ_10");
        strings.add("XYZ_9");
        Collections.sort(strings, this.candidate);
        assertEquals("[XYZ_0, XYZ_1, XYZ_9, XYZ_10]", strings.toString());
    }

}
