/*
 * Copyright (c) 2014 Paul Mackinlay 
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import com.fimtra.util.StringUtils;

/**
 * @author Paul Mackinlay
 */
public class StringUtilsTest {

	@Test
	public void shouldCheckStripToEmpty() {
		assertEquals("", StringUtils.stripToEmpty(null));
		assertEquals("", StringUtils.stripToEmpty(""));
		assertEquals("", StringUtils.stripToEmpty("  "));
		assertEquals("", StringUtils.stripToEmpty("\t"));
		assertEquals("", StringUtils.stripToEmpty("\n"));
		assertEquals("123", StringUtils.stripToEmpty("123"));
		assertEquals("123", StringUtils.stripToEmpty("\t123 "));
		assertEquals("123", StringUtils.stripToEmpty("   123  "));
		assertEquals("1 2 3", StringUtils.stripToEmpty(" 1 2 3 "));
	}
	
	@Test
	public void shouldCheckIsEmpty() {
		assertTrue(StringUtils.isEmpty(null));
		assertTrue(StringUtils.isEmpty(""));
		assertTrue(StringUtils.isEmpty("  "));
		assertTrue(StringUtils.isEmpty("\t"));
		assertTrue(StringUtils.isEmpty("\n"));
		assertFalse(StringUtils.isEmpty("123"));
		assertFalse(StringUtils.isEmpty("\t123 "));
		assertFalse(StringUtils.isEmpty("   123  "));
		assertFalse(StringUtils.isEmpty(" 1 2 3 "));
	}

	@Test 
	public void testSplitJoin()
	{
	    List<String> strings = new LinkedList<String>();
	    strings.add("string1");
	    strings.add("st,ri,ng2");
	    strings.add("string3");
	    strings.add(null);
	    strings.add("str,,ing4");
	    strings.add("stri,ng5");
	    
	    // NOTE: toString test as the null cannot be reconstituted back - it becomes the string "null"
	    assertEquals(strings.toString(), StringUtils.split(StringUtils.join(strings, ','), ',').toString());

	    strings = null;
	    assertEquals(strings, StringUtils.split(StringUtils.join(strings, ','), ','));
	}
	
	@Test
	public void testStartsWith()
	{
	    assertFalse(StringUtils.startsWith(new char[] {}, "string".toCharArray()));
	    assertFalse(StringUtils.startsWith(new char[] {'s','t','r'}, "st".toCharArray()));
	    assertTrue(StringUtils.startsWith(new char[] {'s','t','r'}, "str".toCharArray()));
	    assertTrue(StringUtils.startsWith(new char[] {'s','t','r'}, "string".toCharArray()));
	}
}
