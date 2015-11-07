/*
 * Copyright (c) 2013 Paul Mackinlay 
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

import com.fimtra.util.ExceptionUtils;

/**
 * @author Paul Mackinlay
 */
public class ExceptionUtilsTest {

	@Test
	public void shouldGetBasicStackTrace() {
		RuntimeException e = new RuntimeException();
		String stack = ExceptionUtils.getFullStackTrace(e);
		assertTrue(stack.startsWith(RuntimeException.class.getCanonicalName() + ": " + e.getMessage()));
		assertFalse(stack.contains("Caused by "));
		System.out.println(stack);
	}

	@Test
	public void shouldGetStackTraceWithCause() {
		try {
			throw new IllegalArgumentException("oops");
		} catch (RuntimeException e) {
			RuntimeException e1 = new RuntimeException(e);
			String stack = ExceptionUtils.getFullStackTrace(e1);
			assertTrue(stack.startsWith(RuntimeException.class.getCanonicalName() + ": "
					+ IllegalArgumentException.class.getCanonicalName() + ": " + e.getMessage()));
			assertTrue(stack.contains("Caused by " + IllegalArgumentException.class.getCanonicalName() + ": " + e.getMessage()));
			System.out.println(stack);
		}
	}
}
