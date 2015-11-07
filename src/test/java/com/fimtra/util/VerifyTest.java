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
import static org.junit.Assert.fail;

import org.junit.Test;

import com.fimtra.util.Verify;

/**
 * @author Paul Mackinlay
 */
public class VerifyTest {

	@Test
	public void shouldCheckIsTrue() {
		String msg = "Is not true";
		Verify.isTrue(true, msg);
		try {
			Verify.isTrue(false, msg);
			fail("Should throw IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals(msg, e.getMessage());
		}
	}

	@Test
	public void shouldCheckNotEmpty() {
		String msg = "is empty";
		Verify.notEmpty("hello", msg);
		try {
			Verify.notEmpty(null, msg);
			fail("Should throw IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals(msg, e.getMessage());
		}
		try {
			Verify.notEmpty("", msg);
			fail("Should throw IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals(msg, e.getMessage());
		}
	}

	@Test
	public void shouldCheckNotNull() {
		String msg = "is null";
		Verify.notNull(new Object(), msg);
		try {
			Verify.notNull(null, msg);
			fail("Should throw IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals(msg, e.getMessage());
		}
	}

	// @Test
	// public void test() {
	// Validate.notNull(obj, msg);
	// fail("Not yet implemented");
	// }

}
