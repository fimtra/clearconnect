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

/**
 * Methods to help validation. Each method will throw an exception if validation fails.
 * 
 * @author Paul Mackinlay
 */
public abstract class Verify {

	private Verify() {
		// Not for instantiation
	}

	/**
	 * Validates that a string is not null and not empty.
	 * 
	 * @param msg
	 *            - the message for validation failure
	 * @throws IllegalArgumentException
	 *             is validation fails
	 */
	public static final void notEmpty(String string, String msg) {
		if (string == null || string.isEmpty()) {
			throw new IllegalArgumentException(msg);
		}
	}

	/**
	 * Validates that the condition is true.
	 * 
	 * @param condition
	 *            - the condition that is validated
	 * @param msg
	 *            - the message for validation failure
	 * @throws IllegalArgumentException
	 *             is validation fails
	 */
	public static final void isTrue(boolean condition, String msg) {
		if (condition == false) {
			throw new IllegalArgumentException(msg);
		}
	}

	/**
	 * Validates that the obj is not null.
	 * 
	 * @param msg
	 *            - the message for validation failure
	 * @throws IllegalArgumentException
	 *             is validation fails
	 */
	public static final void notNull(Object obj, String msg) {
		if (!is.notNull(obj)) {
			throw new IllegalArgumentException(msg);
		}
	}
}
