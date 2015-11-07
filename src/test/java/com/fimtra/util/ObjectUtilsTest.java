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
package com.fimtra.util;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.fimtra.util.ObjectUtils;

/**
 * Tests for the {@link ObjectUtils}
 * 
 * @author Ramon Servadei
 */
public class ObjectUtilsTest {

	@Test
	public void testSafeToString() {
		assertNotNull(ObjectUtils.safeToString(null));
		assertNotNull(ObjectUtils.safeToString(this));
		assertNotNull(ObjectUtils.safeToString(new Object() {

			@Override
			public String toString() {
				throw new UnsupportedOperationException();
			}

		}));
	}
}
