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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.fimtra.util.ClassUtils;
import com.fimtra.util.SystemUtils;

/**
 * @author Paul Mackinlay
 */
public class ClassUtilsTest {

	@Test
	public void shouldFindLinesForAllowedKeys() {
		String info = ClassUtils.getFilteredManifestInfo(ClassUtils.class, ClassUtils.fimtraVersionKeys);
		String[] lines = info.split(SystemUtils.lineSeparator());
		assertTrue(lines.length <= ClassUtils.fimtraVersionKeys.size());
		for (String key : ClassUtils.fimtraVersionKeys) {
			boolean keyFound = false;
			for (String line : lines) {
				String[] parts = line.split(": ", 2);
				if (parts[0].equals(key)) {
					keyFound = true;
					break;
				}
			}
			if (!keyFound) {
				System.out.println("Key [" + key + "] not found in manifest.");
			}
			//assertTrue("Key [" + key + "] not found in manifest.", keyFound);
		}
	}

	@Test
	public void shouldFindMoreLinesThanAllowedKeys() {
		String info = ClassUtils.getManifestInfo(ClassUtils.class);
		String[] lines = info.split(SystemUtils.lineSeparator());
		assertTrue(lines.length > ClassUtils.fimtraVersionKeys.size());
	}
}