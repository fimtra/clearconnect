/*
 * Copyright (c) 2016 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.clearconnect.config.impl;

import static org.mockito.Mockito.mock;

import java.io.File;

import com.fimtra.clearconnect.config.ConfigServiceProperties;
import org.junit.Before;
import org.junit.Test;

public class ConfigPersistFactoryTest {

	private File configDir;

	@Before
	public void setUp() {
		this.configDir = mock(File.class);
	}

	@Test
	public void shouldCheckInvalidPersistClass() {
		try {
			System.setProperty(ConfigServiceProperties.Names.CONFIG_PERSIST_CLASS, "non existent class");
			ConfigPersistFactory.getInstance(this.configDir);

			// fail("Expect construction to fail with invalid persist class.");
		} catch (Exception e) {
			// expected
		} finally {
			System.getProperties().remove(ConfigServiceProperties.Names.CONFIG_PERSIST_CLASS);
		}
	}

}
