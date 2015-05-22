/*
 * Copyright (c) 2013 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config.impl;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.platform.RedundancyModeEnum;
import com.fimtra.platform.config.ConfigProperties;
import com.fimtra.platform.config.IConfig;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Paul Mackinlay
 */
public class ConfigUtilsTest {

	private IConfig config;

	@Before
	public void setUp() {
		this.config = mock(IConfig.class);
	}

	@Test
	public void shouldGetWireProtocol() {
		String nonExistantProtocol = "choo-choo train";
		when(this.config.getProperty(ConfigProperties.CONFIG_KEY_INSTANCE_WIRE_PROTOCOL)).thenReturn(new TextValue(nonExistantProtocol));
		try {
			ConfigUtils.getWireProtocol(this.config);
			fail("Expect an IllegalArgumentException because the '" + nonExistantProtocol + "' protocol is not supported. Sorry.");
		} catch (IllegalArgumentException e) {
			// it passes the assertion
		}
	}

	@Test
	public void shouldGetRedundancyMode() {
		assertEquals(RedundancyModeEnum.FAULT_TOLERANT, ConfigUtils.getRedundancyMode(this.config));
		when(this.config.getProperty(ConfigProperties.CONFIG_KEY_INSTANCE_REDUNDANCY_MODE)).thenReturn(
				new TextValue(RedundancyModeEnum.LOAD_BALANCED.name()));
		assertEquals(RedundancyModeEnum.LOAD_BALANCED, ConfigUtils.getRedundancyMode(this.config));
		String nonExistantRedundancyMode = "eat fruit instead";
		when(this.config.getProperty(ConfigProperties.CONFIG_KEY_INSTANCE_REDUNDANCY_MODE)).thenReturn(
				new TextValue(nonExistantRedundancyMode));
		try {
			ConfigUtils.getRedundancyMode(this.config);
			fail("Expect an IllegalArgumentException because the '" + nonExistantRedundancyMode + "' redundancy mode is not supported.");
		} catch (IllegalArgumentException e) {
			// it passes the assertion
		}
	}

	@Test
	public void shouldGetPort() {
		String host = null;
		String configPort = "112233";
		assertTrue(ConfigUtils.getPort(this.config, host) > 0);
		when(this.config.getProperty(ConfigProperties.CONFIG_KEY_INSTANCE_PORT)).thenReturn(new TextValue(configPort));
		assertEquals(Integer.parseInt(configPort), ConfigUtils.getPort(this.config, host));
	}

	@Test
	public void shouldGetHost() {
		String host = "hostname";
		assertFalse(ConfigUtils.getHost(this.config).isEmpty());
		when(this.config.getProperty(ConfigProperties.CONFIG_KEY_INSTANCE_HOST)).thenReturn(new TextValue(host));
		assertEquals(host, ConfigUtils.getHost(this.config));
	}

	@Test
	public void shouldGetPropertyAsInt() {
		String testPropertyKey = "propertyKey";
		int defaultValue = 11;
		long numericConfigValue = 12l;
		String stringConfigValue = "" + numericConfigValue;
		assertEquals(defaultValue, ConfigUtils.getPropertyAsInt(this.config, testPropertyKey, defaultValue));
		when(this.config.getProperty(testPropertyKey)).thenReturn(new TextValue("not an int"));
		assertEquals(defaultValue, ConfigUtils.getPropertyAsInt(this.config, testPropertyKey, defaultValue));
		when(this.config.getProperty(testPropertyKey)).thenReturn(new TextValue(stringConfigValue));
		assertEquals(Integer.parseInt(stringConfigValue), ConfigUtils.getPropertyAsInt(this.config, testPropertyKey, defaultValue));
		when(this.config.getProperty(testPropertyKey)).thenReturn(LongValue.valueOf(numericConfigValue));
		assertEquals((int) numericConfigValue, ConfigUtils.getPropertyAsInt(this.config, testPropertyKey, defaultValue));
	}
}