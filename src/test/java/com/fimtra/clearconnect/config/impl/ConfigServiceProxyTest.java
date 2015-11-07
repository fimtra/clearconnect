/*
 * Copyright (c) 2015 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.clearconnect.config.impl;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.clearconnect.IPlatformServiceProxy;
import com.fimtra.clearconnect.config.IConfigManager;

import static org.mockito.Mockito.mock;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConfigServiceProxyTest {

	private static final String SERVICE_FAMILY_A = "serviceFamilyA";
	private static final String SERVICE_FAMILY_B = "serviceFamilyB";
	private static final String SERVICE_MEMBER_1 = "serviceMember1";
	private static final String SERVICE_MEMBER_2 = "serviceMember2";
	private ConfigServiceProxy configServiceProxy;

	@Before
	public void setUp() {
		IPlatformServiceProxy proxyForConfigService = mock(IPlatformServiceProxy.class);
		this.configServiceProxy = new ConfigServiceProxy(proxyForConfigService);
	}

	@Test
	public void shouldEnsureSameInstanceForSameServiceMember() {
		IConfigManager configManager1 = this.configServiceProxy.getConfigManager(SERVICE_FAMILY_A, SERVICE_MEMBER_1);
		assertNotNull(configManager1);
		IConfigManager configManager2 = this.configServiceProxy.getConfigManager(SERVICE_FAMILY_A, SERVICE_MEMBER_1);
		assertTrue(configManager1 == configManager2);
		IConfigManager configManager3 = this.configServiceProxy.getConfigManager(SERVICE_FAMILY_B, SERVICE_MEMBER_1);
		assertNotNull(configManager3);
		assertTrue(configManager1 != configManager3);
		IConfigManager configManager4 = this.configServiceProxy.getConfigManager(SERVICE_FAMILY_B, SERVICE_MEMBER_2);
		assertNotNull(configManager4);
		assertTrue(configManager3 != configManager4);
	}

}
