/*
 * Copyright (c) 2014 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config.impl;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.platform.IPlatformServiceProxy;
import com.fimtra.platform.config.ConfigServiceProperties;
import com.fimtra.platform.core.PlatformUtils;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Paul Mackinlay
 */
public class ConfigManagerTest {

	private static final IValue testConfig = TextValue.valueOf("test config value");
	private static final String testConfigKey = "test-config-key";
	private static final String testMember = "test-member";
	private static final String testFamily = "test-service";
	private ConfigManager configManager;
	private IPlatformServiceProxy proxyForConfigService;

	@Before
	public void setUp() throws TimeOutException, ExecutionException {
		this.proxyForConfigService = mock(IPlatformServiceProxy.class);
		this.configManager = new ConfigManager(this.proxyForConfigService, testFamily, testMember,
				ConfigServiceProperties.Values.DEFAULT_CONFIG_MANAGER_RPC_TIMEOUT_MILLIS);
		when(this.proxyForConfigService.executeRpc(anyLong(), anyString(), any(IValue.class), any(IValue.class), any(IValue.class)))
				.thenReturn(TextValue.valueOf("mock RPC result"));
		when(this.proxyForConfigService.executeRpc(anyLong(), anyString(), any(IValue.class), any(IValue.class))).thenReturn(
				TextValue.valueOf("mock RPC result"));
	}

	@Test
	public void shouldCreateOrUpdateFamilyConfig() throws TimeOutException, ExecutionException {
		boolean isRpcSuccess = this.configManager.createOrUpdateFamilyConfig(testConfigKey, testConfig);
		assertTrue(isRpcSuccess);
		verify(this.proxyForConfigService, times(1)).executeRpc(ConfigServiceProperties.Values.DEFAULT_CONFIG_MANAGER_RPC_TIMEOUT_MILLIS,
				RpcCreateOrUpdateFamilyConfig.rpcName, TextValue.valueOf(testFamily), TextValue.valueOf(testConfigKey), testConfig);
	}

	@Test
	public void shouldNotCreateOrUpdateFamilyConfig() throws TimeOutException, ExecutionException {
		when(this.proxyForConfigService.executeRpc(anyLong(), anyString(), any(IValue.class), any(IValue.class), any(IValue.class)))
				.thenThrow(new RuntimeException("mock failure"));
		boolean isRpcSuccess = this.configManager.createOrUpdateFamilyConfig(testConfigKey, testConfig);
		assertFalse(isRpcSuccess);
	}

	@Test
	public void shouldCreateOrUpdateMemberConfig() throws TimeOutException, ExecutionException {
		boolean isRpcSuccess = this.configManager.createOrUpdateMemberConfig(testConfigKey, testConfig);
		assertTrue(isRpcSuccess);
		verify(this.proxyForConfigService, times(1)).executeRpc(ConfigServiceProperties.Values.DEFAULT_CONFIG_MANAGER_RPC_TIMEOUT_MILLIS,
				RpcCreateOrUpdateMemberConfig.rpcName,
				TextValue.valueOf(PlatformUtils.composePlatformServiceInstanceID(testFamily, testMember)),
				TextValue.valueOf(testConfigKey), testConfig);
	}

	@Test
	public void shouldNotCreateOrUpdateMemberConfig() throws TimeOutException, ExecutionException {
		when(this.proxyForConfigService.executeRpc(anyLong(), anyString(), any(IValue.class), any(IValue.class), any(IValue.class)))
				.thenThrow(new RuntimeException("mock failure"));
		boolean isRpcSuccess = this.configManager.createOrUpdateMemberConfig(testConfigKey, testConfig);
		assertFalse(isRpcSuccess);
	}

	@Test
	public void shouldDeleteFamilyConfig() throws TimeOutException, ExecutionException {
		boolean isRpcSuccess = this.configManager.deleteFamilyConfig(testConfigKey);
		assertTrue(isRpcSuccess);
		verify(this.proxyForConfigService, times(1)).executeRpc(ConfigServiceProperties.Values.DEFAULT_CONFIG_MANAGER_RPC_TIMEOUT_MILLIS,
				RpcDeleteFamilyConfig.rpcName, TextValue.valueOf(testFamily), TextValue.valueOf(testConfigKey));
	}

	@Test
	public void shouldNotDeleteFamilyConfig() throws TimeOutException, ExecutionException {
		when(this.proxyForConfigService.executeRpc(anyLong(), anyString(), any(IValue.class), any(IValue.class))).thenThrow(
				new RuntimeException("mock failure"));
		boolean isRpcSuccess = this.configManager.deleteFamilyConfig(testConfigKey);
		assertFalse(isRpcSuccess);
	}

	@Test
	public void shouldDeleteMemberConfig() throws TimeOutException, ExecutionException {
		boolean isRpcSuccess = this.configManager.deleteMemberConfig(testConfigKey);
		assertTrue(isRpcSuccess);
		verify(this.proxyForConfigService, times(1)).executeRpc(ConfigServiceProperties.Values.DEFAULT_CONFIG_MANAGER_RPC_TIMEOUT_MILLIS,
				RpcDeleteMemberConfig.rpcName, TextValue.valueOf(PlatformUtils.composePlatformServiceInstanceID(testFamily, testMember)),
				TextValue.valueOf(testConfigKey));
	}

	@Test
	public void shouldNotDeleteMemberConfig() throws TimeOutException, ExecutionException {
		when(this.proxyForConfigService.executeRpc(anyLong(), anyString(), any(IValue.class), any(IValue.class))).thenThrow(
				new RuntimeException("mock failure"));
		boolean isRpcSuccess = this.configManager.deleteMemberConfig(testConfigKey);
		assertFalse(isRpcSuccess);
	}

}
