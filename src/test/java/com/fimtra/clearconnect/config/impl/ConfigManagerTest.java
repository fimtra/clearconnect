/*
 * Copyright (c) 2014 Paul Mackinlay, Fimtra
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
package com.fimtra.clearconnect.config.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fimtra.clearconnect.IPlatformServiceProxy;
import com.fimtra.clearconnect.config.ConfigServiceProperties;
import com.fimtra.clearconnect.core.PlatformUtils;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.TextValue;
import org.junit.Before;
import org.junit.Test;

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
