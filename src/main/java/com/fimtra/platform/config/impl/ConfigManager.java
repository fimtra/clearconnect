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
package com.fimtra.platform.config.impl;

import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.platform.IPlatformServiceProxy;
import com.fimtra.platform.config.IConfigManager;
import com.fimtra.platform.core.PlatformUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;

/**
 * The default implementation of {@link IConfigManager}. A remote config service is supported so the API methods wrap appropriate RPC's,
 * the complexities arising from the asynchronous nature of the RPC's is shielded from the calling client. Exceptions are converted
 * into false return values in the API methods (i.e. the method failed).
 *
 * @author Paul Mackinlay
 */
public class ConfigManager implements IConfigManager {

	private final long rpcTimeoutMillis;
	private final IPlatformServiceProxy proxyForConfigService;
	private final String serviceFamily;
	private final String serviceMember;

	public ConfigManager(IPlatformServiceProxy proxyForConfigService, String serviceFamily, String serviceMember, long rpcTimeoutMillis) {
		this.proxyForConfigService = proxyForConfigService;
		this.serviceFamily = serviceFamily;
		this.serviceMember = serviceMember;
		this.rpcTimeoutMillis = rpcTimeoutMillis;
	}

	@Override
	public boolean createOrUpdateFamilyConfig(String configKey, IValue configValue) {
		try {
			IValue rpcResult = this.proxyForConfigService.executeRpc(this.rpcTimeoutMillis, RpcCreateOrUpdateFamilyConfig.rpcName,
					TextValue.valueOf(this.serviceFamily), TextValue.valueOf(configKey), configValue);
			Log.log(this, "Text value RPC result:" , ObjectUtils.safeToString(rpcResult));
		} catch (Exception e) {
			Log.log(this, "RPC failed", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean deleteFamilyConfig(String configKey) {
		try {
			IValue rpcResult = this.proxyForConfigService.executeRpc(this.rpcTimeoutMillis, RpcDeleteFamilyConfig.rpcName,
					TextValue.valueOf(this.serviceFamily), TextValue.valueOf(configKey));
			Log.log(this, "Text value RPC result:", ObjectUtils.safeToString(rpcResult));
		} catch (Exception e) {
			Log.log(this, "RPC failed", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean createOrUpdateMemberConfig(String configKey, IValue configValue) {
		try {
			IValue rpcResult = this.proxyForConfigService.executeRpc(this.rpcTimeoutMillis, RpcCreateOrUpdateMemberConfig.rpcName,
					TextValue.valueOf(PlatformUtils.composePlatformServiceInstanceID(this.serviceFamily, this.serviceMember)),
					TextValue.valueOf(configKey), configValue);
			Log.log(this, "Text value RPC result:", ObjectUtils.safeToString(rpcResult));
		} catch (Exception e) {
			Log.log(this, "RPC failed", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean deleteMemberConfig(String configKey) {
		try {
			IValue rpcResult = this.proxyForConfigService.executeRpc(this.rpcTimeoutMillis, RpcDeleteMemberConfig.rpcName,
					TextValue.valueOf(PlatformUtils.composePlatformServiceInstanceID(this.serviceFamily, this.serviceMember)),
					TextValue.valueOf(configKey));
			Log.log(this, "Text value RPC result:", ObjectUtils.safeToString(rpcResult));
		} catch (Exception e) {
			Log.log(this, "RPC failed", e);
			return false;
		}
		return true;
	}
}
