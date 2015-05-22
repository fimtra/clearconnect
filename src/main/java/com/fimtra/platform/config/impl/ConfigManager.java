/*
 * Copyright (c) 2014 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
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
