/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei, Fimtra
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

import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.RpcInstance;

/**
 * Encapsulates the logic to create or update configuration for a platform service member.
 *
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
final class RpcCreateOrUpdateMemberConfig extends AbstractCreateOrUpdateConfig {

	static final String rpcName = "createOrUpdateMemberConfig";

	RpcCreateOrUpdateMemberConfig(ConfigService configService, IConfigPersist configPersist,
			IPlatformServiceInstance platformServiceInstance) {
		super(configService, configPersist, platformServiceInstance);

		IRpcInstance createOrUpdateConfigRpc = new RpcInstance(this, TypeEnum.TEXT, rpcName, new String[] { "service instance id",
				"config key", "config value" }, TypeEnum.TEXT, TypeEnum.TEXT, TypeEnum.TEXT);
		this.platformServiceInstance.publishRPC(createOrUpdateConfigRpc);
	}

	/**
	 * arg[0] is the serviceInstanceId for the config that will be created or modified.
	 * <p>
	 * arg[1] is the config key.
	 * <p>
	 * arg[2] is the config value.
	 */
	@Override
	public IValue execute(IValue... args) throws TimeOutException, ExecutionException {
		return super.createOrUpdateConfig(args[0].textValue(), args[1].textValue(), args[2].textValue());
	}
}
