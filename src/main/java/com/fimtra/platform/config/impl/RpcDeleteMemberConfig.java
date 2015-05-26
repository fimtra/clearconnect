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
package com.fimtra.platform.config.impl;

import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.platform.IPlatformServiceInstance;

/**
 * Encapsulates the logic to delete configuration for a platform service instance.
 * 
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
final class RpcDeleteMemberConfig extends AbstractDeleteConfig {

	static final String rpcName = "deleteMemberConfig";

	RpcDeleteMemberConfig(ConfigService configService, ConfigDirReader configDirReader, IPlatformServiceInstance platformServiceInstance) {
		super(configService, configDirReader, platformServiceInstance);

		IRpcInstance deleteConfigRpc = new RpcInstance(this, TypeEnum.TEXT, rpcName, new String[] { "service instance id", "config key" },
				TypeEnum.TEXT, TypeEnum.TEXT);
		this.platformServiceInstance.publishRPC(deleteConfigRpc);
	}

	/**
	 * arg[1] is the serviceInstanceId for the config that will be deleted.
	 * <p>
	 * arg[2] is the config key.
	 */
	@Override
	public IValue execute(IValue... args) throws TimeOutException, ExecutionException {
		return deleteConfig(args[0].textValue(), args[1].textValue());
	}

}
