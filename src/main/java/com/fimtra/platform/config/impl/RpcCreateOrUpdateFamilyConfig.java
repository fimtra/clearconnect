/*
 * Copyright (c) 2014 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
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
 * Encapsulates the logic to create or update configuration for all instances of a platform service.
 *
 * @author Paul Mackinlay
 */
final class RpcCreateOrUpdateFamilyConfig extends AbstractCreateOrUpdateConfig {

	static final String rpcName = "createOrUpdateFamilyConfig";

	RpcCreateOrUpdateFamilyConfig(ConfigService configService, ConfigDirReader configDirReader,
			IPlatformServiceInstance platformServiceInstance) {
		super(configService, configDirReader, platformServiceInstance);

		IRpcInstance createOrUpdateServiceConfigRpc = new RpcInstance(this, TypeEnum.TEXT, rpcName, new String[] { "service family",
				"config key", "config value" }, TypeEnum.TEXT, TypeEnum.TEXT, TypeEnum.TEXT);
		this.platformServiceInstance.publishRPC(createOrUpdateServiceConfigRpc);
	}

	/**
	 * arg[0] is the serviceName for the config that will be created or modified.
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
