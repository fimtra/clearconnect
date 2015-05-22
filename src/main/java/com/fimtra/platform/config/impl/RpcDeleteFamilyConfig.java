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
 * Encapsulates the logic to delete configuration for a service family.
 * 
 * @author Paul Mackinlay
 */
final class RpcDeleteFamilyConfig extends AbstractDeleteConfig {

	static final String rpcName = "deleteFamilyConfig";

	RpcDeleteFamilyConfig(ConfigService configService, ConfigDirReader configDirReader, IPlatformServiceInstance platformServiceInstance) {
		super(configService, configDirReader, platformServiceInstance);

		IRpcInstance deleteConfigRpc = new RpcInstance(this, TypeEnum.TEXT, rpcName, new String[] { "service family", "config key" },
				TypeEnum.TEXT, TypeEnum.TEXT);
		this.platformServiceInstance.publishRPC(deleteConfigRpc);
	}

	/**
	 * arg[1] is the service family for the config that will be deleted.
	 * <p>
	 * arg[2] is the config key.
	 */
	@Override
	public IValue execute(IValue... args) throws TimeOutException, ExecutionException {
		return deleteConfig(args[0].textValue(), args[1].textValue());
	}
}
