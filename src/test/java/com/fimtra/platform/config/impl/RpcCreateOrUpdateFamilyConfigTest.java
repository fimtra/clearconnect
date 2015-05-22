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

import com.fimtra.datafission.IRpcInstance;
import com.fimtra.platform.IPlatformServiceInstance;

import static org.mockito.Matchers.any;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Paul Mackinlay
 */
public class RpcCreateOrUpdateFamilyConfigTest {

	private ConfigService configService;
	private ConfigDirReader configDirReader;
	private IPlatformServiceInstance platformServiceInstance;

	@Before
	public void setUp() {
		this.configService = mock(ConfigService.class);
		this.configDirReader = mock(ConfigDirReader.class);
		this.platformServiceInstance = mock(IPlatformServiceInstance.class);
	}

	@SuppressWarnings("unused")
    @Test
	public void shouldPublishRpc() {
		new RpcCreateOrUpdateFamilyConfig(this.configService, this.configDirReader, this.platformServiceInstance);
		verify(this.platformServiceInstance, times(1)).publishRPC(any(IRpcInstance.class));
	}

}
