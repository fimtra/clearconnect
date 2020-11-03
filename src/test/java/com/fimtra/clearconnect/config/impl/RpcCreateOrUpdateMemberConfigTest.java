/*
 * Copyright (c) 2013 Paul Mackinlay, Fimtra
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.datafission.IRpcInstance;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Paul Mackinlay
 */
public class RpcCreateOrUpdateMemberConfigTest {

	private ConfigService configService;
	private IConfigPersist configPersist;
	private IPlatformServiceInstance platformServiceInstance;

	@Before
	public void setUp() {
		this.configService = mock(ConfigService.class);
		this.configPersist = mock(IConfigPersist.class);
		this.platformServiceInstance = mock(IPlatformServiceInstance.class);
	}

	@SuppressWarnings("unused")
    @Test
	public void shouldPublishRpc() {
		new RpcCreateOrUpdateMemberConfig(this.configService, this.configPersist, this.platformServiceInstance);
		verify(this.platformServiceInstance, times(1)).publishRPC(any(IRpcInstance.class));
	}

}
