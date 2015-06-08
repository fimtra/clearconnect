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

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.config.impl.AbstractCreateOrUpdateConfig;
import com.fimtra.clearconnect.config.impl.ConfigDirReader;
import com.fimtra.clearconnect.config.impl.ConfigService;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.TextValue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertFalse;

/**
 * @author Paul Mackinlay
 */
public class AbstractCreateOrUpdateConfigTest {

	private TestCreateOrUpdateConfig rpcHandler;
	private ConfigService configService;
	private ConfigDirReader configDirReader;
	private IPlatformServiceInstance platformServiceInstance;
	private IRecord record;

	@Before
	public void setUp() {
		this.configService = mock(ConfigService.class);
		this.configDirReader = mock(ConfigDirReader.class);
		this.platformServiceInstance = mock(IPlatformServiceInstance.class);
		this.record = mock(IRecord.class);
		this.rpcHandler = new TestCreateOrUpdateConfig(this.configService, this.configDirReader, this.platformServiceInstance);
	}

	@Test
	public void shouldUpdateConfig() throws TimeOutException, ExecutionException {
		String configRecordName = "service instance";
		String configKey = "config key";
		String configValue = "config value";
		IValue[] args = new IValue[] { new TextValue(configRecordName), new TextValue(configKey), new TextValue(configValue) };
		when(this.platformServiceInstance.getOrCreateRecord(configRecordName)).thenReturn(this.record);
		String result = this.rpcHandler.execute(args).textValue();
		assertFalse(result.startsWith("RPC failed:"));
		verify(this.record, times(1)).put(configKey, configValue);
		verify(this.configService, times(1)).publishConfig();
	}

	@After
	public void tearDown() {
		File file = new File("null.record");
		if (file.exists()) {
			file.delete();
		}
	}

	private class TestCreateOrUpdateConfig extends AbstractCreateOrUpdateConfig {

		TestCreateOrUpdateConfig(ConfigService configService, ConfigDirReader configDirReader,
				IPlatformServiceInstance platformServiceInstance) {
			super(configService, configDirReader, platformServiceInstance);
		}

		@Override
		public IValue execute(IValue... args) throws TimeOutException, ExecutionException {
			return super.createOrUpdateConfig(args[0].textValue(), args[1].textValue(), args[2].textValue());
		}
	}
}
