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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.TextValue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Paul Mackinlay
 */
public class AbstractDeleteConfigTest {

	private static File nullRecord;
	private TestDeleteConfig rpcHandler;
	private ConfigService configService;
	private IConfigPersist configPersist;
	private IPlatformServiceInstance platformServiceInstance;
	private IRecord record;

	@BeforeClass
	public static void staticSetUp() throws IOException {
		nullRecord = new File("null.record");
		nullRecord.createNewFile();
	}

	@AfterClass
	public static void staticTearDown() {
		nullRecord.delete();
	}

	@Before
	public void setUp() {
		this.configService = mock(ConfigService.class);
		this.configPersist = mock(IConfigPersist.class);
		this.platformServiceInstance = mock(IPlatformServiceInstance.class);
		this.record = mock(IRecord.class);
		this.rpcHandler = new TestDeleteConfig(this.configService, this.configPersist, this.platformServiceInstance);
	}

	@Test
	public void shouldDeleteConfig() throws TimeOutException, ExecutionException {
		String serviceInstanceName = "service instance";
		String configKey = "config key";
		IValue[] args = new IValue[] { TextValue.valueOf(serviceInstanceName), TextValue.valueOf(configKey) };
		when(this.platformServiceInstance.getOrCreateRecord(serviceInstanceName)).thenReturn(this.record);
		String result = this.rpcHandler.execute(args).textValue();
		assertFalse(result.startsWith("RPC failed:"));
		verify(this.record, times(1)).remove(configKey);
		verify(this.configService, times(1)).publishConfig();
	}

	private class TestDeleteConfig extends AbstractDeleteConfig {

		TestDeleteConfig(ConfigService configService, IConfigPersist configPersist, IPlatformServiceInstance platformServiceInstance) {
			super(configService, configPersist, platformServiceInstance);
		}

		@Override
		public IValue execute(IValue... args) throws TimeOutException, ExecutionException {
			return super.deleteConfig(args[0].textValue(), args[1].textValue());
		}
	}
}
