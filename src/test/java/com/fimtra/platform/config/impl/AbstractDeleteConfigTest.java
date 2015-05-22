/*
 * Copyright (c) 2014 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config.impl;

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.platform.IPlatformServiceInstance;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertFalse;

/**
 * @author Paul Mackinlay
 */
public class AbstractDeleteConfigTest {

	private static File nullRecord;
	private TestDeleteConfig rpcHandler;
	private ConfigService configService;
	private ConfigDirReader configDirReader;
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
		this.configDirReader = mock(ConfigDirReader.class);
		this.platformServiceInstance = mock(IPlatformServiceInstance.class);
		this.record = mock(IRecord.class);
		this.rpcHandler = new TestDeleteConfig(this.configService, this.configDirReader, this.platformServiceInstance);
	}

	@Test
	public void shouldDeleteConfig() throws TimeOutException, ExecutionException {
		String serviceInstanceName = "service instance";
		String configKey = "config key";
		IValue[] args = new IValue[] { new TextValue(serviceInstanceName), new TextValue(configKey) };
		when(this.platformServiceInstance.getOrCreateRecord(serviceInstanceName)).thenReturn(this.record);
		String result = this.rpcHandler.execute(args).textValue();
		assertFalse(result.startsWith("RPC failed:"));
		verify(this.record, times(1)).remove(configKey);
		verify(this.configService, times(1)).publishConfig();
	}

	private class TestDeleteConfig extends AbstractDeleteConfig {

		TestDeleteConfig(ConfigService configService, ConfigDirReader configDirReader, IPlatformServiceInstance platformServiceInstance) {
			super(configService, configDirReader, platformServiceInstance);
		}

		@Override
		public IValue execute(IValue... args) throws TimeOutException, ExecutionException {
			return super.deleteConfig(args[0].textValue(), args[1].textValue());
		}
	}
}
