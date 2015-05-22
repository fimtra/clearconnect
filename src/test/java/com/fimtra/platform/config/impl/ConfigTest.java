/*
 * Copyright (c) 2013 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config.impl;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.AtomicChange;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.platform.IPlatformServiceProxy;
import com.fimtra.platform.config.ConfigServiceProperties;
import com.fimtra.platform.config.IConfig.IConfigChangeListener;
import com.fimtra.platform.core.PlatformUtils;

import static org.mockito.Matchers.eq;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link Config}
 *
 * @author Paul Mackinlay
 */
public class ConfigTest {

	private IPlatformServiceProxy configServiceProxy;
	private String serviceName;
	private String serviceInstanceName;
	private Config config;

	@Before
	public void setUp() {
		this.serviceName = "testService";
		this.serviceInstanceName = "testServiceInstance";
		String configResource = "testServiceInstance";
		this.configServiceProxy = mock(IPlatformServiceProxy.class);
		this.config = new Config("./", configResource, this.serviceName, this.serviceInstanceName, this.configServiceProxy);
	}

	@Test
	public void shouldGetLocalProperties() {
		Map<String, IValue> localProperties = this.config.properties;
		assertEquals(2, localProperties.size());
		String key1 = "key1";
		String key2 = "key2";
		String value1 = "value1";
		String value2 = "value2";
		assertTrue(localProperties.containsKey(key1));
		assertTrue(localProperties.containsKey(key2));
		assertEquals(value1, localProperties.get(key1).textValue());
		assertEquals(value2, localProperties.get(key2).textValue());
	}

	@Test
	public void shouldNotifyOnceOnListenerAdd() {
		IConfigChangeListener listener = mock(IConfigChangeListener.class);
		this.config.addConfigChangeListener(listener);
		this.config.addConfigChangeListener(listener);
		String key1 = "key1";
		String key2 = "key2";
		String value1 = "value1";
		String value2 = "value2";
		verify(listener, times(1)).onPropertyChange(key1, TextValue.valueOf(value1));
		verify(listener, times(1)).onPropertyChange(key2, TextValue.valueOf(value2));
	}

	@SuppressWarnings("boxing")
	@Test
	public void shouldNotNofityOverwrittenConfig() {
		String serviceInstanceId = PlatformUtils.composePlatformServiceInstanceID(this.serviceName, this.serviceInstanceName);
		IRecord memberRecord = mock(IRecord.class);
		when(this.configServiceProxy.getRecordImage(serviceInstanceId, ConfigServiceProperties.Values.DEFAULT_CONFIG_RPC_TIMEOUT_MILLIS))
				.thenReturn(memberRecord);
		IRecord configRecord = mock(IRecord.class);
		when(configRecord.getName()).thenReturn(this.serviceName);
		IConfigChangeListener listener = mock(IConfigChangeListener.class);
		this.config.addConfigChangeListener(listener);
		Map<String, IValue> removedEntries = new HashMap<String, IValue>();
		final TextValue v9 = new TextValue("config 1 removed");
		String removeKey = "rkey1";
		removedEntries.put(removeKey, v9);
		when(memberRecord.containsKey(removeKey)).thenReturn(Boolean.TRUE);

		Map<String, IValue> putEntries = new HashMap<String, IValue>();
		String key1 = "key1";
		String key2 = "key2";
		String value1 = "config 1";
		String value2 = "config 2";
		final TextValue config1 = new TextValue(value1);
		when(memberRecord.containsKey(key1)).thenReturn(Boolean.FALSE);
		when(memberRecord.containsKey(key2)).thenReturn(Boolean.TRUE);
		putEntries.put(key1, config1);
		final TextValue config2 = new TextValue(value2);
		putEntries.put(key2, config2);
		IRecordChange atomicChange = new AtomicChange(this.serviceName, putEntries, null, removedEntries);
		this.config.masterConfigChangeListener.onChange(configRecord, atomicChange);

		verify(listener, times(1)).onPropertyChange(eq(key1), eq(config1));
	}

	@Test
	public void testListenerNotification() {
		IConfigChangeListener listener = mock(IConfigChangeListener.class);
		this.config.addConfigChangeListener(listener);
		IConfigChangeListener listener2 = mock(IConfigChangeListener.class);
		this.config.addConfigChangeListener(listener2);

		// trigger an update
		Map<String, IValue> removedEntries = new HashMap<String, IValue>();
		final TextValue v9 = new TextValue("v9");
		removedEntries.put("key1", v9);
		Map<String, IValue> putEntries = new HashMap<String, IValue>();
		final TextValue v1 = new TextValue("v1");
		putEntries.put("k1", v1);
		final TextValue v2 = new TextValue("v2");
		putEntries.put("k2", v2);
		IRecordChange atomicChange = new AtomicChange("sdf1", putEntries, null, removedEntries);
		this.config.masterConfigChangeListener.onChange(null, atomicChange);

		// remove listener
		this.config.removeConfigChangeListener(listener);

		// trigger another update (its a duplicate but nevermind!)
		removedEntries = new HashMap<String, IValue>();
		removedEntries.put("key2", v9);
		putEntries = new HashMap<String, IValue>();
		putEntries.put("k1", v1);
		final TextValue v22 = new TextValue("v2.2");
		putEntries.put("k2", v22);
		atomicChange = new AtomicChange("sdf1", putEntries, null, removedEntries);
		this.config.masterConfigChangeListener.onChange(null, atomicChange);

		verify(listener, times(1)).onPropertyChange(eq("k1"), eq(v1));
		verify(listener, times(1)).onPropertyChange(eq("k2"), eq(v2));
		verify(listener, times(1)).onPropertyChange(eq("key1"), (IValue) eq(null));
		verify(listener2, times(1)).onPropertyChange(eq("k1"), eq(v1));
		verify(listener2, times(1)).onPropertyChange(eq("k2"), eq(v2));
		verify(listener2, times(1)).onPropertyChange(eq("k2"), eq(v22));
		verify(listener2, times(1)).onPropertyChange(eq("key1"), (IValue) eq(null));
		verify(listener2, times(1)).onPropertyChange(eq("key2"), (IValue) eq(null));

		assertEquals(this.config.properties, putEntries);
	}

	@After
	public void tearDown() {
		File dir = new File("logs");
		if (dir.exists() && dir.isDirectory()) {
			for (File file : dir.listFiles()) {
				file.delete();
			}
			dir.delete();
		}
	}
}