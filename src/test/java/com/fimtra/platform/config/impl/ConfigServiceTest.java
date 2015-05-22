/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config.impl;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.platform.IPlatformRegistryAgent;
import com.fimtra.platform.IPlatformServiceProxy;
import com.fimtra.platform.PlatformCoreProperties;
import com.fimtra.platform.config.IConfig;
import com.fimtra.platform.config.IConfig.IConfigChangeListener;
import com.fimtra.platform.config.IConfigServiceProxy;
import com.fimtra.platform.core.PlatformRegistry;
import com.fimtra.platform.core.PlatformRegistryAgent;
import com.fimtra.platform.core.PlatformUtils;
import com.fimtra.platform.event.IRecordConnectionStatusListener;
import com.fimtra.platform.event.IServiceAvailableListener;

import static org.mockito.Matchers.eq;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link ConfigService}
 * 
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public class ConfigServiceTest {
	private final static String LOOPBACK = "127.0.0.1";
	private static final String MEMBER = "firing";
	private static final String SERVICE = "lasers";
	private static final int TIMEOUT = 6000;
	private static final int RECONNECT_TIMEOUT = 500;
	ConfigService candidate;
	PlatformRegistry registry;
	ConfigServiceProxy proxy;
	IPlatformRegistryAgent agent;

	static ConfigService createConfigService() {
		return new ConfigService(LOOPBACK, PlatformCoreProperties.Values.REGISTRY_PORT);
	}

	@Before
	public void setUp() throws IOException {
		deleteConfigDir();
		this.registry = new PlatformRegistry(getClass().getSimpleName(), LOOPBACK);
		this.candidate = createConfigService();
		this.agent = new PlatformRegistryAgent(getClass().getSimpleName(), LOOPBACK);
		this.agent.setRegistryReconnectPeriodMillis(RECONNECT_TIMEOUT);

		this.agent.waitForPlatformService(IConfigServiceProxy.CONFIG_SERVICE);

		IPlatformServiceProxy platformServiceProxy = this.agent.getPlatformServiceProxy(IConfigServiceProxy.CONFIG_SERVICE);
		platformServiceProxy.setReconnectPeriodMillis(RECONNECT_TIMEOUT);
		this.proxy = new ConfigServiceProxy(platformServiceProxy);

		// just in case - lets delete the config record we're using
		this.candidate.platformServiceInstance.deleteRecord(this.candidate.platformServiceInstance.getRecord(PlatformUtils
				.composePlatformServiceInstanceID(SERVICE, MEMBER)));
	}

	private static void deleteConfigDir() {
		File dir = new File("config");
		if (dir.exists() && dir.isDirectory()) {
			for (File file : dir.listFiles()) {
				file.delete();
			}
			dir.delete();
		}
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
		deleteConfigDir();
	}

	@Test
	public void testConfigUpdateAndReReadOnRestart() throws Exception {
		final IConfig config = this.proxy.getConfig(SERVICE, MEMBER);

		assertNotNull(config);

		final IConfigChangeListener listener = mock(IConfigChangeListener.class);
		config.addConfigChangeListener(listener);

		final TextValue v1 = new TextValue("value1");
		final String k1 = "key1";
		final String k2 = "key2";

		addMemberConfigAndVerify(listener, k1, v1);

		deleteMemberConfigAndVerify(listener, k1);

		addMemberConfigAndVerify(listener, k1, v1);

		addFamilyConfigAndVerify(listener, k2, v1);

		deleteFamilyConfigAndVerify(listener, k2);

		addFamilyConfigAndVerify(listener, k2, v1);

		final AtomicReference<CountDownLatch> availableLatch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
		final AtomicReference<CountDownLatch> unavailableLatch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
		this.agent.addServiceAvailableListener(new IServiceAvailableListener() {
			@Override
			public void onServiceUnavailable(String serviceFamily) {
				if (IConfigServiceProxy.CONFIG_SERVICE.equals(serviceFamily)) {
					unavailableLatch.get().countDown();
				}
			}

			@Override
			public void onServiceAvailable(String serviceFamily) {
				if (IConfigServiceProxy.CONFIG_SERVICE.equals(serviceFamily)) {
					availableLatch.get().countDown();
				}
			}
		});
		assertTrue(availableLatch.get().await(10, TimeUnit.SECONDS));

		availableLatch.set(new CountDownLatch(1));

		// destroy the config service, re-create it and check we can still execute the RPCs and get
		// changes
		this.candidate.destroy();
		this.registry.destroy();
		this.registry = new PlatformRegistry(getClass().getSimpleName(), LOOPBACK);

		assertTrue(unavailableLatch.get().await(10, TimeUnit.SECONDS));
		this.candidate = createConfigService();
		assertTrue(availableLatch.get().await(10, TimeUnit.SECONDS));

		assertEquals(v1, config.getProperty(k1));

		this.agent.waitForPlatformService(IConfigServiceProxy.CONFIG_SERVICE);

		// wait for the proxy to reconnect
		final AtomicReference<CountDownLatch> connected = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
		this.agent.getPlatformServiceProxy(IConfigServiceProxy.CONFIG_SERVICE).addRecordConnectionStatusListener(
				new IRecordConnectionStatusListener() {
					@Override
					public void onRecordDisconnected(String recordName) {
					}

					@Override
					public void onRecordConnected(String recordName) {
						connected.get().countDown();
					}

					@Override
					public void onRecordConnecting(String recordName) {
					}
				});
		assertTrue(connected.get().await(10, TimeUnit.SECONDS));

		assertEquals(v1, config.getProperty(k1));

		deleteMemberConfigAndVerify(listener, k1);

		addMemberConfigAndVerify(listener, k1, v1);

		deleteFamilyConfigAndVerify(listener, k2);

		addFamilyConfigAndVerify(listener, k2, v1);
	}

	void deleteMemberConfigAndVerify(final IConfigChangeListener listener, final String k1) {
		boolean delete = this.proxy.getConfigManager(SERVICE, MEMBER).deleteMemberConfig(k1);
		assertTrue(delete);
		verify(listener, timeout(TIMEOUT).atLeastOnce()).onPropertyChange(eq(k1), (IValue) eq(null));
		waitAndReset(listener);
	}

	void addMemberConfigAndVerify(final IConfigChangeListener listener, final String k1, final TextValue v1) {
		boolean added = this.proxy.getConfigManager(SERVICE, MEMBER).createOrUpdateMemberConfig(k1, v1);
		assertTrue(added);
		verify(listener, timeout(TIMEOUT).atLeastOnce()).onPropertyChange(eq(k1), eq(v1));
		waitAndReset(listener);
	}

	void deleteFamilyConfigAndVerify(final IConfigChangeListener listener, final String k1) {
		boolean delete = this.proxy.getConfigManager(SERVICE, MEMBER).deleteFamilyConfig(k1);
		assertTrue(delete);
		verify(listener, timeout(TIMEOUT).atLeastOnce()).onPropertyChange(eq(k1), (IValue) eq(null));
		waitAndReset(listener);
	}

	void addFamilyConfigAndVerify(final IConfigChangeListener listener, final String k1, final TextValue v1) {
		boolean added = this.proxy.getConfigManager(SERVICE, MEMBER).createOrUpdateFamilyConfig(k1, v1);
		assertTrue(added);
		verify(listener, timeout(TIMEOUT).atLeastOnce()).onPropertyChange(eq(k1), eq(v1));
		waitAndReset(listener);
	}

	private static void waitAndReset(IConfigChangeListener listener) {
		try {
			Thread.sleep(100);
			reset(listener);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
