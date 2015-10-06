/*
 * Copyright (c) 2015 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.clearconnect.config.impl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.IPlatformServiceProxy;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.config.IConfigServiceProxy;
import com.fimtra.clearconnect.core.PlatformRegistry;
import com.fimtra.clearconnect.core.PlatformRegistryAgent;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.ThreadUtils;

/**
 * @author Paul Mackinlay
 */
public class ConfigServiceGCTester {

	enum ServiceInstance {
		A("service1", "member1"), B("service1", "member2"), C("service1", "member3"), D("service2", "member1"), E("service3", "member1"), F(
				"service4", "member1"), G("service4", "member2"), H("service4", "member3"), I("service4", "member4"), J("service5",
				"member1"), K("service5", "member2"), L("service6", "member1"), M("service7", "member1"), N("service8", "member1"), O(
				"service8", "member2"), P("service8", "member3"), Q("service9", "member1"), R("service10", "member1"), S("service10",
				"member2"), T("service11", "member1");

		final String family;
		final String member;

		private ServiceInstance(String family, String member) {
			this.family = family;
			this.member = member;
		}
	}

	private static final List<String> configKeys = Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9",
			"key10", "key11", "key12", "key13", "key14", "key15", "key16", "key17", "key18", "key19", "key20", "key21", "key22", "key23",
			"key24", "key25", "key26", "key27", "key28", "key29", "key30", "key31", "key32", "key33", "key34", "key35", "key36", "key37",
			"key38", "key39", "key40", "key41", "key42", "key43", "key44", "key45", "key46", "key47", "key48", "key49", "key50");
	private static final List<TextValue> configValues = Arrays.asList(TextValue.valueOf("value1"), TextValue.valueOf("value2"),
			TextValue.valueOf("value3"), TextValue.valueOf("value4"), TextValue.valueOf("value5"), TextValue.valueOf("value6"),
			TextValue.valueOf("value7"), TextValue.valueOf("value8"), TextValue.valueOf("value9"), TextValue.valueOf("value10"),
			TextValue.valueOf("value11"), TextValue.valueOf("value12"), TextValue.valueOf("value13"), TextValue.valueOf("value14"),
			TextValue.valueOf("value15"), TextValue.valueOf("value16"), TextValue.valueOf("value17"), TextValue.valueOf("value18"),
			TextValue.valueOf("value19"), TextValue.valueOf("value20"), TextValue.valueOf("value21"), TextValue.valueOf("value22"),
			TextValue.valueOf("value23"), TextValue.valueOf("value24"), TextValue.valueOf("value25"), TextValue.valueOf("value26"),
			TextValue.valueOf("value27"), TextValue.valueOf("value28"), TextValue.valueOf("value29"), TextValue.valueOf("value30"));
	private static final Random random = new Random();

	private final CountDownLatch stopLatch;
	private final ConfigService configService;
	private final PlatformRegistry registry;
	private final ConfigServiceProxy configServiceProxy;
	private final IPlatformRegistryAgent agent;

	ConfigServiceGCTester() {
		try {
			deleteDir("config");
			this.stopLatch = new CountDownLatch(1);
			this.registry = new PlatformRegistry(getClass().getSimpleName(), TcpChannelUtils.LOCALHOST_IP);
			this.configService = new ConfigService(TcpChannelUtils.LOCALHOST_IP, PlatformCoreProperties.Values.REGISTRY_PORT);
			this.agent = new PlatformRegistryAgent(getClass().getSimpleName(), TcpChannelUtils.LOCALHOST_IP);
			this.agent.waitForPlatformService(IConfigServiceProxy.CONFIG_SERVICE);
			IPlatformServiceProxy platformServiceProxy = this.agent.getPlatformServiceProxy(IConfigServiceProxy.CONFIG_SERVICE);
			this.configServiceProxy = new ConfigServiceProxy(platformServiceProxy);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	void stop() {
		this.stopLatch.countDown();
		this.configService.destroy();
		this.registry.destroy();
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			// ignore
		}
		deleteDir("logs");
		deleteDir("config");
	}

	void start() {
		// Fully populate config
		for (ServiceInstance serviceInstance : ServiceInstance.values()) {
			for (String configKey : configKeys) {
				addMemberConfig(serviceInstance, configKey, getRandomValue());
				addFamilyConfig(serviceInstance, configKey, getRandomValue());
			}
		}

		long cycles = 0l;
		// Randomly change config
		while (this.stopLatch.getCount() == 1) {
			switch (random.nextInt(3)) {
			case 0:
				addMemberConfig(ServiceInstance.A, getRandomKey(), getRandomValue());
				break;
			case 1:
				addFamilyConfig(ServiceInstance.A, getRandomKey(), getRandomValue());
				break;
			case 2:
				deleteMemberConfig(ServiceInstance.A, getRandomKey());
				break;
			default:
				deleteFamilyConfig(ServiceInstance.A, getRandomKey());
				break;
			}
			cycles++;
		}
		System.out.println(cycles);
	}

	private void deleteMemberConfig(ServiceInstance serviceInstance, String k1) {
		this.configServiceProxy.getConfigManager(serviceInstance.family, serviceInstance.member).deleteMemberConfig(k1);
	}

	private void addMemberConfig(ServiceInstance serviceInstance, String k1, TextValue v1) {
		this.configServiceProxy.getConfigManager(serviceInstance.family, serviceInstance.member).createOrUpdateMemberConfig(k1, v1);
	}

	private void deleteFamilyConfig(ServiceInstance serviceInstance, String k1) {
		this.configServiceProxy.getConfigManager(serviceInstance.family, serviceInstance.member).deleteFamilyConfig(k1);
	}

	private void addFamilyConfig(ServiceInstance serviceInstance, String k1, TextValue v1) {
		this.configServiceProxy.getConfigManager(serviceInstance.family, serviceInstance.member).createOrUpdateFamilyConfig(k1, v1);
	}

	private static TextValue getRandomValue() {
		return configValues.get(random.nextInt(configValues.size()));
	}

	private static String getRandomKey() {
		return configKeys.get(random.nextInt(configKeys.size()));
	}

	private static void deleteDir(String dirName) {
		File dir = new File(dirName);
		if (dir.exists() && dir.isDirectory()) {
			for (File file : dir.listFiles()) {
				file.delete();
			}
			dir.delete();
		}
	}

	public static void main(String[] args) {
		int timeoutSecs = (args != null && args.length > 0) ? Integer.parseInt(args[0]) : 600;
		final ConfigServiceGCTester tester = new ConfigServiceGCTester();
		ThreadUtils.newScheduledExecutorService(ConfigServiceGCTester.class.getSimpleName(), 1).schedule(new Runnable() {
			@Override
			public void run() {
				tester.stop();
			}
		}, timeoutSecs, TimeUnit.SECONDS);
		tester.start();
	}
}
