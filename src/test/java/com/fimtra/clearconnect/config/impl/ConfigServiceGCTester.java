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

import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.IPlatformServiceProxy;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.config.IConfigServiceProxy;
import com.fimtra.clearconnect.core.PlatformRegistryAgent;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannelUtils;

/**
 * Will aggressively create, update and delete config in the {@link ConfigService} - use this to check it's performance using a tool like
 * VisualVM. By default this will run for 100000 configuration changes (cycles) in a non-random fashion (config will be changed in a
 * predicatable and ordered fashion).
 * <p>
 * The number of cycles can be controlled by passing it in the 1st argument of {@link ConfigServiceGCTester#main(String[])}
 * <p>
 * If a 2nd argument is used in {@link ConfigServiceGCTester#main(String[])} then this will operate in a random mode where config is changed
 * randomly.
 * 
 * <p>
 * To use
 * <li>
 * start a CC registry on {@link TcpChannelUtils#LOCALHOST_IP}</li>
 * <li>
 * start {@link ConfigServiceGCTester} with appropriate arguments</li>
 * <p>
 * 
 * @author Paul Mackinlay
 */
public class ConfigServiceGCTester {

	static {
		System.setProperty("dataFission.proxyReconnectPeriodMillis", "200");
	}

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
	private static final boolean isWin = (System.getProperty("os.name").toLowerCase().indexOf("win") >= 0);
	private static int maxCycles;
	private static long startTimestamp;

	private final boolean isRandom;
	private final ConfigService configService;
	private final ConfigServiceProxy configServiceProxy;
	private final IPlatformRegistryAgent agent;
	double cycles;

	ConfigServiceGCTester(boolean isRandom) {
		try {
			deleteDir("logs");
			deleteDir("config");
			this.cycles = 0d;
			this.isRandom = isRandom;
			this.configService = new ConfigService(TcpChannelUtils.LOCALHOST_IP, PlatformCoreProperties.Values.REGISTRY_PORT);
			this.agent = new PlatformRegistryAgent(getClass().getSimpleName(), TcpChannelUtils.LOCALHOST_IP);
			this.agent.waitForPlatformService(IConfigServiceProxy.CONFIG_SERVICE);
			IPlatformServiceProxy platformServiceProxy = this.agent.getPlatformServiceProxy(IConfigServiceProxy.CONFIG_SERVICE);
			this.configServiceProxy = new ConfigServiceProxy(platformServiceProxy);
			System.out.println(new StringBuffer("Is random: ").append(this.isRandom)
					.append("\nTake heap dump and then press any key to continue.").toString());
			System.in.read();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	void stop() {
		System.out.println(new StringBuffer("Total cycles: ").append(this.cycles).append("\n").append("Millis to complete: ")
				.append(System.currentTimeMillis() - startTimestamp).append("\n")
				.append("Take heap dump and then press any key to continue.").toString());
		try {
			System.in.read();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		this.configService.destroy();
		deleteDir("config");
		System.exit(0);
	}

	void start() {
		startTimestamp = System.currentTimeMillis();
		// Fully populate config
		for (ServiceInstance serviceInstance : ServiceInstance.values()) {
			addMemberConfig(serviceInstance, getRandomKey(), getRandomValue());
			addFamilyConfig(serviceInstance, getRandomKey(), getRandomValue());
		}

		while (true) {
			if (this.isRandom) {
				// Randomly change config
				switch (random.nextInt(3)) {
				case 0:
					addMemberConfig(getRandomServiceInstance(), getRandomKey(), getRandomValue());
					break;
				case 1:
					addFamilyConfig(getRandomServiceInstance(), getRandomKey(), getRandomValue());
					break;
				case 2:
					deleteMemberConfig(getRandomServiceInstance(), getRandomKey());
					break;
				default:
					deleteFamilyConfig(getRandomServiceInstance(), getRandomKey());
					break;
				}
				incrementCycle();
			} else {
				for (ServiceInstance serviceInstance : ServiceInstance.values()) {
					for (String key : configKeys) {
						for (TextValue value : configValues) {
							addMemberConfig(serviceInstance, key, value);
							incrementCycle();
							addFamilyConfig(serviceInstance, key, value);
							incrementCycle();
							deleteMemberConfig(serviceInstance, key);
							incrementCycle();
							deleteFamilyConfig(serviceInstance, key);
							incrementCycle();
						}
					}
				}
			}
		}
	}

	private void deleteMemberConfig(ServiceInstance serviceInstance, String k1) {
		this.configServiceProxy.getConfigManager(serviceInstance.family, serviceInstance.member).deleteMemberConfig(k1);
		pause();
	}

	private void addMemberConfig(ServiceInstance serviceInstance, String k1, TextValue v1) {
		this.configServiceProxy.getConfigManager(serviceInstance.family, serviceInstance.member).createOrUpdateMemberConfig(k1, v1);
		pause();
	}

	private void deleteFamilyConfig(ServiceInstance serviceInstance, String k1) {
		this.configServiceProxy.getConfigManager(serviceInstance.family, serviceInstance.member).deleteFamilyConfig(k1);
		pause();
	}

	private void addFamilyConfig(ServiceInstance serviceInstance, String k1, TextValue v1) {
		this.configServiceProxy.getConfigManager(serviceInstance.family, serviceInstance.member).createOrUpdateFamilyConfig(k1, v1);
		pause();
	}

	private void incrementCycle() {
		this.cycles++;
		if (this.cycles >= maxCycles) {
			stop();
		}
	}

	// let disk IO settle
	private static void pause() {
		try {
			Thread.sleep((isWin ? 10 : 1));
		} catch (InterruptedException e) {
			// ignore
		}
	}

	private static TextValue getRandomValue() {
		return configValues.get(random.nextInt(configValues.size()));
	}

	private static String getRandomKey() {
		return configKeys.get(random.nextInt(configKeys.size()));
	}

	private static ServiceInstance getRandomServiceInstance() {
		return ServiceInstance.values()[random.nextInt(ServiceInstance.values().length)];
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

	/**
	 * @see ConfigServiceGCTester
	 */
	public static void main(String[] args) {
		maxCycles = (args != null && args.length > 0) ? Integer.parseInt(args[0]) : 100000;
		boolean isRandom = (args != null && args.length > 1) ? true : false;
		final ConfigServiceGCTester tester = new ConfigServiceGCTester(isRandom);
		tester.start();
	}
}
