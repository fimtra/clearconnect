/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.benchmark;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue;
import com.fimtra.platform.IPlatformServiceInstance;
import com.fimtra.platform.PlatformServiceAccess;
import com.fimtra.platform.event.IRecordSubscriptionListener;
import com.fimtra.platform.event.IServiceAvailableListener;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.Log;
import com.fimtra.util.SystemUtils;
import com.fimtra.util.is;

/**
 * A service that benchmarks the platform and network environment. It requires other {@link EchoService} instances to be running, publishes
 * records and measures the latency for
 * receiving the record back from every echo service.
 * 
 * @author Ramon Servadei
 */
public class BenchmarkService {
	private static final int TIMEOUT_SECS = 10;

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		Log.log(BenchmarkService.class, "STARTING");

		NetworkTest.main(args);

		new BenchmarkService(TcpChannelUtils.LOOPBACK);
	}

	static final String PING_RECORD = "benchmarkRecord";
	private static final String SIGNATURE = "signature";
	static final String BENCHMARK_SERVICE = "BENCHMARK SERVICE";
	static final AtomicLong totalMessagesTx = new AtomicLong(), totalMessagesRx = new AtomicLong();

	final PlatformServiceAccess platformAccess;
	final IPlatformServiceInstance benchmarkService;

	public BenchmarkService(String registryHost) throws Exception {
		this.platformAccess = new PlatformServiceAccess(BENCHMARK_SERVICE, "primary", registryHost);
		this.benchmarkService = this.platformAccess.getPlatformServiceInstance();

		final Set<String> echoServiceNames = new HashSet<String>();
		this.platformAccess.getPlatformRegistryAgent().addServiceAvailableListener(new IServiceAvailableListener() {
			@Override
			public void onServiceUnavailable(String serviceFamily) {
				if (serviceFamily.startsWith(EchoService.ECHO_SERVICE)) {

				}
			}

			@Override
			public void onServiceAvailable(String serviceFamily) {
				if (serviceFamily.startsWith(EchoService.ECHO_SERVICE)) {
					echoServiceNames.add(serviceFamily);
				}
			}
		});

		Log.log(this, "Waiting 5 seconds for echo services discovery...");
		Thread.sleep(5000);

		// todo parameterise these
		final int maxRecords = 16;
		final int maxFields = 21;
		final int fieldIncrease = 5;
		final boolean singleThroughputTest = false;

		StringBuilder results = new StringBuilder();
		results.append("echoServiceCount, recordCount");
		for (int fieldCount = 1; fieldCount < maxFields; fieldCount += fieldIncrease) {
			results.append(",avgRtdLatencyMicros (").append(fieldCount).append(" fields)");
		}

		long start = System.nanoTime();

		if (singleThroughputTest) {
			final int runCount = 10000;
			for (int i = 0; i < 50; i++) {
				doEchoLatencyTest(echoServiceNames, 1, 1, runCount);
			}
		} else {
			final int runCount = 1000;
			// warm up
			for (int recordCount = 1; recordCount < 3; recordCount++) {
				for (int fieldCount = 1; fieldCount < maxFields; fieldCount += fieldIncrease) {
					doEchoLatencyTest(echoServiceNames, recordCount, fieldCount, runCount);
				}
			}

			for (int recordCount = 1; recordCount < maxRecords; recordCount++) {
				results.append(SystemUtils.lineSeparator());
				results.append(echoServiceNames.size()).append(",").append(recordCount);
				for (int fieldCount = 1; fieldCount < maxFields; fieldCount += fieldIncrease) {
					results.append(",").append(doEchoLatencyTest(echoServiceNames, recordCount, fieldCount, runCount));
				}
			}
		}
		long timeMicros = (long) (((double) (System.nanoTime() - start)) / 1000);

		results.append(SystemUtils.lineSeparator()).append("totalTx=").append(totalMessagesTx);
		results.append(SystemUtils.lineSeparator()).append("totalRx=").append(totalMessagesRx);
		results.append(SystemUtils.lineSeparator()).append("totalTimeSecs=").append((long) (((double) timeMicros) / 1000000));
		results.append(SystemUtils.lineSeparator()).append("txMsgsPerSec=")
				.append((long) (((double) totalMessagesTx.get() / timeMicros) * 1000000));
		results.append(SystemUtils.lineSeparator()).append("rxMsgsPerSec=")
				.append((long) (((double) totalMessagesRx.get() / timeMicros) * 1000000));
		results.append(SystemUtils.lineSeparator()).append("networkMsgsPerSec=")
				.append((long) (((double) (totalMessagesRx.get() + totalMessagesTx.get()) / timeMicros) * 1000000));
		IRecord connectionsRecords = this.benchmarkService.getRecord(ISystemRecordNames.CONTEXT_CONNECTIONS);
		Set<String> subMapKeys = connectionsRecords.getSubMapKeys();
		long kbPublished = 0;
		long msgsPublished = 0;
		for (String key : subMapKeys) {
			kbPublished += connectionsRecords.getOrCreateSubMap(key).get(IContextConnectionsRecordFields.KB_COUNT).longValue();
			msgsPublished += connectionsRecords.getOrCreateSubMap(key).get(IContextConnectionsRecordFields.MESSAGE_COUNT).longValue();
		}
		// NOTE: discrepancy between the publisher's recorded msgsPublished and totalMessagesTx is
		// probably due to sampling of when the stats are updated
		results.append(SystemUtils.lineSeparator()).append("kbPublished=").append(kbPublished);
		results.append(SystemUtils.lineSeparator()).append("msgsPublished=").append(msgsPublished);
		results.append(SystemUtils.lineSeparator()).append("avgMsgSizeBytes=")
				.append((long) (((double) kbPublished / msgsPublished) * 1024));
		Log.banner(this, results.toString());
		System.err.println(results.toString());
		System.err.println(System.getProperty("os.name") + " (" + System.getProperty("os.version") + "), " + System.getProperty("os.arch")
				+ ", Java " + System.getProperty("java.version") + ", cpus=" + Runtime.getRuntime().availableProcessors());
		System.err.println("FINISHED, PRESS A KEY TO TERMINATE...");
		System.in.read();
	}

	public void destroy() {
		this.platformAccess.destroy();
	}

	long doEchoLatencyTest(Set<String> echoServiceNames, final int maxRecordCount, final int maxFieldCount, final int runCount)
			throws InterruptedException {
		final int echoServiceCount = echoServiceNames.size();

		Log.log(this, "Test " + maxRecordCount + " records, " + maxFieldCount + " fields");

		final CountDownLatch totalUpdateCounter = new CountDownLatch(echoServiceCount * maxRecordCount * runCount);
		final List<Long> roundTripLatency = new CopyOnWriteArrayList<Long>();
		final ConcurrentMap<String, CountDownLatch> recordUpdateCounters = new ConcurrentHashMap<String, CountDownLatch>();
		final ConcurrentMap<String, String> recordSignatures = new ConcurrentHashMap<String, String>();

		String recordName;

		// setup the record specific update latches so we ensure each record us updated by the
		// specfied run count
		for (int recordCount = 0; recordCount < maxRecordCount; recordCount++) {
			recordName = PING_RECORD + recordCount;
			recordUpdateCounters.put(recordName, new CountDownLatch(runCount));
		}

		/*
		 * The listener that will measure latency and trigger a new ping record
		 */
		final IRecordListener echoListener = new IRecordListener() {
			@Override
			public void onChange(IRecord imageCopy, IRecordChange atomicChange) {
				final long time = System.nanoTime();

				totalMessagesRx.incrementAndGet();

				final IValue signatureField = imageCopy.get(SIGNATURE);
				if (signatureField != null && is.eq(signatureField.textValue(), recordSignatures.get(imageCopy.getName()))) {
					// Log.log(this, "Received " + atomicChange);
					roundTripLatency.add(Long.valueOf(time - imageCopy.get("field0").longValue()));
					final CountDownLatch recordUpdateCounter = recordUpdateCounters.get(imageCopy.getName());
					recordUpdateCounter.countDown();
					if (recordUpdateCounter.getCount() >= 0) {
						totalUpdateCounter.countDown();
						if (totalUpdateCounter.getCount() >= 0) {
							// trigger the update from the listener
							String signature = UUID.randomUUID().toString();
							recordSignatures.put(imageCopy.getName(), signature);
							doUpdate(maxFieldCount, signature, imageCopy.getName());
						}
					}
				}
			}
		};

		try {
			final AtomicReference<String> name = new AtomicReference<String>();

			/*
			 * SUBSCRIBE FOR THE PING RECORD IN THE ECHO SERVICE
			 */
			// for each echo service, add the echo record listener for each record we ping
			for (int recordCount = 0; recordCount < maxRecordCount; recordCount++) {
				recordName = PING_RECORD + recordCount;

				for (String serviceName : echoServiceNames) {
					this.platformAccess.getPlatformRegistryAgent().getPlatformServiceProxy(serviceName)
							.addRecordListener(echoListener, recordName);
				}
			}

			// create the ping record and ensure all echo services are subscribed for it
			for (int recordCount = 0; recordCount < maxRecordCount; recordCount++) {
				recordName = PING_RECORD + recordCount;
				name.set(recordName);

				// ensure the echo services have subscribed...
				final CountDownLatch subscribers = new CountDownLatch(1);

				final IRecordSubscriptionListener listener = new IRecordSubscriptionListener() {
					@Override
					public void onRecordSubscriptionChange(SubscriptionInfo subscriptionInfo) {
						if (subscriptionInfo.getRecordName().equals(name.get())
								&& subscriptionInfo.getCurrentSubscriberCount() == echoServiceCount) {
							subscribers.countDown();
						}
					}
				};

				this.benchmarkService.addRecordSubscriptionListener(listener);
				try {
					this.benchmarkService.getOrCreateRecord(recordName);
					if (!subscribers.await(TIMEOUT_SECS, TimeUnit.SECONDS)) {
						throw new IllegalStateException("Did not get subscription from all echo services for " + recordName + "..."
								+ subscribers.getCount());
					}
				} finally {
					this.benchmarkService.removeRecordSubscriptionListener(listener);
				}
			}

			// trigger the update stream
			for (int recordCount = 0; recordCount < maxRecordCount; recordCount++) {
				recordName = PING_RECORD + recordCount;
				String signature = UUID.randomUUID().toString();
				recordSignatures.put(recordName, signature);
				doUpdate(maxFieldCount, signature, recordName);
			}

			Log.log(this, "Waiting for " + totalUpdateCounter.getCount() + " echo responses from " + echoServiceCount + " echo services...");
			final boolean finished = totalUpdateCounter.await(TIMEOUT_SECS, TimeUnit.SECONDS);
			if (!finished) {
				throw new IllegalStateException("Did not get all notifications:" + totalUpdateCounter.getCount());
			}

			// now process the results
			double total = 0;
			for (Long latency : roundTripLatency) {
				total += latency.doubleValue();
			}
			final double avgLatencyNanos = total / roundTripLatency.size();
			final long avgLatencyMicros = (long) ((avgLatencyNanos / 1000d));
			return avgLatencyMicros;
		} finally {
			for (int recordCount = 0; recordCount < maxRecordCount; recordCount++) {
				recordName = PING_RECORD + recordCount;
				for (String serviceName : echoServiceNames) {
					this.platformAccess.getPlatformRegistryAgent().getPlatformServiceProxy(serviceName)
							.removeRecordListener(echoListener, recordName);
				}
			}
		}
	}

	void doUpdate(int maxFieldCount, final String signature, String recordName) {
		IRecord record = this.benchmarkService.getOrCreateRecord(recordName);
		for (int fieldCount = 0; fieldCount < maxFieldCount; fieldCount++) {
			record.put("field" + fieldCount, System.nanoTime());
		}
		record.put(SIGNATURE, signature);
		totalMessagesTx.incrementAndGet();
		this.benchmarkService.publishRecord(record);
	}
}
