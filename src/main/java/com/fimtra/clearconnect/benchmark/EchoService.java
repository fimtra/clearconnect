/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
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
package com.fimtra.clearconnect.benchmark;

import java.io.IOException;
import java.util.UUID;

import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.IPlatformServiceProxy;
import com.fimtra.clearconnect.PlatformServiceAccess;
import com.fimtra.clearconnect.event.IRecordAvailableListener;
import com.fimtra.clearconnect.event.IServiceAvailableListener;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.Log;
import com.fimtra.util.is;

/**
 * Echos back records published from a {@link BenchmarkService}
 * 
 * @author Ramon Servadei
 */
public class EchoService {
	@SuppressWarnings("unused")
	public static void main(String[] args) throws IOException {
		new EchoService(TcpChannelUtils.LOOPBACK);
		System.in.read();
	}

	static final String ECHO_SERVICE = "ECHO SERVICE";

	final PlatformServiceAccess platformAccess;
	final IPlatformServiceInstance echoService;
	final IRecordListener echoBackListener;
	final IRecordAvailableListener recordAvailableListener;

	IPlatformServiceProxy benchmarkServiceProxy;

	public EchoService(String registryHost) {
		// NOTE: each SERVICE FAMILY must be unique as the benchmarking needs to connect to each
		// individual service instance
		this.platformAccess = new PlatformServiceAccess(ECHO_SERVICE + "-" + UUID.randomUUID().toString(), "", registryHost);
		this.echoService = this.platformAccess.getPlatformServiceInstance();
		final IPlatformRegistryAgent agent = this.platformAccess.getPlatformRegistryAgent();

		this.echoBackListener = new IRecordListener() {
			@Override
			public void onChange(IRecord imageCopy, IRecordChange atomicChange) {
				if (imageCopy.getName().startsWith(BenchmarkService.PING_RECORD)) {
					// Log.log(this, "Received " + atomicChange);
					// copy and echo back the received record
					final IRecord localRecord = EchoService.this.echoService.getOrCreateRecord(imageCopy.getName());
					localRecord.clear();
					localRecord.putAll(imageCopy);
					EchoService.this.echoService.publishRecord(localRecord);
				}
			}
		};
		this.recordAvailableListener = new IRecordAvailableListener() {
			@Override
			public void onRecordUnavailable(String recordName) {
				EchoService.this.benchmarkServiceProxy.removeRecordListener(EchoService.this.echoBackListener, recordName);
			}

			@Override
			public void onRecordAvailable(String recordName) {
				EchoService.this.echoService.getOrCreateRecord(recordName);
				EchoService.this.benchmarkServiceProxy.addRecordListener(EchoService.this.echoBackListener, recordName);
			}
		};

		agent.addServiceAvailableListener(new IServiceAvailableListener() {
			@Override
			public void onServiceUnavailable(String serviceFamily) {
				if (is.eq(BenchmarkService.BENCHMARK_SERVICE, serviceFamily)) {
					EchoService.this.platformAccess.getPlatformRegistryAgent().destroyPlatformServiceProxy(serviceFamily);
					EchoService.this.benchmarkServiceProxy = null;
				}
			}

			@Override
			public void onServiceAvailable(String serviceFamily) {
				if (is.eq(BenchmarkService.BENCHMARK_SERVICE, serviceFamily)) {
					Log.log(EchoService.this, "Found " + serviceFamily);
					EchoService.this.benchmarkServiceProxy = agent.getPlatformServiceProxy(serviceFamily);
					EchoService.this.benchmarkServiceProxy.addRecordAvailableListener(EchoService.this.recordAvailableListener);
				}
			}
		});
	}
}
