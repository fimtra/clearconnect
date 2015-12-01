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
package com.fimtra.clearconnect.rpc;

import java.util.Properties;
import java.util.TreeSet;

import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.util.SystemUtils;

/**
 * An {@link IRpcExecutionHandler} that gets JVM statistics and returns then as a {@link TextValue}.
 * 
 * @author Paul Mackinlay
 */
public class RpcGetJvmStatsHandler implements IRpcExecutionHandler {

	private static final String sysPropertiesLabel = "System properties";
	private static final String noProcessorsLabel = "Number of processes: ";
	private static final String freeMemoryLabel = "Free memory (MB): ";
	private static final String maxMemoryLabel = "Max memory (MB): ";
	private static final String totalMemoryLabel = "Total memory (MB): ";
	private static double megaByteFactor = 1d / (1024 * 1024);

	@Override
	public IValue execute(IValue... args) throws TimeOutException, ExecutionException {
		Runtime runtime = Runtime.getRuntime();
		int processorCount = runtime.availableProcessors();
		long freeMemoryMegaBytes = (long) (runtime.freeMemory() * megaByteFactor);
		long maxMemoryMegaBytpes = (long) (runtime.maxMemory() * megaByteFactor);
		long totalMemoryMegaBytes = (long) (runtime.totalMemory() * megaByteFactor);
		Properties systemProperties = System.getProperties();

		StringBuilder statsBuilder = new StringBuilder();
		statsBuilder.append(totalMemoryLabel).append(totalMemoryMegaBytes).append(SystemUtils.lineSeparator());
		statsBuilder.append(maxMemoryLabel).append(maxMemoryMegaBytpes).append(SystemUtils.lineSeparator());
		statsBuilder.append(freeMemoryLabel).append(freeMemoryMegaBytes).append(SystemUtils.lineSeparator());
		statsBuilder.append(noProcessorsLabel).append(processorCount).append(SystemUtils.lineSeparator());
		statsBuilder.append(SystemUtils.lineSeparator()).append(sysPropertiesLabel).append(SystemUtils.lineSeparator());
		for (String systemPropertyKey : new TreeSet<String>(systemProperties.stringPropertyNames())) {
			statsBuilder.append(systemPropertyKey).append(": ").append(systemProperties.getProperty(systemPropertyKey))
					.append(SystemUtils.lineSeparator());
		}
		return TextValue.valueOf(statsBuilder.toString());
	}
}
