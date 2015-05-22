package com.fimtra.platform.rpc;

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
	private static int megaByteFactor = 1024 * 1024;

	@Override
	public IValue execute(IValue... args) throws TimeOutException, ExecutionException {
		Runtime runtime = Runtime.getRuntime();
		int processorCount = runtime.availableProcessors();
		long freeMemoryMegaBytes = runtime.freeMemory() / megaByteFactor;
		long maxMemoryMegaBytpes = runtime.maxMemory() / megaByteFactor;
		long totalMemoryMegaBytes = runtime.totalMemory() / megaByteFactor;
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
		return new TextValue(statsBuilder.toString());
	}
}
