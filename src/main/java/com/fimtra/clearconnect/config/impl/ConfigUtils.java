/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei, Fimtra
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

import java.io.IOException;

import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.IPlatformServiceInstance;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.config.ConfigProperties;
import com.fimtra.clearconnect.config.IConfig;
import com.fimtra.clearconnect.core.PlatformUtils;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannelUtils;

/**
 * Utilities for the config service.
 * 
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
public abstract class ConfigUtils {

	private ConfigUtils() {
	}

	/**
	 * Gets the port from config or uses a default.
	 */
	public synchronized static int getPort(IConfig config, String host) {
		IValue portProperty = config.getProperty(ConfigProperties.CONFIG_KEY_INSTANCE_PORT);
		if (isEmptyConfigProperty(portProperty)) {
			return PlatformUtils.getNextFreeDefaultTcpServerPort(host);
		}
		return Integer.parseInt(portProperty.textValue());
	}

	/**
	 * Gets the hostname from config or uses a default.
	 */
	public static String getHost(IConfig config) {
		IValue hostProperty = config.getProperty(ConfigProperties.CONFIG_KEY_INSTANCE_HOST);
		if (isEmptyConfigProperty(hostProperty)) {
			return TcpChannelUtils.LOCALHOST_IP;
		}
		return hostProperty.textValue();
	}

	/**
	 * Gets the redundancy mode from config or uses a default.
	 */
	public static RedundancyModeEnum getRedundancyMode(IConfig config) {
		IValue redundancyModeProperty = config.getProperty(ConfigProperties.CONFIG_KEY_INSTANCE_REDUNDANCY_MODE);
		if (!isEmptyConfigProperty(redundancyModeProperty)) {
			return RedundancyModeEnum.valueOf(redundancyModeProperty.textValue());
		}
		return RedundancyModeEnum.FAULT_TOLERANT;
	}

	/**
	 * Gets the wire protocol from config or uses a default.
	 */
	public static WireProtocolEnum getWireProtocol(IConfig config) {
		IValue wireProtocolProperty = config.getProperty(ConfigProperties.CONFIG_KEY_INSTANCE_WIRE_PROTOCOL);
		if (!isEmptyConfigProperty(wireProtocolProperty)) {
			return WireProtocolEnum.valueOf(wireProtocolProperty.textValue());
		}
		return WireProtocolEnum.STRING;
	}

	private static boolean isEmptyConfigProperty(IValue configProperty) {
		return (configProperty == null || configProperty.textValue().isEmpty());
	}

	/**
	 * Convenience method to get a platform service instance. This will create the instance if it
	 * does not exist. The {@link IConfig} object is used to obtain the parameters for constructing
	 * the service instance, if these are not in the configuration then suitable defaults are used.
	 * The following table lists the configuration properties and the defaults:
	 * <table border="1">
	 * <tr>
	 * <th>property</th>
	 * <th>type</th>
	 * <th>meaning</th>
	 * <th>default</th>
	 * </tr>
	 * <tr>
	 * <td>{@link ConfigProperties#CONFIG_KEY_INSTANCE_PORT}</td>
	 * <td>{@link LongValue}</td>
	 * <td>The TCP server port for the service</td>
	 * <td>The first free port on the local host between
	 * {@link PlatformCoreProperties#TCP_SERVER_PORT_RANGE_START} and
	 * {@link PlatformCoreProperties#TCP_SERVER_PORT_RANGE_END}</td>
	 * </tr>
	 * <tr>
	 * <td>{@link ConfigProperties#CONFIG_KEY_INSTANCE_HOST}</td>
	 * <td>{@link TextValue}</td>
	 * <td>The hostname to use for the service</td>
	 * <td>The canonical localhost's name {@link TcpChannelUtils#LOCALHOST_IP}</td>
	 * </tr>
	 * <tr>
	 * <td>{@link ConfigProperties#CONFIG_KEY_INSTANCE_WIRE_PROTOCOL}</td>
	 * <td>{@link TextValue}</td>
	 * <td>The wire protocol to use, one of the {@link WireProtocolEnum} strings</td>
	 * <td>{@link WireProtocolEnum#STRING}</td>
	 * </tr>
	 * <tr>
	 * <td>{@link ConfigProperties#CONFIG_KEY_INSTANCE_REDUNDANCY_MODE}</td>
	 * <td>{@link TextValue}</td>
	 * <td>The redundancy mode for the service, one of the {@link RedundancyModeEnum} strings</td>
	 * <td>{@link RedundancyModeEnum#FAULT_TOLERANT}</td>
	 * </tr>
	 * </table>
	 * 
	 * @param serviceName
	 *            the name of the service; a service may have many supporting instances
	 * @param serviceMemberName
	 *            the name of this member in the service; this is unique only within the service
	 *            (e.g. serviceName=TimeService, serviceMemberName=PRIMARY, SECONDARY etc.)
	 * @param config
	 *            the configuration for this service name and member
	 * @param agent
	 *            the agent to construct the service instance
	 * @return the platform service instance
	 * @throws IOException
	 */
	public synchronized static IPlatformServiceInstance getPlatformServiceInstance(String serviceName, String serviceMemberName, IConfig config,
			IPlatformRegistryAgent agent) {
		String host = ConfigUtils.getHost(config);
		int port = ConfigUtils.getPort(config, host);
		return getPlatformServiceInstanceInternal(serviceName, serviceMemberName, config, agent, host, port);
	}

	public synchronized static IPlatformServiceInstance getPlatformServiceInstance(String serviceName, IConfig config, IPlatformRegistryAgent agent) {
		String host = ConfigUtils.getHost(config);
		int port = ConfigUtils.getPort(config, host);
		String serviceMemberName = host + ":" + port;
		return getPlatformServiceInstanceInternal(serviceName, serviceMemberName, config, agent, host, port);
	}

	static IPlatformServiceInstance getPlatformServiceInstanceInternal(String serviceName, String serviceMemberName,
			IConfig config, IPlatformRegistryAgent agent, String host, int port) {
		WireProtocolEnum wireProtocol = ConfigUtils.getWireProtocol(config);
		RedundancyModeEnum redundancyMode = ConfigUtils.getRedundancyMode(config);
		boolean isPlatformServiceCreated = agent.createPlatformServiceInstance(serviceName, serviceMemberName, host, port, wireProtocol,
				redundancyMode);
		if (isPlatformServiceCreated) {
			return agent.getPlatformServiceInstance(serviceName, serviceMemberName);
		}
		throw new RuntimeException("Unable to create platform service instance for serviceName=" + serviceName + ", memberName="
				+ serviceMemberName + ", host=" + host + ", port=" + port + ", wireProtocol=" + wireProtocol + ", redundancyMode="
				+ redundancyMode);
	}

	/**
	 * Gets the property with propertyKey as an int. If it is not configured or value cannot be parsed, defaultValue is returned.
	 */
	public static int getPropertyAsInt(IConfig config, String propertyKey, int defaultValue) {
		IValue iValue = config.getProperty(propertyKey);
		int returnValue = defaultValue;
		if (iValue != null) {
			String text = iValue.textValue();
			if (!text.isEmpty()) {
				try {
					returnValue = Integer.parseInt(text);
				} catch (Exception e) {
					// no op
				}
			}
		}
		return returnValue;
	}

}