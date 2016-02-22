/*
 * Copyright (c) 2016 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.tcpchannel.TcpChannelUtils;

/**
 * Utilities used for bootstrapping.
 * 
 * @author Paul Mackinlay
 */
public abstract class BootstrapUtil {

	private final static String sysKeyHomeDir = "user.home";
	private final static String sysKeyWorkingDir = "user.dir";
	private final static String sysKeyFileSeparator = "file.separator";

	private final static String initKeyInitFileName = "boot.initFilename";
	private final static String initKeyRegstryEndPointAddresses = "boot.registryEndPointsAddresses";
	private static final char colonDelimiter = ':';
	private static final char commaDelimiter = ',';
	private static final String fileExtProperties = ".properties";
	private static final String dot = ".";

	private BootstrapUtil() {
		// no access outside this class
	}

	/**
	 * Returns the registry {@link EndPointAddress}es by detecting them in this order:
	 * <ol>
	 * <li>the system property with key <i>boot.registryEndPointsAddresses</i></li>
	 * <li>the property with key <i>boot.registryEndPointsAddresses</i> in the initProperties parameter</li>
	 * <li>using the localhost IP address and the default registry port</li>
	 * </ol>
	 * Where the {@link EndPointAddress}es are a property, the value should have the following format:
	 * 
	 * <pre>
	 * host1:port1,host2:port2...
	 * </pre>
	 */
	public static EndPointAddress[] getRegistryEndPoints(Properties initProperties) {
		String registryConfigString = System.getProperty(initKeyRegstryEndPointAddresses);
		if (registryConfigString == null) {
			registryConfigString = initProperties.getProperty(initKeyRegstryEndPointAddresses);
		}
		List<EndPointAddress> registryEndPoints = new ArrayList<EndPointAddress>();
		if (registryConfigString == null) {
			registryEndPoints.add(new EndPointAddress(TcpChannelUtils.LOCALHOST_IP, PlatformCoreProperties.Values.REGISTRY_PORT));
		} else {
			List<String> registryConfigs = StringUtils.split(registryConfigString, commaDelimiter);
			for (String registryConfig : registryConfigs) {
				List<String> registryParts = StringUtils.split(registryConfig, colonDelimiter);
				registryEndPoints.add(new EndPointAddress(registryParts.get(0), Integer.parseInt(registryParts.get(1))));
			}
		}
		return registryEndPoints.toArray(new EndPointAddress[] {});
	}

	/**
	 * Returns the init filename by detecting it in this order:
	 * <ol>
	 * <li>the system property with key <i>boot.initFilename</i></li>
	 * <li>a file called .uiName.properties in the home directory where uiName is the method parameter</li>
	 * </ol>
	 */
	public static String getHomeDirInitFilename(String uiName) {
		return (System.getenv(initKeyInitFileName) == null
				? System.getProperty(sysKeyHomeDir) + System.getProperty(sysKeyFileSeparator) + dot + uiName + fileExtProperties
				: System.getProperty(initKeyInitFileName));
	}

	/**
	 * Returns the init filename by detecting it in this order:
	 * <ol>
	 * <li>the system property with key <i>boot.initFilename</i></li>
	 * <li>a file called serverName.properties in the working directory where serverName is the method parameter</li>
	 * </ol>
	 */
	public static String getWorkingDirInitFilename(String serverName) {
		return (System.getenv(initKeyInitFileName) == null
				? System.getProperty(sysKeyWorkingDir) + System.getProperty(sysKeyFileSeparator) + serverName + fileExtProperties
				: System.getProperty(initKeyInitFileName));
	}

	/**
	 * Returns the initial properties in the initFilename.
	 */
	public static Properties getInitProperties(String initFilename) {
		Properties initProperties = new Properties();
		InputStream inputStream = null;
		try {
			inputStream = BootstrapUtil.class.getResourceAsStream(initFilename);
			initProperties.load(inputStream);
		} catch (Exception e) {
			// no properties - will use defaults
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}
		return initProperties;
	}
}
