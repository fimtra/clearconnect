/*
 * Copyright (c) 2016 Paul Mackinlay, Fimtra
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
public abstract class BootstrapUtils {

	private final static String sysKeyHomeDir = "user.home";
	private final static String sysKeyWorkingDir = "user.dir";
	private final static String sysKeyFileSeparator = "file.separator";

	private final static String initKeyInitFileName = "boot.initFilename";
	private final static String initKeyRegstryEndPointAddresses = "boot.registryEndPointAddresses";
	private static final char colonDelimiter = ':';
	private static final char commaDelimiter = ',';
	private static final String fileExtProperties = ".properties";
	private static final String dot = ".";

	private BootstrapUtils() {
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
	 * 
	 * @see TcpChannelUtils#LOCALHOST_IP
	 * @see PlatformCoreProperties.Values#REGISTRY_PORT
	 */
	public static EndPointAddress[] getRegistryEndPoints(Properties initProperties) {
		String registryConfigString = System.getProperty(initKeyRegstryEndPointAddresses);
		if (registryConfigString == null) {
			registryConfigString = initProperties.getProperty(initKeyRegstryEndPointAddresses);
		}
		List<EndPointAddress> registryEndPoints = new ArrayList<EndPointAddress>();
		if (registryConfigString == null || registryConfigString.isEmpty()) {
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
	 * <li>a file called '.initFile.properties' in the home directory where initFile is the method parameter</li>
	 * </ol>
	 */
	public static String getHomeDirInitFilename(String initFile) {
		return (System.getProperty(initKeyInitFileName) == null
				? System.getProperty(sysKeyHomeDir) + System.getProperty(sysKeyFileSeparator) + dot + initFile + fileExtProperties
				: System.getProperty(initKeyInitFileName));
	}

	/**
	 * Returns the init filename by detecting it in this order:
	 * <ol>
	 * <li>the system property with key <i>boot.initFilename</i></li>
	 * <li>a file called 'serverName.properties' in the working directory where serverName is the method parameter</li>
	 * </ol>
	 */
	public static String getWorkingDirInitFilename(String serverName) {
		return (System.getProperty(initKeyInitFileName) == null
				? System.getProperty(sysKeyWorkingDir) + System.getProperty(sysKeyFileSeparator) + serverName + fileExtProperties
				: System.getProperty(initKeyInitFileName));
	}

	/**
	 * Returns the initial properties in the initFilename. If will detect location of the initFilename by:
	 * <ol>
	 * <li>using the filesystem</li>
	 * <li>using the classloader (searches within the classpath), this will also find files packaged within a jar</li>
	 * </ol>
	 */
	public static Properties getInitProperties(String initFilename) {
		Properties initProperties = new Properties();
		InputStream inputStream = null;
		try {
			try {
				inputStream = new FileInputStream(initFilename);
			} catch (FileNotFoundException e) {
				// ignore
			}
			if (inputStream == null) {
				// Not on the filesystem - will try the classpath (possibly within a jar)
				inputStream = BootstrapUtils.class.getResourceAsStream(initFilename);
			}
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