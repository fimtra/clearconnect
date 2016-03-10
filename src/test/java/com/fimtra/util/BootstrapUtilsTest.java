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

import java.util.Properties;

import org.junit.Test;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.tcpchannel.TcpChannelUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Paul Mackinlay
 */
public class BootstrapUtilsTest {

	private static final String uiName = "ui_name";
	private static final String serverName = "server_name";

	@Test
	public void shouldTestUtils() {
		assertEquals(System.getProperty("user.home") + System.getProperty("file.separator") + "." + uiName + ".properties",
				BootstrapUtils.getHomeDirInitFilename(uiName));
		System.setProperty("boot.initFilename", uiName + ".properties");
		assertEquals(uiName + ".properties", BootstrapUtils.getHomeDirInitFilename(uiName));
		System.getProperties().remove("boot.initFilename");

		assertEquals(System.getProperty("user.dir") + System.getProperty("file.separator") + serverName + ".properties",
				BootstrapUtils.getWorkingDirInitFilename(serverName));
		System.setProperty("boot.initFilename", serverName + ".properties");
		assertEquals(serverName + ".properties", BootstrapUtils.getWorkingDirInitFilename(serverName));
		System.getProperties().remove("boot.initFilename");

		assertTrue(BootstrapUtils.getInitProperties("non existant file").isEmpty());

		Properties initProperties = BootstrapUtils.getInitProperties("/testInitFile.properties");
		assertEquals(1, initProperties.size());

		EndPointAddress[] endpoints = BootstrapUtils.getRegistryEndPoints(BootstrapUtils.getInitProperties("non existant file"));
		assertEquals(1, endpoints.length);
		assertEquals(TcpChannelUtils.LOCALHOST_IP, endpoints[0].getNode());
		assertEquals(PlatformCoreProperties.Values.REGISTRY_PORT, endpoints[0].getPort());

		endpoints = BootstrapUtils.getRegistryEndPoints(initProperties);
		assertEquals(2, endpoints.length);
		assertEquals("test", endpoints[0].getNode());
		assertEquals(100, endpoints[0].getPort());
		assertEquals("test1", endpoints[1].getNode());
		assertEquals(101, endpoints[1].getPort());
	}
}
