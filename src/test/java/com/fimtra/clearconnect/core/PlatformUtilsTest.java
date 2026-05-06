/*
 * Copyright (c) 2013 Ramon Servadei, Paul Mackinlay, Fimtra
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
package com.fimtra.clearconnect.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.fimtra.clearconnect.IPlatformServiceComponent;
import com.fimtra.clearconnect.event.IRpcAvailableListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.field.TextValue;
import org.junit.Test;
import org.mockito.stubbing.Answer;

/**
 * Tests for {@link PlatformUtils}
 * 
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
@SuppressWarnings("boxing")
public class PlatformUtilsTest
{

    @Test(expected = TimeOutException.class)
    public void testExecuteRpcUnavailable() throws TimeOutException, ExecutionException
    {
        IPlatformServiceComponent component = mock(IPlatformServiceComponent.class);
        PlatformUtils.executeRpc(component, 10, "");
    }

    @Test
    public void testExecuteRpcAvailable() throws TimeOutException, ExecutionException
    {
        IPlatformServiceComponent component = mock(IPlatformServiceComponent.class);
        final TextValue result = TextValue.valueOf("result!");
        IRpcInstance rpc = mock(IRpcInstance.class);
        when(rpc.execute()).thenReturn(result);

        when(component.getRpc(eq("rpc1"))).thenReturn(rpc);

        assertEquals(result, PlatformUtils.executeRpc(component, 10, "rpc1"));
    }

    @Test
    public void testExecuteRpcUnavailableThenAvailable() throws TimeOutException, ExecutionException
    {
        IPlatformServiceComponent component = mock(IPlatformServiceComponent.class);

        // an empty map is returned so we need to wait for publishing...so setup the listener
        final IRpcInstance rpc = mock(IRpcInstance.class);
        when(rpc.getName()).thenReturn("rpc1");
        when(rpc.getArgTypes()).thenReturn(new TypeEnum[0]);
        final TextValue result = TextValue.valueOf("result!");
        when(rpc.execute()).thenReturn(result);

        when(component.addRpcAvailableListener(any(IRpcAvailableListener.class))).then(
                (Answer<Boolean>) invocation -> {
                    ((IRpcAvailableListener) invocation.getArguments()[0]).onRpcAvailable(rpc);
                    return Boolean.TRUE;
                });

        assertEquals(result, PlatformUtils.executeRpc(component, 10, "rpc1"));
    }

    @Test
    public void testDecomposeClientFromProxyName()
    {
        String service = "Service!";
        String client = "lasers";
        final String proxyName = PlatformUtils.composeProxyName(service, client);
        assertEquals(client, PlatformUtils.decomposeClientFromProxyName(proxyName));
    }

    @Test
    public void testDecomposeServiceInstanceID()
    {
        String serviceName = "sdf1 d";
        String serviceMemberName = "sdf2 sdf";
        assertEquals(serviceName,
            PlatformUtils.decomposePlatformServiceInstanceID(PlatformUtils.composePlatformServiceInstanceID(
                serviceName, serviceMemberName))[0]);
        assertEquals(serviceMemberName,
            PlatformUtils.decomposePlatformServiceInstanceID(PlatformUtils.composePlatformServiceInstanceID(
                serviceName, serviceMemberName))[1]);
        assertNull(PlatformUtils.decomposePlatformServiceInstanceID("not correct format"));
    }
    
    @Test
    public void testDecomposeServiceInstanceID_with_square_brace_in_serviceName()
    {
        String serviceName = "sdf1 d[a special]";
        String serviceMemberName = "sdf2 sdf";
        assertEquals(serviceName,
            PlatformUtils.decomposePlatformServiceInstanceID(PlatformUtils.composePlatformServiceInstanceID(
                serviceName, serviceMemberName))[0]);
        assertEquals(serviceMemberName,
            PlatformUtils.decomposePlatformServiceInstanceID(PlatformUtils.composePlatformServiceInstanceID(
                serviceName, serviceMemberName))[1]);
        assertNull(PlatformUtils.decomposePlatformServiceInstanceID("not correct format"));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testComposeServiceInstanceID_with_close_square_brace_in_memberName()
    {
        String serviceName = "sdf1 d[a special]";
        String serviceMemberName = "sdf2 ]sdf";
        PlatformUtils.composePlatformServiceInstanceID(serviceName, serviceMemberName);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testComposeServiceInstanceID_with_open_square_brace_in_memberName()
    {
        String serviceName = "sdf1 d[a special]";
        String serviceMemberName = "sdf2 [sdf";
        PlatformUtils.composePlatformServiceInstanceID(serviceName, serviceMemberName);
    }

	@Test
	public void testIsClearConnectRecord() {
		String recordName = null;
		assertFalse(PlatformUtils.isClearConnectRecord(recordName));
		assertFalse(PlatformUtils.isClearConnectRecord("a custom name of my choice"));
		assertTrue(PlatformUtils.isClearConnectRecord(PlatformServiceInstance.SERVICE_STATS_RECORD_NAME));
		for (String name : ContextUtils.SYSTEM_RECORDS) {
			assertTrue(PlatformUtils.isClearConnectRecord(name));
		}
	}

    @Test
    public void testAddProcessId() throws UnknownHostException
    {
        final String hostName = InetAddress.getLocalHost()
                .getHostName();
        final String hostAddress = InetAddress.getLocalHost()
                .getHostAddress();
        final String name_hostName = "name@" + hostName;
        final String name_hostAddress = "name@" + hostAddress;
        final String expected_hostName = name_hostName + "#123";
        final String expected_hostAddress = name_hostAddress + "#123";
        final String jvmName_hostName = "123@" + hostName;
        final String jvmName_hostAddress = "123@" + hostAddress;
        final String jvmName_pidOnly = "123";

        assertEquals(expected_hostName, PlatformUtils.doAddProcessId("name", jvmName_hostName));
        assertEquals(expected_hostAddress, PlatformUtils.doAddProcessId("name", jvmName_hostAddress));

        assertEquals(expected_hostAddress, PlatformUtils.doAddProcessId("name", jvmName_pidOnly));
        assertEquals(expected_hostAddress, PlatformUtils.doAddProcessId(name_hostAddress, jvmName_pidOnly));
        assertEquals(expected_hostName, PlatformUtils.doAddProcessId(name_hostName, jvmName_pidOnly));

        assertEquals(expected_hostName, PlatformUtils.doAddProcessId(name_hostName, jvmName_hostName));
        assertEquals(expected_hostAddress, PlatformUtils.doAddProcessId(name_hostAddress, jvmName_hostAddress));
    }

    @Test
    public void test_composeHostQualifiedName() throws UnknownHostException
    {
        final String expected_withHostname = "user@" + InetAddress.getLocalHost().getHostName();
        assertEquals(expected_withHostname, PlatformUtils.composeHostQualifiedName(expected_withHostname));
        final String expected_withIp = "user@" + InetAddress.getLocalHost().getHostAddress();
        assertEquals(expected_withIp, PlatformUtils.composeHostQualifiedName(expected_withIp));
        assertEquals(expected_withIp, PlatformUtils.composeHostQualifiedName("user"));
    }
}
