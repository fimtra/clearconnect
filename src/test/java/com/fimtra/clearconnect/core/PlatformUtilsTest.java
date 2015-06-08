/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
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
package com.fimtra.clearconnect.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.TransportTechnologyEnum;
import com.fimtra.clearconnect.IPlatformServiceComponent;
import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.core.PlatformUtils;
import com.fimtra.clearconnect.event.IRpcAvailableListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.field.TextValue;

import static org.mockito.Matchers.any;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link PlatformUtils}
 * 
 * @author Ramon Servadei
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
        Map<String, IRpcInstance> rpcs = new HashMap<String, IRpcInstance>();
        final TextValue result = new TextValue("result!");
        IRpcInstance rpc = mock(IRpcInstance.class);
        when(rpc.execute()).thenReturn(result);

        rpcs.put("rpc1", rpc);
        when(component.getAllRpcs()).thenReturn(rpcs);

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
        final TextValue result = new TextValue("result!");
        when(rpc.execute()).thenReturn(result);

        when(component.addRpcAvailableListener(any(IRpcAvailableListener.class))).then(new Answer<Boolean>()
        {

            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable
            {
                ((IRpcAvailableListener) invocation.getArguments()[0]).onRpcAvailable(rpc);
                return Boolean.TRUE;
            }
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
    public void testGetNextFreeDefaultTcpServerPort() throws IOException
    {
        if (ChannelUtils.TRANSPORT != TransportTechnologyEnum.TCP)
        {
            return;
        }

        final int nextFreeDefaultTcpServerPort = PlatformUtils.getNextFreeDefaultTcpServerPort(null);
        assertTrue("Got: " + nextFreeDefaultTcpServerPort, nextFreeDefaultTcpServerPort > -1);
        @SuppressWarnings("unused")
        ServerSocket ssoc = new ServerSocket(nextFreeDefaultTcpServerPort, 50, InetAddress.getByName(null));
        final int nextFreeDefaultTcpServerPort2 = PlatformUtils.getNextFreeDefaultTcpServerPort(null);
        assertTrue(nextFreeDefaultTcpServerPort2 == nextFreeDefaultTcpServerPort + 1);

        // exhaust all the default sockets
        List<ServerSocket> ssocs = new ArrayList<ServerSocket>();
        try
        {
            for (int i = PlatformCoreProperties.Values.TCP_SERVER_PORT_RANGE_START + 2; i <= PlatformCoreProperties.Values.TCP_SERVER_PORT_RANGE_END; i++)
            {
                ssocs.add(new ServerSocket(PlatformUtils.getNextFreeDefaultTcpServerPort(null), 50,
                    InetAddress.getByName(null)));
            }
            try
            {
                PlatformUtils.getNextFreeDefaultTcpServerPort(null);
                fail("Should throw exception");
            }
            catch (RuntimeException e)
            {
            }
        }
        finally
        {
            for (ServerSocket serverSocket : ssocs)
            {
                try
                {
                    serverSocket.close();
                    Thread.sleep(50);
                }
                catch (Exception e)
                {
                }
            }
        }
    }
}
