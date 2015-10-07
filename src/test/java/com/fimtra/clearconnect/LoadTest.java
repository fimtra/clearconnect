/*
 * Copyright (c) 2015 Ramon Servadei 
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
package com.fimtra.clearconnect;

import java.io.IOException;

import com.fimtra.clearconnect.core.PlatformRegistryAgent;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.Log;

/**
 * @author Ramon Servadei
 */
public class LoadTest
{

    public static void main(String[] args) throws IOException, InterruptedException
    {
        // its a tough test - RPC timeout can happen
        System.getProperties().setProperty(DataFissionProperties.Names.RPC_EXECUTION_DURATION_TIMEOUT_MILLIS, "10000");
        System.getProperties().setProperty(DataFissionProperties.Names.RPC_EXECUTION_START_TIMEOUT_MILLIS, "10000");
        Log.log(LoadTest.class, "STARTING...");

        int port = 55556;
        final int maxLoops = 15;
        for (int j = 0; j < maxLoops; j++)
        {
            System.err.println("Loop #" + j + "/" + maxLoops);
            int MAX = 100;
            IPlatformRegistryAgent[] agents = new IPlatformRegistryAgent[MAX];
            for (int i = 0; i < MAX; i++)
            {
                System.err.println("Creating agent " + i);
                final String suffix = i + "-" + System.nanoTime();
                agents[i] = new PlatformRegistryAgent("Test-Agent-" + suffix, TcpChannelUtils.LOCALHOST_IP, 55555);

                // create a FT service
                port = createService(port, agents, i, suffix, "FT");

                // create a LB service
                port = createService(port, agents, i, suffix, "LB");
            }
            System.err.println("Waiting...");
            if (j == maxLoops - 1)
            {
                System.err.println("PRESS A KEY TO END");
                System.in.read();
            }
            Thread.sleep(3000);
            for (int i = 0; i < MAX; i++)
            {
                System.err.println("Destroying agent " + i);
                agents[i].destroy();
            }
        }
    }

    static int createService(int port, IPlatformRegistryAgent[] agents, int i, final String suffix, String type)
    {
        IPlatformServiceInstance platformServiceInstance;
        IRecord record;
        int createAttempts;
        createAttempts = 0;
        while (!agents[i].createPlatformServiceInstance("Test-" + type + "-service-" + suffix, "",
            TcpChannelUtils.LOCALHOST_IP, port++, WireProtocolEnum.STRING, RedundancyModeEnum.FAULT_TOLERANT))
        {
            System.err.println("Could not create " + type + " service on port " + port);
            if (createAttempts++ > 5)
            {
                System.exit(1);
            }
        }

        platformServiceInstance = agents[i].getPlatformServiceInstance("Test-" + type + "-service-" + suffix, "");
        record = platformServiceInstance.getOrCreateRecord("record-" + type + "-" + suffix);
        record.put("field", 0);
        platformServiceInstance.publishRecord(record);
        platformServiceInstance.publishRPC(new RpcInstance(TypeEnum.DOUBLE, "rpc-" + type + "-" + suffix));
        return port;
    }

}
