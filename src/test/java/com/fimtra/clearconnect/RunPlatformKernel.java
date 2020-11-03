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
package com.fimtra.clearconnect;

import com.fimtra.tcpchannel.TcpChannelUtils;

/**
 * Run a {@link PlatformKernel}
 * 
 * @author Ramon Servadei
 */
public class RunPlatformKernel
{
    @SuppressWarnings("unused")
    public static void main(String[] args) throws Exception
    {
        new PlatformKernel("TestPlatform", TcpChannelUtils.LOCALHOST_IP);
        System.in.read();
    }
}
