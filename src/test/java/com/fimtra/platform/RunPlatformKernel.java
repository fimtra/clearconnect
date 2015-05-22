/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform;

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
        new PlatformKernel("TestPlatform", TcpChannelUtils.LOOPBACK);
        System.in.read();
    }
}
