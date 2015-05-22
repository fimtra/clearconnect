/*
 * Copyright (c) 2013 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.tcpchannel.TcpChannelUtils;

/**
 * @author Ramon Servadei
 */
public class MatrixClient
{
    @SuppressWarnings("unused")
    public static void main(String[] args) throws Exception
    {
        new MatrixClient();

        synchronized ("")
        {
            "".wait();
        }
    }

    PlatformServiceAccess matrixClientAccess;

    public MatrixClient() throws Exception
    {
        final String host = TcpChannelUtils.LOOPBACK; 
        this.matrixClientAccess = new PlatformServiceAccess("MATRIX-client", "lasers", host);
        final IPlatformServiceProxy matrixEngineProxy =
            this.matrixClientAccess.getPlatformRegistryAgent().getPlatformServiceProxy(MatrixServer.SERVICE_NAME);
        matrixEngineProxy.addRecordListener(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageCopy, IRecordChange atomicChange)
            {
                System.err.println("Transmission latency (including publisher encode and receiver decode time)="
                    + (System.currentTimeMillis() - imageCopy.get("END").longValue()) + "ms");
                System.err.println("CALC_LATENCY=" + imageCopy.get("CALC_LATENCY").longValue() + "ms");
                System.err.println("UPDATE_LATENCY=" + imageCopy.get("UPDATE_LATENCY").longValue() + "ms");
                System.err.println("DELTA size=" + atomicChange.getSubMapKeys().size() + " rows");
                System.err.println("DELTA size=" + atomicChange.toString().getBytes().length);                
            }
        }, MatrixServer.MATRIX_RECORD_NAME);
    }
}
