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
package com.fimtra.platform;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.ThreadUtils;

/**
 * @author Ramon Servadei
 */
public class MatrixServer
{

    static final String MATRIX_RECORD_NAME = "matrix01";
    static final String SERVICE_NAME = "MATRIX-engine";

    @SuppressWarnings("unused")
    public static void main(String[] args) throws Exception
    {
        new MatrixServer();

        synchronized ("")
        {
            "".wait();
        }
    }

    PlatformServiceAccess matrixServerAccess;
    IPlatformServiceInstance matrixEngine;

    public MatrixServer() throws Exception
    {
        final String host = TcpChannelUtils.LOOPBACK;
        this.matrixServerAccess = new PlatformServiceAccess(SERVICE_NAME, "01", host);
        this.matrixEngine = this.matrixServerAccess.getPlatformServiceInstance();
        start();
    }

    public void start()
    {
        final String matrixRecordName = MATRIX_RECORD_NAME;
        final IRecord matrix01 = this.matrixEngine.getOrCreateRecord(matrixRecordName);
        final int ROW_MAX = 100;
        final int COL_MAX = 100;
        for (int x = 0; x < ROW_MAX; x++)
        {
            final Map<String, IValue> row = matrix01.getOrCreateSubMap("ROW_" + x);
            for (int y = 0; y < COL_MAX; y++)
            {
                row.put("COL_" + y, new DoubleValue(x * y));
            }
        }
        this.matrixEngine.publishRecord(matrix01);

        if (true)
        {
            startUpdating(matrix01, ROW_MAX, COL_MAX, 100);
        }
    }

    private void startUpdating(final IRecord matrix01, final int ROW_MAX, final int COL_MAX, final int PERIOD)
    {
        final ScheduledExecutorService updater = ThreadUtils.newScheduledExecutorService("updater", 1);
        updater.scheduleWithFixedDelay(new Runnable()
        {
            @SuppressWarnings("boxing")
            @Override
            public void run()
            {
                String key;
                IValue value;
                Map<String, IValue> row;
                final long start = System.currentTimeMillis();

                // only update 1/4 the matrix
                int count = ROW_MAX / 10;

                for (int x = 0; x < count; x++)
                {
                	int r =(int)( Math.random() * ROW_MAX);
                    row = matrix01.getOrCreateSubMap("ROW_" + r);
                    for (int y = 0; y < COL_MAX / 1; y++)
                    {
                        key = "COL_" + y;
                        value = row.get(key);
                        row.put(key, new DoubleValue(value.doubleValue() + 1));
                    }
                }
                final long end = System.currentTimeMillis();
                matrix01.put("START", Long.valueOf(start));
                matrix01.put("END", Long.valueOf(end));
                matrix01.put("CALC_LATENCY", Long.valueOf(end - start));
                MatrixServer.this.matrixEngine.publishRecord(matrix01);
                matrix01.put("UPDATE_LATENCY", Long.valueOf(System.currentTimeMillis() - start));
            }
        }, PERIOD, PERIOD, TimeUnit.MILLISECONDS);
    }
}
