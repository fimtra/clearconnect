/*
 * Copyright (c) 2013 Ramon Servadei 
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
package com.fimtra.datafission.core;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.Publisher;
import com.fimtra.datafission.core.StringProtocolCodec;
import com.fimtra.datafission.field.LongValue;

/**
 * Context to be used for a telnet client
 * 
 * @author Ramon Servadei
 */
public class RemoteContextTelnetServer
{
    private static final int PORT = 20000;
    private static final String LOCALHOST = "localhost";
    private static final String KEY_PREFIX = "K";
    final static String record1 = "record1";
    final static String record2 = "record2";
    final static String record3 = "record3";
    final static int KEY_COUNT = 6;
    final static int UPDATE_COUNT = 10;
    private static final int ATOMIC_CHANGE_PERIOD_MILLIS = 500;

    static Context context = new Context("TelnetServer");
    static ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);

    public static void main(String[] args) throws IOException
    {
        @SuppressWarnings("unused")
        Publisher publisher = new Publisher(context, new StringProtocolCodec(), LOCALHOST, PORT);

        createMapAndStartUpdating(record1);
        createMapAndStartUpdating(record2);
        createMapAndStartUpdating(record3);

        executor.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                context.publishAtomicChange(record1);
                context.publishAtomicChange(record2);
                context.publishAtomicChange(record3);
            }
        }, 0, ATOMIC_CHANGE_PERIOD_MILLIS, TimeUnit.MILLISECONDS);

        System.in.read();
    }

    private static void createMapAndStartUpdating(String recordName)
    {
        final IRecord record = context.createRecord(recordName);
        for (int i = 0; i < KEY_COUNT; i++)
        {
            record.put(KEY_PREFIX + i, LongValue.valueOf(0));
        }
        executor.scheduleAtFixedRate(new Runnable()
        {
            Random random = new Random();

            @Override
            public void run()
            {
                // one key never updates
                for (int i = 0; i < KEY_COUNT - 1; i++)
                {
                    if (this.random.nextBoolean())
                    {
                        String key = KEY_PREFIX + i;
                        record.put(key, LongValue.valueOf(record.get(key).longValue() + 1));
                        record.getOrCreateSubMap("sub").put(key, LongValue.valueOf(record.get(key).longValue() + 1));
                    }
                }
            }
        }, 0, ATOMIC_CHANGE_PERIOD_MILLIS * 2, TimeUnit.MILLISECONDS);
    }
}
