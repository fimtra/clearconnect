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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.Publisher;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.datafission.core.StringProtocolCodec;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.SystemUtils;

/**
 * The publisher for benchmarking. After starting this, start a {@link BenchmarkSubscriber}
 * 
 * @author Ramon Servadei
 */
public class BenchmarkPublisher
{
    public static void main(String[] args) throws InterruptedException
    {
        // create the context that will hold the record(s)
        final Context context = new Context("BenchmarkPublisher");

        // enable remote access to the context, this opens a TCP server socket on localhost:222222
        final Publisher publisher =
            new Publisher(context, new StringProtocolCodec(), args.length == 0 ? TcpChannelUtils.LOCALHOST_IP : args[0],
                22222);

        final AtomicReference<CountDownLatch> runLatch = new AtomicReference<CountDownLatch>();

        // this RPC is called by the subscriber after each run completes
        context.createRpc(new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                runLatch.get().countDown();
                return null;
            }
        }, TypeEnum.TEXT, "runComplete"));

        // wait for subscribers
        final CountDownLatch start = new CountDownLatch(1);
        context.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
            {
                if (imageValidInCallingThreadOnly.keySet().contains("BenchmarkRecord-0"))
                {
                    start.countDown();
                }
            }
        }, ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);

        System.err.print("Waiting for subscriber...");
        start.await();
        System.err.println("done");

        AtomicLong allRunsPublishCount = new AtomicLong();
        AtomicLong allRunsLatencyMicros = new AtomicLong();
        // warmup
        doTest(context, runLatch, allRunsLatencyMicros, allRunsPublishCount);

        StringBuilder results = doTest(context, runLatch, allRunsLatencyMicros, allRunsPublishCount);

        results.append("Total updates: " + allRunsPublishCount.get()).append(SystemUtils.lineSeparator());
        results.append("Avg RX latency (usec): " + allRunsLatencyMicros.get() / allRunsPublishCount.get()).append(
            SystemUtils.lineSeparator());
        results.append("Avg msg size (bytes): " + publisher.getBytesPublished() / publisher.getMessagesPublished()).append(
            SystemUtils.lineSeparator());
        results.append("CPU count: " + Runtime.getRuntime().availableProcessors()).append(SystemUtils.lineSeparator());
        results.append("JVM version: " + System.getProperty("java.version")).append(SystemUtils.lineSeparator());

        System.err.println(results);

        System.err.println("Finished");
    }

    static StringBuilder doTest(Context context, final AtomicReference<CountDownLatch> runLatch,
        AtomicLong allRunsLatencyMicros, AtomicLong allRunsPublishCount) throws InterruptedException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Concurrent record count, avg latency (uSec)").append(SystemUtils.lineSeparator());

        IRecord record;
        final int maxUpdates = 10000;
        final int maxRecordCount = 64;
        final Random rnd = new Random();
        long runStartNanos, runLatencyMicros, publishCount;
        int currentRecord = 0;
        for (int recordCount = 1; recordCount <= maxRecordCount; recordCount++)
        {
            System.err.print("Updating " + recordCount + " concurrent records...");
            runLatch.set(new CountDownLatch(1));
            publishCount = 0;
            runStartNanos = System.nanoTime();
            currentRecord = 0;
            for (int updateNumber = 1; updateNumber <= maxUpdates; updateNumber++)
            {
                // get each record and update - go backwards so the 0th one is always the last one
                record = context.getOrCreateRecord("BenchmarkRecord-" + currentRecord++);
                record.put("maxRecordCount", LongValue.valueOf(maxRecordCount));
                record.put("concurrentRecordCount", LongValue.valueOf(recordCount));
                record.put("maxUpdates", LongValue.valueOf(maxUpdates));
                record.put("updateNumber", LongValue.valueOf(updateNumber));
                record.put("data1", LongValue.valueOf(rnd.nextLong()));
                record.put("data2", DoubleValue.valueOf(rnd.nextDouble()));
                record.put("data3", TextValue.valueOf("" + rnd.nextLong()));
                context.publishAtomicChange(record);
                publishCount++;
                if (currentRecord > maxRecordCount - 1)
                {
                    currentRecord = 0;
                }
            }

            // wait for the subscriber to acknowledge this run
            System.err.print("waiting for run to complete...");
            runLatch.get().await();
            runLatencyMicros = (System.nanoTime() - runStartNanos) / 1000;
            allRunsLatencyMicros.addAndGet(runLatencyMicros);
            allRunsPublishCount.addAndGet(publishCount);
            System.err.println("completed.");
            sb.append(recordCount).append(",").append((runLatencyMicros / (publishCount))).append(
                SystemUtils.lineSeparator());
        }
        return sb;
    }
}
