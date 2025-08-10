package com.fimtra.clearconnect.benchmark;

import java.awt.*;
import java.awt.datatransfer.StringSelection;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fimtra.clearconnect.core.PlatformUtils;
import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.GZipProtocolCodec;
import com.fimtra.datafission.field.AbstractValue;
import com.fimtra.datafission.field.BlobValue;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.thimble.ISequentialRunnable;

/**
 * Provides a simple, single-threaded performance test to compare versions for relative performance in a few
 * metrics. Each metric is compiled from measuring in a loop for the metric. The test takes <30secs to run.
 * <p>
 * Metrics are in milliseconds:
 * <ul>
 * <li>record updating (key-value puts)</li>
 * <li>encode-decode</li>
 * <li>values toString [DoubleValue, LongValue, TextValue, BlobValue]</li>
 * <li>values fromString [DoubleValue, LongValue, TextValue, BlobValue]</li>
 * <li>values valueOf [DoubleValue, LongValue, TextValue, BlobValue]</li>
 * </ul>
 * <p>
 * Output example:
 * <pre>version=?.?.?: recordUpdate=950, encodeDecode=2960, valueToStringTimes[D,L,T,B]=[480, 100, 200, 1040], valueFromStringTimes[D,L,T,B]=[140, 70, 150, 1040], valueOfTimes[D,L,T,B]=[0, 10, 40, 20], Windows 11 (10.0), amd64, Java 1.8.0_452, cpus=16</pre>
 *
 * @author Ramon Servadei
 */
public class PerformanceTest
{
    public static void main(String[] args) throws InterruptedException
    {
        final int MAX_LOOPS = 1_000_000;
        final int MAX_LOOPS_VALUES = MAX_LOOPS * 10;

        TextValue textValue = new TextValue("sdf");
        DoubleValue doubleValue = new DoubleValue(3.14159);
        LongValue longValue = LongValue.valueOf(2);
        final byte[] bytes = "01234567890123456789012345678901234567890123456789".getBytes();
        BlobValue blobValue = BlobValue.valueOf(bytes);

        final long[] valueOfTimes = new long[4];
        valueOfTimes[0] = doLoopTest(MAX_LOOPS_VALUES, () -> DoubleValue.valueOf(3.0d));
        valueOfTimes[1] = doLoopTest(MAX_LOOPS_VALUES, () -> LongValue.valueOf(2));
        valueOfTimes[2] = doLoopTest(MAX_LOOPS_VALUES, () -> TextValue.valueOf("sdf"));
        valueOfTimes[3] = doLoopTest(MAX_LOOPS_VALUES, () -> BlobValue.valueOf(bytes));

        final long[] valueToStringTimes = new long[4];
        valueToStringTimes[0] = doLoopTest(MAX_LOOPS_VALUES, () -> doubleValue.toStringAppender()
                .toString());
        valueToStringTimes[1] = doLoopTest(MAX_LOOPS_VALUES, () -> longValue.toStringAppender()
                .toString());
        valueToStringTimes[2] = doLoopTest(MAX_LOOPS_VALUES, () -> textValue.toStringAppender()
                .toString());
        valueToStringTimes[3] = doLoopTest(MAX_LOOPS_VALUES, () -> blobValue.toStringAppender()
                .toString());

        final char[] sDoubleValue = getChars(doubleValue);
        final char[] sLongValue = getChars(longValue);
        final char[] sTextValue = getChars(textValue);
        final char[] sBlobValue = getChars(blobValue);
        final long[] valueFromStringTimes = new long[4];
        valueFromStringTimes[0] = doLoopTest(MAX_LOOPS_VALUES,
                () -> AbstractValue.constructFromCharValue(sDoubleValue, sDoubleValue.length));
        valueFromStringTimes[1] = doLoopTest(MAX_LOOPS_VALUES,
                () -> AbstractValue.constructFromCharValue(sLongValue, sLongValue.length));
        valueFromStringTimes[2] = doLoopTest(MAX_LOOPS_VALUES,
                () -> AbstractValue.constructFromCharValue(sTextValue, sTextValue.length));
        valueFromStringTimes[3] = doLoopTest(MAX_LOOPS_VALUES,
                () -> AbstractValue.constructFromCharValue(sBlobValue, sBlobValue.length));

        final Context context = new Context("Perf-test");
        // 1 record means we get single-threaded logic and a good view of code efficiency across runs
        final String recordName = "perf-record";
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicLong recordUpdateTime = new AtomicLong();
        final ISequentialRunnable sequentialRunnable = new ISequentialRunnable()
        {
            long l;

            @Override
            public Object context()
            {
                return recordName;
            }

            @Override
            public void run()
            {
                IRecord record = null;
                long t = System.nanoTime();
                for (int i = 0; i < MAX_LOOPS; i++)
                {
                    record = context.getOrCreateRecord(recordName);
                    String text;
                    for (int d = 0; d < 4; d++)
                    {
                        l++;
                        record.put("k-d-" + d, DoubleValue.valueOf(l));
                        record.put("k-l-" + d, LongValue.valueOf(l));
                        text = "" + l;
                        record.put("k-t-" + d, TextValue.valueOf(text));
                        record.put("k-b-" + d, BlobValue.valueOf(text.getBytes()));
                    }
                }
                recordUpdateTime.set(System.nanoTime() - t);
                context.publishAtomicChange(record);
            }
        };

        final AtomicLong encodeDecodeTime = new AtomicLong();
        final ICodec codec = new GZipProtocolCodec();

        context.addObserver((image, atomicChange) -> {

            long t = (System.nanoTime());
            for (int i = 0; i < MAX_LOOPS; i++)
            {
                codec.getAtomicChangeFromRxMessage(
                        ByteBuffer.wrap(codec.getTxMessageForAtomicChange(atomicChange)));
            }
            encodeDecodeTime.set(System.nanoTime() - t);

            // finish the test
            latch.countDown();

        }, recordName);

        // start
        context.executeSequentialCoreTask(sequentialRunnable);

        if (!latch.await(30, TimeUnit.SECONDS))
        {
            System.err.println("FAILED");
        }
        else
        {
            for (int i = 0; i < valueFromStringTimes.length; i++)
            {
                valueFromStringTimes[i] /= 1_000_000;
                valueFromStringTimes[i] = round10s(valueFromStringTimes[i]);
            }
            for (int i = 0; i < valueToStringTimes.length; i++)
            {
                valueToStringTimes[i] /= 1_000_000;
                valueToStringTimes[i] = round10s(valueToStringTimes[i]);
            }
            for (int i = 0; i < valueOfTimes.length; i++)
            {
                valueOfTimes[i] /= 1_000_000;
                valueOfTimes[i] = round10s(valueOfTimes[i]);
            }

            final String result = "version=" + PlatformUtils.VERSION + ": "

                    + "recordUpdate=" + round10s((recordUpdateTime.get()) / 1_000_000L)

                    + ", encodeDecode=" + round10s((encodeDecodeTime.get()) / 1_000_000L)

                    + ", valueToStringTimes[D,L,T,B]=" + Arrays.toString(valueToStringTimes)

                    + ", valueFromStringTimes[D,L,T,B]=" + Arrays.toString(valueFromStringTimes)

                    + ", valueOfTimes[D,L,T,B]=" + Arrays.toString(valueOfTimes)

                    + ", " + System.getProperty("os.name") + " (" + System.getProperty("os.version") + "), "
                    + System.getProperty("os.arch") + ", Java " + System.getProperty("java.version")
                    + ", cpus=" + Runtime.getRuntime()
                    .availableProcessors();

            final StringSelection strse1 = new StringSelection(result);
            Toolkit.getDefaultToolkit()
                    .getSystemClipboard()
                    .setContents(strse1, strse1);
            System.err.println(result + " (copied to clipboard)");
        }
        System.exit(0);
    }

    private static long round10s(long l)
    {
        return Math.round(l / 10f) * 10L;
    }

    private static char[] getChars(IValue value)
    {
        final String string = value.toStringAppender()
                .toString();
        final char[] chars = new char[string.length()];
        string.getChars(0, string.length(), chars, 0);
        return chars;
    }

    private static long doLoopTest(int loops, Runnable action)
    {
        long time = System.nanoTime();
        for (int i = 0; i < loops; i++)
        {
            action.run();
        }
        return System.nanoTime() - time;
    }
}

