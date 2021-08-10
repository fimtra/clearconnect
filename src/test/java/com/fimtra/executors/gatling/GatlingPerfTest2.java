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
package com.fimtra.executors.gatling;

import static com.fimtra.executors.gatling.GatlingPerfTest.getExecutorTime;
import static com.fimtra.executors.gatling.GatlingPerfTest.getGatlingTime;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

/**
 * Simple performance test for the {@link GatlingExecutor}
 *
 * @author Ramon Servadei
 */
@SuppressWarnings({ "boxing", "unused" })
public class GatlingPerfTest2
{

    @Before
    public void setUp() throws Exception
    {
    }

    @Test
    public void testSequentialPerformanceVsExecutors() throws InterruptedException
    {
        int LOOP_MAX = 50000;
        int contextCount = Runtime.getRuntime().availableProcessors();
        long executorTime = (long) (getExecutorTime(LOOP_MAX, contextCount) / 1_000_000d);
        long gatlingTime = (long) (getGatlingTime(LOOP_MAX, contextCount)  / 1_000_000d);
        System.err.println("executorTime=" + executorTime );
        System.err.println(" gatlingTime=" + gatlingTime);
        assertTrue("executorTime (" + executorTime + ") is less than gatlingTime (" + gatlingTime + ")",
                executorTime > gatlingTime);
    }
}
