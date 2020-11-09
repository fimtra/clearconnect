/*
 * Copyright (c) 2014 Ramon Servadei 
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

import java.util.concurrent.ScheduledExecutorService;

import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.core.CoalescingRecordListener.TimedCoalescingStrategy;
import com.fimtra.util.ThreadUtils;
import org.junit.After;
import org.junit.Before;

/**
 * Tests for the {@link CoalescingRecordListener} using the {@link TimedCoalescingStrategy}
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings("unused")
public class TimedStrategyCoalescingRecordListenerTest extends CoalescingRecordListenerTest
{
    private ScheduledExecutorService executor;

    @Override
    @Before
    public void setUp() throws Exception
    {
        super.setUp();
        this.executor = ThreadUtils.newScheduledExecutorService("TimedStrategyCoalescingRecordListenerTest", 2);
    }

    @Override
    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
        this.executor.shutdown();
    }

    @Override
    protected IRecordListener wrap(TestCachingAtomicChangeObserver observer)
    {
        return new CoalescingRecordListener(this.executor, 250, observer, ContextUtils.CORE_EXECUTOR, this);
    }

}
