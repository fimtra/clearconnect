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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.util.Log;

/**
 * Caches {@link AtomicChange} objects and can notify threads when the required number of updates have
 * occurred.
 *
 * @author Ramon Servadei
 */
public class TestCachingAtomicChangeObserver implements IRecordListener
{
    public boolean log;
    public CountDownLatch latch;
    AtomicInteger changeTicks = new AtomicInteger();
    AtomicReference<IRecord> first = new AtomicReference<IRecord>();
    AtomicReference<IRecord> current = new AtomicReference<IRecord>();
    AtomicReference<IRecord> previous = new AtomicReference<IRecord>();
    List<IRecordChange> changes = new CopyOnWriteArrayList<IRecordChange>();

    public TestCachingAtomicChangeObserver()
    {
        this(false);
    }

    public TestCachingAtomicChangeObserver(boolean log)
    {
        this(null, log);
    }

    public TestCachingAtomicChangeObserver(CountDownLatch latch)
    {
        this(latch, false);
    }

    public TestCachingAtomicChangeObserver(CountDownLatch latch, boolean log)
    {
        super();
        this.latch = latch;
        this.log = log;
    }

    int count;

    @Override
    public void onChange(IRecord image, IRecordChange atomicChange)
    {
        if (this.log)
        {
            Log.log(this, "atomicChange=" + atomicChange + ", image=" + image);
        }
        this.changeTicks.incrementAndGet();
        if (this.first.get() == null)
        {
            this.first.set(image);
        }
        this.previous.set(this.current.get());
        final ImmutableSnapshotRecord snapshot = ImmutableSnapshotRecord.create(image);
        this.current.set(snapshot);
        this.changes.add(atomicChange);
        notifyThreads();
    }

    public IRecord getLatestImage()
    {
        return this.current.get();
    }

    public void reset()
    {
        this.changeTicks.set(0);
        this.previous.set(null);
        this.current.set(null);
        this.changes.clear();
    }

    private void notifyThreads()
    {
        if (this.latch != null)
        {
            this.latch.countDown();
        }
    }
}