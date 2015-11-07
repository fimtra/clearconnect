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

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.core.AtomicChange;
import com.fimtra.util.Log;

/**
 * Caches {@link AtomicChange} objects and can notify threads when the required number of updates
 * have occurred.
 * 
 * @author Ramon Servadei
 */
public class TestCachingAtomicChangeObserver implements IRecordListener
{
    public boolean log;
    public CountDownLatch latch;
    List<IRecord> images = new CopyOnWriteArrayList<IRecord>();
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

    @Override
    public void onChange(IRecord image, IRecordChange atomicChange)
    {
        if (this.log)
        {
            Log.log(this, "atomicChange=" + atomicChange + ", image=" + image);
        }
        this.images.add(ImmutableSnapshotRecord.create(image));
        this.changes.add(atomicChange);
        notifyThreads();
    }

    public IRecord getLatestImage()
    {
        if (this.images.size() == 0)
        {
            return null;
        }
        return this.images.get(this.images.size() - 1);
    }

    public void reset()
    {
        this.images.clear();
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