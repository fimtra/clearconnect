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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.field.LongValue;

/**
 * Check sequences of updates for a LongValue key
 * 
 * @author Ramon Servadei
 */
class TestLongValueSequenceCheckingAtomicChangeObserver implements IRecordListener
{
    CountDownLatch latch;
    List<String> errors = new ArrayList<String>(1);
    IRecordChange lastAtomicChange;

    TestLongValueSequenceCheckingAtomicChangeObserver()
    {
    }

    @Override
    public void onChange(IRecord source, IRecordChange atomicChange)
    {
        Set<String> keys = source.keySet();
        for (String key : keys)
        {
            checkLongValue(atomicChange, key);
        }

        this.lastAtomicChange = atomicChange;
        if (this.latch != null)
        {
            this.latch.countDown();
        }
    }

    private void checkLongValue(IRecordChange atomicChange, String theKey)
    {
        LongValue vNow = (LongValue) atomicChange.getPutEntries().get(theKey);
        LongValue vThen = (LongValue) atomicChange.getOverwrittenEntries().get(theKey);
        if (vThen != null)
        {
            if (!(vNow.longValue() >= vThen.longValue()))
            {
                add("[1] SEVERE out-of-sequence for " + atomicChange.getName() + ", key " + theKey + ", now=" + vNow
                    + ", was=" + vThen + ": " + atomicChange);
            }
            if (this.lastAtomicChange != null)
            {
                vNow = (LongValue) atomicChange.getPutEntries().get(theKey);
                vThen = (LongValue) this.lastAtomicChange.getPutEntries().get(theKey);
                if (vThen != null && !(vNow.longValue() >= vThen.longValue()))
                {
                    add("[2] SEVERE out-of-sequence for " + atomicChange.getName() + ", key " + theKey + ", now="
                        + vNow + ", was=" + vThen);
                }
            }
        }
    }

    private void add(String string)
    {
        this.errors.add(string);
    }

    void verify()
    {
        for (String error : this.errors)
        {
            TestCase.fail(error);
        }
    }
}