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
package com.fimtra.datafission.core;

import com.fimtra.datafission.IRecord;

/**
 * A snapshot of a record that is also immutable.
 * <p>
 * The snapshot will not change and cannot be changed.
 * 
 * @author Ramon Servadei
 */
public class ImmutableSnapshotRecord extends ImmutableRecord
{

    /**
     * Create a snapshot of the {@link IRecord} as the source for a new {@link ImmutableRecord}
     * instance.
     */
    public static ImmutableSnapshotRecord create(IRecord template)
    {
        if (template instanceof ImmutableRecord)
        {
            return new ImmutableSnapshotRecord(((ImmutableRecord) template).backingRecord.clone());
        }
        else
        {
            return new ImmutableSnapshotRecord(((Record) template).clone());
        }
    }

    /**
     * Used to construct an immutable record from a snapshot of a record.
     */
    ImmutableSnapshotRecord(Record snapshot)
    {
        super(snapshot);
    }

    @Override
    public String toString()
    {
        return "(ImmutableSnapshot)" + this.backingRecord.toString();
    }
}
