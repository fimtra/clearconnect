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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.util.Log;

/**
 * Handles applying images/delta changes to local records received from remote records.
 * 
 * @author Ramon Servadei
 */
final class ImageDeltaChangeProcessor
{
    final static int NOOP = 0;
    final static int PUBLISH = 1;
    final static int RESYNC = 2;

    final Map<String, Map<Long, IRecordChange>> cachedDeltas;
    /** Tracks when the image of a record is received (deltas consumed after this) */
    final Map<String, Boolean> imageReceived;

    ImageDeltaChangeProcessor()
    {
        this.cachedDeltas = new ConcurrentHashMap<String, Map<Long, IRecordChange>>();
        this.imageReceived = new ConcurrentHashMap<String, Boolean>();
    }

    int processRxChange(final IRecordChange changeToApply, final String name, IRecord record)
    {
        final boolean imageAlreadyReceived = this.imageReceived.containsKey(name);
        if (record.getSequence() + 1 != changeToApply.getSequence())
        {
            final boolean isDelta = changeToApply.getScope() == IRecordChange.DELTA_SCOPE.charValue();
            if (imageAlreadyReceived)
            {
                if (isDelta)
                {
                    Log.log(this, "Incorrect delta seq for ", name, ", delta.seq=",
                        Long.toString(changeToApply.getSequence()), ", record.seq=",
                        Long.toString(record.getSequence()));

                    if (this.imageReceived.remove(name) != null)
                    {
                        return RESYNC;
                    }
                }
                else
                {
                    // an image is only acceptable when it has already been received if the image
                    // sequence number matches the current record sequence number (it is, in effect
                    // a duplicate), otherwise we must re-sync
                    if (record.getSequence() != changeToApply.getSequence())
                    {
                        Log.log(this, "Incorrect image seq for ", name, ", image.seq=",
                            Long.toString(changeToApply.getSequence()), ", record.seq=",
                            Long.toString(record.getSequence()));

                        if (this.imageReceived.remove(name) != null)
                        {
                            return RESYNC;
                        }
                    }
                }
            }

            if (isDelta)
            {
                Map<Long, IRecordChange> deltas = this.cachedDeltas.get(name);
                if (deltas == null)
                {
                    deltas = new LinkedHashMap<Long, IRecordChange>();
                    this.cachedDeltas.put(name, deltas);
                }
                deltas.put(Long.valueOf(changeToApply.getSequence()), changeToApply);
                Log.log(this, "Cached delta for ", name, ", delta.seq=", Long.toString(changeToApply.getSequence()),
                    ", record.seq=", Long.toString(record.getSequence()));
            }
            else
            {
                // its an image
                Log.log(this, "Processing image for ", name, ", image.seq=", Long.toString(changeToApply.getSequence()));

                changeToApply.applyCompleteAtomicChangeToRecord(record);
                // apply any subsequent deltas
                Map<Long, IRecordChange> deltas = this.cachedDeltas.remove(name);
                if (deltas != null)
                {
                    Map.Entry<Long, IRecordChange> entry = null;
                    Long deltaSequence = null;
                    IRecordChange deltaChange = null;
                    long lastSequence = -1;
                    for (Iterator<Map.Entry<Long, IRecordChange>> it = deltas.entrySet().iterator(); it.hasNext();)
                    {
                        entry = it.next();
                        deltaSequence = entry.getKey();
                        deltaChange = entry.getValue();
                        if (deltaSequence.longValue() > changeToApply.getSequence())
                        {
                            if (lastSequence > -1 && lastSequence + 1 != deltaSequence.longValue())
                            {
                                Log.log(this, "Incorrect sequence for cached delta ", name, ", delta.seq=",
                                    Long.toString(deltaSequence.longValue()), " last.seq=", Long.toString(lastSequence));
                                return RESYNC;
                            }
                            Log.log(this, "Applying delta for ", name, ", delta.seq=",
                                Long.toString(deltaSequence.longValue()));
                            deltaChange.applyCompleteAtomicChangeToRecord(record);
                            lastSequence = deltaSequence.longValue();
                        }
                    }
                }

                if (!imageAlreadyReceived)
                {
                    this.imageReceived.put(name, Boolean.TRUE);
                }
                return PUBLISH;
            }
        }
        else
        {
            changeToApply.applyCompleteAtomicChangeToRecord(record);
            if (!imageAlreadyReceived)
            {
                this.imageReceived.put(name, Boolean.TRUE);
            }
            return PUBLISH;
        }
        return NOOP;
    }

    void unsubscribed(String recordName)
    {
        this.imageReceived.remove(recordName);
    }
}
