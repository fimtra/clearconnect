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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.util.Log;
import com.fimtra.util.LowGcLinkedList;

/**
 * Handles applying images/delta changes to local records received from remote records.
 * <p>
 * NOTE: this class has logic to deal with multicast topology where deltas are sent on a separate
 * stream to images, hence why this class has logic to cache deltas and apply them on image being
 * received.
 * 
 * @author Ramon Servadei
 */
final class ImageDeltaChangeProcessor
{
    final static int NOOP = 0;
    final static int PUBLISH = 1;
    final static int RESYNC = 2;

    final Map<String, LowGcLinkedList<IRecordChange>> cachedDeltas;
    /** Tracks when the image of a record is received (deltas consumed after this) */
    final Map<String, Boolean> imageReceived;

    ImageDeltaChangeProcessor()
    {
        this.cachedDeltas = new ConcurrentHashMap<String, LowGcLinkedList<IRecordChange>>();
        this.imageReceived = new ConcurrentHashMap<String, Boolean>();
    }

    /**
     * Called when a {@link ProxyContext} re-connects. This clearc the caches of the processor so
     * everything starts "fresh".
     */
    void reset()
    {
        this.cachedDeltas.clear();
        this.imageReceived.clear();
    }
    
    int processRxChange(final IRecordChange changeToApply, final String name, IRecord record)
    {
        final boolean imageAlreadyReceived = this.imageReceived.containsKey(name);
        
        // if the sequence is wrong...
        if (record.getSequence() + 1 != changeToApply.getSequence())
        {
            final boolean isDelta = changeToApply.getScope() == IRecordChange.DELTA_SCOPE_CHAR;
            if (imageAlreadyReceived)
            {
                if (isDelta)
                {
                    Log.log(this, "Incorrect delta seq for ", name, ", delta.seq=",
                        Long.toString(changeToApply.getSequence()), ", record.seq=",
                        Long.toString(record.getSequence()));

                    this.imageReceived.remove(name);
                    return RESYNC;
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

                        this.imageReceived.remove(name);
                        return RESYNC;
                    }
                }
            }

            if (isDelta)
            {
                LowGcLinkedList<IRecordChange> deltas = this.cachedDeltas.get(name);
                if (deltas == null)
                {
                    deltas = new LowGcLinkedList<IRecordChange>();
                    this.cachedDeltas.put(name, deltas);
                }
                deltas.add(changeToApply);

                if (deltas.size() > DataFissionProperties.Values.DELTA_COUNT_LOG_THRESHOLD)
                {
                    Log.log(this, "Cached delta count is ", Integer.toString(deltas.size()), " for ", name,
                        ", delta.seq=", Long.toString(changeToApply.getSequence()), ", record.seq=",
                        Long.toString(record.getSequence()));
                    
                    if (deltas.size() > DataFissionProperties.Values.DELTA_COUNT_LOG_THRESHOLD * 2)
                    {
                        Log.log(this, "Scheduling resync, too many cached deltas for ", name);
                        return RESYNC;
                    }
                }
            }
            else
            {
                // its an image

                changeToApply.applyCompleteAtomicChangeToRecord(record);
                
                // apply any subsequent deltas (this only occurs over multicast topology)
                final LowGcLinkedList<IRecordChange> deltas = this.cachedDeltas.remove(name);
                if (deltas != null)
                {
                    Log.log(this, "Processing deltas for image ", name, ", image.seq=",
                        Long.toString(changeToApply.getSequence()), ", delta count is ",
                        Integer.toString(deltas.size()));

                    long deltaSequence = -1;
                    long lastSequence = -1;
                    for (IRecordChange deltaChange : deltas)
                    {
                        deltaSequence = deltaChange.getSequence();
                        // this allows us to skip deltas that are earlier than the received image
                        if (deltaSequence > changeToApply.getSequence())
                        {
                            if (lastSequence > -1 && lastSequence + 1 != deltaSequence)
                            {
                                Log.log(this, "Incorrect sequence for cached delta ", name, ", delta.seq=",
                                    Long.toString(deltaSequence), " last.seq=", Long.toString(lastSequence));
                                return RESYNC;
                            }
                            deltaChange.applyCompleteAtomicChangeToRecord(record);
                            lastSequence = deltaSequence;
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
