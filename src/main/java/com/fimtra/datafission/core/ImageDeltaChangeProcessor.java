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
        this.cachedDeltas = new ConcurrentHashMap<>();
        this.imageReceived = new ConcurrentHashMap<>();
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

    /**
     * Used for updating a local record from a remote atomic change received from the remote record instance.
     * This is like {@link IRecordChange#applyCompleteAtomicChangeToRecord(IRecord)} but also updates the record's CHANGE
     * sequence to match {@link IRecordChange#getSequence()}. This ensures that when the local record is published, the
     * sequence of the record updates to match the received remote change.
     */
    void applyRemoteChangeToLocalRecord(IRecordChange changeToApply, IRecord record)
    {
        synchronized (record.getWriteLock())
        {
            changeToApply.applyCompleteAtomicChangeToRecord(record);

            if (record instanceof Record)
            {
                ((Record) record).getPendingAtomicChange().setSequence(changeToApply.getSequence());
            }
        }
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
                    // if the image is a previous image...
                    if (record.getSequence() > changeToApply.getSequence())
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
                    deltas = new LowGcLinkedList<>();
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
                        
						this.imageReceived.remove(name);
                        return RESYNC;
                    }
                }
            }
            else
            {
                // its an image and it forms the base-line definition for the record
                record.clear();
                applyRemoteChangeToLocalRecord(changeToApply, record);

                // apply any subsequent deltas (this only occurs over multicast topology)
                final LowGcLinkedList<IRecordChange> deltas = this.cachedDeltas.remove(name);
                if (deltas != null)
                {
                    long deltaSequence = -1;
                    long lastSequence = changeToApply.getSequence();
                    for (IRecordChange deltaChange : deltas)
                    {
                        deltaSequence = deltaChange.getSequence();
                        // this allows us to skip deltas that are earlier than the received image
                        if (deltaSequence > lastSequence)
                        {
                            if (lastSequence + 1 != deltaSequence)
                            {
                                Log.log(this, "Incorrect sequence for cached delta ", name, ", delta.seq=",
                                    Long.toString(deltaSequence), " last.seq=", Long.toString(lastSequence));
                                
                                this.imageReceived.remove(name);
                                return RESYNC;
                            }
                            applyRemoteChangeToLocalRecord(deltaChange, record);
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
            applyRemoteChangeToLocalRecord(changeToApply, record);
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
