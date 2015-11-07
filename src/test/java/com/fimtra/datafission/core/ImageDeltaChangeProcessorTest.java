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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;

/**
 * Tests the {@link ImageDeltaChangeProcessor}
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings("boxing")
public class ImageDeltaChangeProcessorTest
{
    ImageDeltaChangeProcessor candidate;
    String name;
    IRecord record;
    IRecordChange changeToApply;

    void verifyGetSequenceCalled()
    {
        verify(this.changeToApply, atLeastOnce()).getSequence();
        verify(this.record, atLeastOnce()).getSequence();
    }

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new ImageDeltaChangeProcessor();
        this.name = "test-recordName";
        this.record = mock(IRecord.class);
        this.changeToApply = mock(IRecordChange.class);

        // simulate we have an image
        this.candidate.imageReceived.put(this.name, Boolean.TRUE);
    }

    @Test
    public void testProcessRxChange_Image_SequenceOK()
    {
        when(this.changeToApply.getSequence()).thenReturn(2l);
        when(this.record.getSequence()).thenReturn(1l);

        assertEquals(ImageDeltaChangeProcessor.PUBLISH, this.candidate.processRxChange(this.changeToApply, this.name, this.record));
        verify(this.changeToApply).applyCompleteAtomicChangeToRecord(eq(this.record));
        verifyGetSequenceCalled();
        verifyNoMoreInteractions(this.record, this.changeToApply);
    }

    @Test
    public void testProcessRxChange_Image_Sequence_SAME()
    {
        when(this.changeToApply.getSequence()).thenReturn(1l);
        when(this.record.getSequence()).thenReturn(1l);
        when(this.changeToApply.getScope()).thenReturn(IRecordChange.IMAGE_SCOPE);

        assertEquals(ImageDeltaChangeProcessor.PUBLISH, this.candidate.processRxChange(this.changeToApply, this.name, this.record));
        verify(this.changeToApply).applyCompleteAtomicChangeToRecord(eq(this.record));
        verify(this.changeToApply).getScope();
        verifyGetSequenceCalled();
        verifyNoMoreInteractions(this.record, this.changeToApply);
    }

    @Test
    public void testProcessRxChange_Image_Sequence_Wrong()
    {
        when(this.changeToApply.getSequence()).thenReturn(23l);
        when(this.record.getSequence()).thenReturn(1l);
        when(this.changeToApply.getScope()).thenReturn(IRecordChange.IMAGE_SCOPE);

        assertEquals(ImageDeltaChangeProcessor.RESYNC, this.candidate.processRxChange(this.changeToApply, this.name, this.record));
        verify(this.changeToApply).getScope();
        verifyGetSequenceCalled();
        verifyNoMoreInteractions(this.record, this.changeToApply);
    }
    
    @Test
    public void testProcessRxChange_Image_Sequence_Wrong_deltas_pending_incorrect_sequences()
    {
        // check we apply pending deltas ontop of the image (we skip a delta that is before the
        // image) BUT the delta sequences are wrong - expect a resync
        
        this.candidate.imageReceived.clear();
        final LinkedHashMap<Long, IRecordChange> deltas = new LinkedHashMap<Long, IRecordChange>();
        this.candidate.cachedDeltas.put(this.name, deltas);
        IRecordChange change20 = mock(IRecordChange.class);
        IRecordChange change24 = mock(IRecordChange.class);
        IRecordChange change25 = mock(IRecordChange.class);
        deltas.put(20l, change20);
        // add 25 before 24
        deltas.put(25l, change25);
        deltas.put(24l, change24);
        when(change20.getSequence()).thenReturn(20l);
        when(change24.getSequence()).thenReturn(24l);
        when(change25.getSequence()).thenReturn(25l);
        
        final long changeSeq = 23l;
        when(this.changeToApply.getSequence()).thenReturn(changeSeq);
        when(this.record.getSequence()).thenReturn(1l);
        when(this.changeToApply.getScope()).thenReturn(IRecordChange.IMAGE_SCOPE);
        
        assertEquals(ImageDeltaChangeProcessor.RESYNC, this.candidate.processRxChange(this.changeToApply, this.name, this.record));
        
        // check the delta is cleared
        assertEquals(0, this.candidate.cachedDeltas.size());
        
        verify(this.changeToApply).applyCompleteAtomicChangeToRecord(eq(this.record));
        verify(change25).applyCompleteAtomicChangeToRecord(eq(this.record));

        // change 24 is not applied as it is found AFTER 25 and we then resync
        verify(change24, never()).applyCompleteAtomicChangeToRecord(eq(this.record));
        // change 20 is not applied (the image is for 23)
        verify(change20, never()).applyCompleteAtomicChangeToRecord(eq(this.record));
        verify(this.changeToApply).getScope();
        verifyGetSequenceCalled();
        verifyNoMoreInteractions(this.record, this.changeToApply);
    }

    @Test
    public void testProcessRxChange_Image_Sequence_Wrong_deltas_pending()
    {
        // check we apply pending deltas ontop of the image (we skip a delta that is before the
        // image)
        
        this.candidate.imageReceived.clear();
        final LinkedHashMap<Long, IRecordChange> deltas = new LinkedHashMap<Long, IRecordChange>();
        this.candidate.cachedDeltas.put(this.name, deltas);
        IRecordChange change20 = mock(IRecordChange.class);
        IRecordChange change24 = mock(IRecordChange.class);
        IRecordChange change25 = mock(IRecordChange.class);
        deltas.put(20l, change20);
        deltas.put(24l, change24);
        deltas.put(25l, change25);
        when(change20.getSequence()).thenReturn(20l);
        when(change24.getSequence()).thenReturn(24l);
        when(change25.getSequence()).thenReturn(25l);

        final long changeSeq = 23l;
        when(this.changeToApply.getSequence()).thenReturn(changeSeq);
        when(this.record.getSequence()).thenReturn(1l);
        when(this.changeToApply.getScope()).thenReturn(IRecordChange.IMAGE_SCOPE);

        assertEquals(ImageDeltaChangeProcessor.PUBLISH, this.candidate.processRxChange(this.changeToApply, this.name, this.record));

        // check the delta is cleared
        assertEquals(0, this.candidate.cachedDeltas.size());

        verify(this.changeToApply).applyCompleteAtomicChangeToRecord(eq(this.record));
        verify(change24).applyCompleteAtomicChangeToRecord(eq(this.record));
        verify(change25).applyCompleteAtomicChangeToRecord(eq(this.record));

        // change 20 is not applied (the image is for 23)
        verify(change20, never()).applyCompleteAtomicChangeToRecord(eq(this.record));
        verify(this.changeToApply).getScope();
        verifyGetSequenceCalled();
        verifyNoMoreInteractions(this.record, this.changeToApply);
    }

    @Test
    public void testProcessRxChange_Delta_Sequence_Wrong_no_image_yet()
    {
        this.candidate.imageReceived.clear();
        
        final long changeSeq = 23l;
        when(this.changeToApply.getSequence()).thenReturn(changeSeq);
        when(this.record.getSequence()).thenReturn(1l);
        
        when(this.changeToApply.getScope()).thenReturn(IRecordChange.DELTA_SCOPE);
        
        assertEquals(ImageDeltaChangeProcessor.NOOP, this.candidate.processRxChange(this.changeToApply, this.name, this.record));
        
        // check the delta is cached
        assertEquals(1, this.candidate.cachedDeltas.size());
        assertEquals(1, this.candidate.cachedDeltas.get(this.name).size());
        assertEquals(this.changeToApply, this.candidate.cachedDeltas.get(this.name).get(changeSeq));
        
        verify(this.changeToApply).getScope();
        verifyGetSequenceCalled();
        verifyNoMoreInteractions(this.record, this.changeToApply);
    }
    
    @Test
    public void testProcessRxChange_Delta_Sequence_Wrong_image_received()
    {
        final long changeSeq = 23l;
        when(this.changeToApply.getSequence()).thenReturn(changeSeq);
        when(this.record.getSequence()).thenReturn(1l);

        when(this.changeToApply.getScope()).thenReturn(IRecordChange.DELTA_SCOPE);

        assertEquals(ImageDeltaChangeProcessor.RESYNC, this.candidate.processRxChange(this.changeToApply, this.name, this.record));

        assertEquals(0, this.candidate.cachedDeltas.size());

        verify(this.changeToApply).getScope();
        verifyGetSequenceCalled();
        verifyNoMoreInteractions(this.record, this.changeToApply);
    }

    @Test
    public void testUnsubscribed()
    {
        this.candidate.imageReceived.put(this.name, Boolean.TRUE);
        this.candidate.unsubscribed(this.name);
        assertEquals(0, this.candidate.imageReceived.size());
    }

}
