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

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.HybridProtocolCodec.KeyCodesProducer;
import com.fimtra.datafission.field.LongValue;

/**
 * Tests for the {@link HybridProtocolCodec}
 * 
 * @author Ramon Servadei
 */
@Ignore
public class HybridProtocolCodecTest extends CodecBaseTest
{
    @Override
    ICodec<?> constructCandidate()
    {
        KeyCodesProducer.KEY_CODE_DICTIONARY.clear();
        KeyCodesProducer.NEXT_CODE.set(1);
        return new HybridProtocolCodec();
    }

//    @Test
    public void testVersusString()
    {
        Map<String, IValue> addedEntries = new HashMap<String, IValue>();
        addedEntries.put("key1", LongValue.valueOf(1));
        addedEntries.put("key2", LongValue.valueOf(2));
        addedEntries.put("key3", LongValue.valueOf(3));
        addedEntries.put("key4", LongValue.valueOf(4));

        byte[] hybrid =
            (this.candidate.getTxMessageForAtomicChange(new AtomicChange(name, addedEntries,
                new HashMap<String, IValue>(), new HashMap<String, IValue>())));
        byte[] string =
            (new StringProtocolCodec().getTxMessageForAtomicChange(new AtomicChange(name, addedEntries,
                new HashMap<String, IValue>(), new HashMap<String, IValue>())));
        hybrid =
            (this.candidate.getTxMessageForAtomicChange(new AtomicChange(name, addedEntries,
                new HashMap<String, IValue>(), new HashMap<String, IValue>())));
        string =
            (new StringProtocolCodec().getTxMessageForAtomicChange(new AtomicChange(name, addedEntries,
                new HashMap<String, IValue>(), new HashMap<String, IValue>())));
        System.err.println("hybrid.length=" + hybrid.length + ", string.length=" + string.length);
        assertTrue("hybrid.length=" + hybrid.length + ", string.length=" + string.length, hybrid.length < string.length);
    }
}
