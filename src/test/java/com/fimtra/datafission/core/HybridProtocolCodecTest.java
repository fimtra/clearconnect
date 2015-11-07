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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.core.AtomicChange;
import com.fimtra.datafission.core.HybridProtocolCodec;
import com.fimtra.datafission.core.HybridProtocolCodec.KeyCodesProducer;

/**
 * Tests for the {@link HybridProtocolCodec}
 * 
 * @author Ramon Servadei
 */
public class HybridProtocolCodecTest extends CodecBaseTest
{
    @Override
    ICodec<?> constructCandidate()
    {
        return new HybridProtocolCodec();
    }

    @SuppressWarnings("boxing")
    @Test
    public void testFirstMessageHasFullDictionary()
    {
        KeyCodesProducer.KEY_CODE_DICTIONARY.put("a key", (char) 900);
        KeyCodesProducer.KEY_CODE_DICTIONARY.put("a key2", (char) 901);

        final Map<Character, String> reverseKeyCodes =
            ((HybridProtocolCodec) this.candidate).keyCodeConsumer.reverseKeyCodes;
        assertEquals(0, reverseKeyCodes.size());

        final byte[] txMessageForAtomicChange =
            this.candidate.getTxMessageForAtomicChange(new AtomicChange("a change"));
        this.candidate.getAtomicChangeFromRxMessage(txMessageForAtomicChange);

        assertEquals(KeyCodesProducer.KEY_CODE_DICTIONARY.size(), reverseKeyCodes.size());
        Map<String, Character> codes = new HashMap<String, Character>();

        Map.Entry<Character, String> entry = null;
        Character key = null;
        String value = null;
        for (Iterator<Map.Entry<Character, String>> it = reverseKeyCodes.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            key = entry.getKey();
            value = entry.getValue();
            codes.put(value, key);
        }

        assertEquals(KeyCodesProducer.KEY_CODE_DICTIONARY, codes);
    }
}
