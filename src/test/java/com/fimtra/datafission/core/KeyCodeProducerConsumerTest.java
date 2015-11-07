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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.core.HybridProtocolCodec.KeyCodesConsumer;
import com.fimtra.datafission.core.HybridProtocolCodec.KeyCodesProducer;

/**
 * Tests for a {@link KeyCodesProducer} and {@link KeyCodesConsumer}
 * 
 * @author Ramon Servadei
 */
public class KeyCodeProducerConsumerTest
{

    @Before
    public void setUp() throws Exception
    {
        KeyCodesProducer.KEY_CODE_DICTIONARY.clear();
        KeyCodesProducer.NEXT_CODE.set(1);
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testProduceConsume()
    {
        KeyCodesProducer producer = new KeyCodesProducer();
        final int MAX = 3000;
        Set<String> names = new HashSet<String>();
        for (int i = 0; i < MAX; i++)
        {
            names.add("F" + i);
        }
        final ByteBuffer wireFormat = producer.produceWireFormat(names);

        KeyCodesConsumer other = new KeyCodesConsumer();
        byte[] data = new byte[wireFormat.limit()];
        wireFormat.get(data, 0, data.length);
        other.consumeWireFormat(data);

        for (int i = 0; i < MAX; i++)
        {
            assertEquals(i, producer.getCodeFor(other.getKeyForCode((char) i)));
        }
    }

    @Test
    public void testDuplicateProduce()
    {
        KeyCodesProducer producer = new KeyCodesProducer();
        final int MAX = 3000;
        Set<String> names = new HashSet<String>();
        for (int i = 0; i < MAX; i++)
        {
            names.add("F" + i);
        }
        producer.produceWireFormat(names);
        final Map<String, Character> keyCodes = new HashMap<String, Character>(producer.keyCodes);
        // try again, we should not add new codes
        producer.produceWireFormat(names);
        assertEquals(keyCodes, producer.keyCodes);
    }

    @Test
    public void testCodesAreReUsed()
    {
        KeyCodesProducer producer = new KeyCodesProducer();
        final int MAX = 50;
        Set<String> names = new HashSet<String>();
        for (int i = 0; i < MAX; i++)
        {
            names.add("F" + i);
        }
        producer.produceWireFormat(names);

        // try again, we should not add new codes
        KeyCodesProducer producer2 = new KeyCodesProducer();
        Set<String> names2 = new HashSet<String>();
        // reverse the names
        for (int i = MAX - 1; i > -1; i--)
        {
            names2.add("F" + i);
        }
        producer2.produceWireFormat(names2);

        assertEquals(producer.keyCodes, producer2.keyCodes);
    }
}
