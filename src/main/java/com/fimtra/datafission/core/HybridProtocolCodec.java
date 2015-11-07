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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.field.AbstractValue;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.ByteBufferUtils;

/**
 * A codec for messages that are sent between a {@link Publisher} and {@link ProxyContext} using a
 * hybrid protocol of text and binary; text for action-type messages and a binary for atomic change
 * messages.
 * <p>
 * NOTE: the binary protocol uses an <b>unsigned</b> short data type for each field code and also
 * for the length of the binary data for the field value. This limits this codec to supporting
 * runtimes that have 2^16 - 1 distinct field names (65535) and also limits the maximum size of any
 * field's data to 65536 bytes. However, this should be sufficient for any application.
 * 
 * @see #getTxMessageForAtomicChange(IRecordChange)
 * @author Ramon Servadei
 */
public class HybridProtocolCodec extends StringProtocolCodec
{
    private static final byte NULL_DATA_TYPE = (byte) 127;
    private static final byte[] EMPTY_ARRAY = new byte[0];

    /**
     * Produces wire-formats for key-codes. ABNF for wire-format for key-codes:
     * 
     * <pre>
     *  key-codes = 1*key-code-spec ; the integer code for each key
     *  key-code-spec = key-name-len key-name key-code
     *  key-name-len = 2OCTET ; the length of the key-name
     *  key-name = 1*ALPHA; UTF8
     *  key-code = 2OCTET ; the integer code for the key
     * </pre>
     * 
     * @author Ramon Servadei
     */
    static class KeyCodesProducer
    {
        /**
         * The dictionary of key names and codes - this ensures that the same key across all records
         * will use the same code for the lifetime of the runtime.
         * <p>
         * {@link Character} is used as it is an unsigned, 16bit data type giving 65535 possible
         * field codes.
         */
        static final Map<String, Character> KEY_CODE_DICTIONARY = new HashMap<String, Character>();
        static final AtomicInteger NEXT_CODE = new AtomicInteger(1);

        static final char NULL_KEY_CODE = 0;

        final Map<String, Character> keyCodes;

        KeyCodesProducer()
        {
            this.keyCodes = new ConcurrentHashMap<String, Character>();
        }

        ByteBuffer produceWireFormat(Set<String> names)
        {
            ByteBuffer buffer = ByteBuffer.allocate(ByteBufferUtils.BLOCK_SIZE);
            byte[] keyName;
            Character keyCode;
            synchronized (KEY_CODE_DICTIONARY)
            {
                for (String key : names)
                {
                    if (key == null || this.keyCodes.containsKey(key))
                    {
                        continue;
                    }
                    keyName = key.getBytes(UTF8);
                    buffer = ByteBufferUtils.putChar((char) keyName.length, buffer);
                    buffer = ByteBufferUtils.copyBytesIntoBuffer(keyName, buffer);

                    keyCode = KEY_CODE_DICTIONARY.get(key);
                    if (keyCode == null)
                    {
                        keyCode = Character.valueOf((char) NEXT_CODE.getAndIncrement());
                        KEY_CODE_DICTIONARY.put(key, keyCode);
                    }
                    buffer = ByteBufferUtils.putChar(keyCode.charValue(), buffer);
                    this.keyCodes.put(key, keyCode);
                }
            }
            buffer.flip();
            return buffer;
        }

        char getCodeFor(String key)
        {
            if (key == null)
            {
                return NULL_KEY_CODE;
            }
            final Character code = this.keyCodes.get(key);
            if (code == null)
            {
                throw new NullPointerException("No keyCode for '" + key + "', codes=" + this.keyCodes);
            }
            return code.charValue();
        }
    }

    /**
     * Consumes wire-formats for key-codes. ABNF for wire-format for key-codes:
     * 
     * <pre>
     *  key-codes = 1*key-code-spec ; the integer code for each key
     *  key-code-spec = key-name-len key-name key-code
     *  key-name-len = 2OCTET ; the length of the key-name
     *  key-name = 1*ALPHA; UTF8
     *  key-code = 2OCTET ; the integer code for the key
     * </pre>
     * 
     * @author Ramon Servadei
     */
    static class KeyCodesConsumer
    {
        final Map<Character, String> reverseKeyCodes;

        KeyCodesConsumer()
        {
            this.reverseKeyCodes = new ConcurrentHashMap<Character, String>();
        }

        void consumeWireFormat(byte[] messagePart)
        {
            final ByteBuffer buffer = ByteBuffer.wrap(messagePart);
            byte[] nameBytes;
            String value;
            Character key;
            while (buffer.position() < buffer.limit())
            {
                nameBytes = new byte[buffer.getChar()];
                System.arraycopy(messagePart, buffer.position(), nameBytes, 0, nameBytes.length);
                buffer.position(buffer.position() + nameBytes.length);
                value = new String(nameBytes, UTF8);
                key = Character.valueOf(buffer.getChar());
                this.reverseKeyCodes.put(key, value);
            }
        }

        String getKeyForCode(char keyCode)
        {
            if (keyCode == KeyCodesProducer.NULL_KEY_CODE)
            {
                return null;
            }
            final String key = this.reverseKeyCodes.get(Character.valueOf(keyCode));
            if (key == null)
            {
                throw new NullPointerException("No key found for code: " + keyCode + "(" + (int) keyCode + "), codes="
                    + this.reverseKeyCodes);
            }
            return key;
        }
    }

    /**
     * The key codes consumer that will provide the ability to resolve all keycodes into a key name
     * for all records
     */
    final KeyCodesConsumer keyCodeConsumer;
    /**
     * For sending there is a producer per record - this ensures we know which keycodes have already
     * been sent for a key
     */
    final ConcurrentMap<String, KeyCodesProducer> keyCodeProducers;
    /**
     * Tracks how many messages have been sent - the first message sent needs to include the entire
     * {@link KeyCodesProducer#KEY_CODE_DICTIONARY}
     */
    final AtomicLong messageCount = new AtomicLong();

    public HybridProtocolCodec()
    {
        super();
        this.keyCodeConsumer = new KeyCodesConsumer();
        this.keyCodeProducers = new ConcurrentHashMap<String, KeyCodesProducer>();
    }

    /**
     * Get the byte[] representing the record changes to transmit to a {@link ProxyContext}. The
     * format of the data in ABNF notation:
     * 
     * <pre>
     *  seq atomic-change [sub-map-atomic-changes]
     * 
     *  seq = scope seq_num
     *  scope = "i" | "d" ; identifies either an image or delta, char
     *  seq_num = 8OCTET ; the sequency number as a long
     *  
     *  atomic-change = name-len name key-codes-len [key-codes] put-len [puts] remove-len [removes]
     *  sub-map-atomic-changes = 0*atomic-change
     *  
     *  name-len = 2OCTET ; the length of name in bytes
     *  key-codes-len = 4OCTET ; the length of key-codes in bytes
     *  put-len = 4OCTET ; the length of puts in bytes
     *  remove-len = 4OCTET ;  the length of removes in bytes
     *  
     *  name = 1*ALPHA ; the name of the notifying record instance (UTF8)
     *  
     *  key-codes = 0*key-code-spec ; the integer code for each key
     *  
     *  key-code-spec = key-name-len key-name key-code
     *  key-name-len = 2OCTET ; the length of the key-name
     *  key-name = 1*ALPHA; UTF8
     *  key-code = 2OCTET ; the integer code for the key
     *  
     *  puts = 0*key-value
     *  
     *  removes = 0*key-value
     * 
     *  key-value = key-code data-type [data-len] data
     *  key-code = 2OCTET ; the integer code for the key
     *  data-type = OCTET; the data type
     *  data-len = 2OCTET; the length for the data in bytes, only present for text data type, others are 8 bytes
     *  data = 1*OCTET; the data
     * </pre>
     * 
     * @param subMapName
     *            the name of the record
     * @param putEntries
     *            the entries that were put
     * @param removedEntries
     *            the entries that were removed
     * @return the data[] representing the changes
     */
    @Override
    public byte[] getTxMessageForAtomicChange(IRecordChange atomicChange)
    {
        boolean sendDictionary = this.messageCount.getAndIncrement() == 0;
        final Set<String> subMapKeys = atomicChange.getSubMapKeys();
        ByteBuffer buffer = ByteBuffer.allocate(ByteBufferUtils.BLOCK_SIZE);
        final KeyCodesProducer keyCodeProducer = getKeyCodeProducer(atomicChange.getName());

        // scope char
        buffer = ByteBufferUtils.putChar(atomicChange.getScope(), buffer);
        // sequence number
        buffer = buffer.putLong(atomicChange.getSequence());

        buffer = encodeAtomicChange(keyCodeProducer, atomicChange, buffer, sendDictionary);

        if (subMapKeys.size() > 0)
        {
            for (String subMapKey : subMapKeys)
            {
                buffer = encodeAtomicChange(keyCodeProducer, atomicChange.getSubMapAtomicChange(subMapKey), buffer);
            }
        }

        buffer.flip();
        return ByteBufferUtils.getBytesFromBuffer(buffer, buffer.limit());
    }

    private static ByteBuffer encodeAtomicChange(KeyCodesProducer keyCodeProducer, IRecordChange atomicChange,
        ByteBuffer buffer)
    {
        return encodeAtomicChange(keyCodeProducer, atomicChange, buffer, false);
    }

    private static ByteBuffer encodeAtomicChange(KeyCodesProducer keyCodeProducer, IRecordChange atomicChange,
        ByteBuffer buffer, boolean sendCompleteDictionary)
    {
        final byte[] name = atomicChange.getName().getBytes(UTF8);
        ByteBuffer localBuf = buffer;
        // name-len
        localBuf = ByteBufferUtils.putChar((char) name.length, localBuf);
        // name
        localBuf = ByteBufferUtils.put(name, localBuf);

        final byte[] allCodes;
        if (sendCompleteDictionary)
        {
            synchronized (KeyCodesProducer.KEY_CODE_DICTIONARY)
            {
                allCodes =
                    ByteBufferUtils.asBytes(keyCodeProducer.produceWireFormat(KeyCodesProducer.KEY_CODE_DICTIONARY.keySet()));
            }
        }
        else
        {
            allCodes = EMPTY_ARRAY;
        }

        // we must always perform this in case we need to add new keys that are currently not in the
        // full dictionary
        final byte[] putKeyCodes =
            ByteBufferUtils.asBytes(keyCodeProducer.produceWireFormat(atomicChange.getPutEntries().keySet()));
        final byte[] removeKeyCodes =
            ByteBufferUtils.asBytes(keyCodeProducer.produceWireFormat(atomicChange.getRemovedEntries().keySet()));

        // key-codes-len
        localBuf.putInt((putKeyCodes.length + removeKeyCodes.length + allCodes.length));
        // key-codes
        localBuf = ByteBufferUtils.copyBytesIntoBuffer(putKeyCodes, localBuf);
        localBuf = ByteBufferUtils.copyBytesIntoBuffer(removeKeyCodes, localBuf);
        localBuf = ByteBufferUtils.copyBytesIntoBuffer(allCodes, localBuf);

        final ByteBuffer reuse8ByteBuffer = ByteBuffer.allocate(8);

        // put-len
        // but we don't know the length yet, so save the position
        final int putLenPos = localBuf.position();
        localBuf = ByteBufferUtils.putInt(0, localBuf);
        localBuf = addKeyValues(keyCodeProducer, localBuf, reuse8ByteBuffer, atomicChange.getPutEntries(), putLenPos);

        // remove-len
        // but we don't know the length yet, so save the position
        final int removeLenPos = localBuf.position();
        localBuf = ByteBufferUtils.putInt(0, localBuf);
        localBuf =
            addKeyValues(keyCodeProducer, localBuf, reuse8ByteBuffer, atomicChange.getRemovedEntries(), removeLenPos);
        return localBuf;
    }

    static ByteBuffer addKeyValues(KeyCodesProducer keyCodeProducer, ByteBuffer buffer,
        final ByteBuffer reuse8ByteBuffer, final Map<String, IValue> entries, final int lenPos)
    {
        byte[] dataBytes;
        Map.Entry<String, IValue> entry;
        String key;
        IValue value;
        TypeEnum type;
        ByteBuffer localBuf = buffer;
        if (entries.size() > 0)
        {
            for (Iterator<Map.Entry<String, IValue>> it = entries.entrySet().iterator(); it.hasNext();)
            {
                reuse8ByteBuffer.clear();
                entry = it.next();
                key = entry.getKey();
                value = entry.getValue();

                // key-code
                localBuf = ByteBufferUtils.putChar(keyCodeProducer.getCodeFor(key), localBuf);
                if (value == null)
                {
                    localBuf = ByteBufferUtils.put(NULL_DATA_TYPE, localBuf);
                    // no data-len
                }
                else
                {
                    // data-type
                    type = value.getType();
                    localBuf = ByteBufferUtils.put((byte) type.ordinal(), localBuf);
                    dataBytes = AbstractValue.toBytes(value, reuse8ByteBuffer);
                    // only add the length for text - others are 8bytes by default
                    switch(type)
                    {
                        case DOUBLE:
                        case LONG:
                            break;
                        case TEXT:
                        case BLOB:
                            // data-len
                            localBuf = ByteBufferUtils.putChar((char) dataBytes.length, localBuf);
                            break;
                    }
                    // data
                    localBuf = ByteBufferUtils.copyBytesIntoBuffer(dataBytes, localBuf);
                }
            }
            localBuf.putInt(lenPos, (localBuf.position() - (lenPos + 4)));
        }
        return localBuf;
    }

    private KeyCodesProducer getKeyCodeProducer(String name)
    {
        KeyCodesProducer producer = this.keyCodeProducers.get(name);
        if (producer == null)
        {
            this.keyCodeProducers.putIfAbsent(name, new KeyCodesProducer());
            return this.keyCodeProducers.get(name);
        }
        return producer;
    }

    /**
     * Convert a byte[] created from the {@link #getTxMessageForAtomicChange(String, Map, Map)}
     * method into a {@link AtomicChange} representing the 'puts' and 'removes' to a named record
     * instance.
     * 
     * @param data
     *            the received byte[] data
     * @return the converted change from the data
     * @throws RuntimeException
     *             if there is a problem converting
     */
    @Override
    public IRecordChange getAtomicChangeFromRxMessage(byte[] data)
    {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        final char scope = buffer.getChar();
        final long sequence = buffer.getLong();
        String name = new String(ByteBufferUtils.getBytesFromBuffer(buffer, buffer.getChar()), UTF8);
        final AtomicChange atomicChange = new AtomicChange(name);

        atomicChange.setScope(scope);
        atomicChange.setSequence(sequence);

        try
        {
            decodeAtomicChange(this.keyCodeConsumer, atomicChange, buffer, null);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not complete " + atomicChange, e);
        }

        while (buffer.position() < buffer.limit())
        {
            name = new String(ByteBufferUtils.getBytesFromBuffer(buffer, buffer.getChar()), UTF8);
            decodeAtomicChange(this.keyCodeConsumer, atomicChange, buffer, name);
        }

        return atomicChange;
    }

    private static void decodeAtomicChange(KeyCodesConsumer keyCodesConsumer, AtomicChange atomicChange,
        ByteBuffer buffer, String submapName)
    {
        // key-codes-len
        final int keyCodesLen = buffer.getInt();

        if (keyCodesLen > 0)
        {
            // key-codes
            keyCodesConsumer.consumeWireFormat(ByteBufferUtils.getBytesFromBuffer(buffer, keyCodesLen));
        }

        char keyCode;
        byte dataType;
        TypeEnum type;
        char dataLen = 0;
        IValue value;

        final int putLen = buffer.getInt();
        final int putEnd = buffer.position() + putLen;
        while (buffer.position() < putEnd)
        {
            keyCode = buffer.getChar();
            dataType = buffer.get();
            if (dataType == NULL_DATA_TYPE)
            {
                if (submapName != null)
                {
                    atomicChange.mergeSubMapEntryUpdatedChange(submapName, keyCodesConsumer.getKeyForCode(keyCode),
                        null, null);
                }
                else
                {
                    atomicChange.mergeEntryUpdatedChange(keyCodesConsumer.getKeyForCode(keyCode), null, null);
                }
            }
            else
            {
                type = TypeEnum.values()[dataType];
                switch(type)
                {
                    case DOUBLE:
                    case LONG:
                        dataLen = 8;
                        break;
                    case TEXT:
                    case BLOB:
                        // data-len only exists for text
                        dataLen = buffer.getChar();
                        break;
                }
                value = AbstractValue.fromBytes(type, buffer, dataLen);
                if (submapName != null)
                {
                    atomicChange.mergeSubMapEntryUpdatedChange(submapName, keyCodesConsumer.getKeyForCode(keyCode),
                        value, null);
                }
                else
                {
                    atomicChange.mergeEntryUpdatedChange(keyCodesConsumer.getKeyForCode(keyCode), value, null);
                }
            }
        }

        final int removeLen = buffer.getInt();
        final int removeEnd = buffer.position() + removeLen;
        while (buffer.position() < removeEnd)
        {
            keyCode = buffer.getChar();
            dataType = buffer.get();
            if (dataType == NULL_DATA_TYPE)
            {
                if (submapName != null)
                {
                    atomicChange.mergeSubMapEntryRemovedChange(submapName, keyCodesConsumer.getKeyForCode(keyCode),
                        null);
                }
                else
                {
                    atomicChange.mergeEntryRemovedChange(keyCodesConsumer.getKeyForCode(keyCode), null);
                }
            }
            else
            {
                type = TypeEnum.values()[dataType];
                switch(type)
                {
                    case DOUBLE:
                    case LONG:
                        dataLen = 8;
                        break;
                    case TEXT:
                    case BLOB:
                        // data-len only exists for text (or blob)
                        dataLen = buffer.getChar();
                        break;
                }
                value = AbstractValue.fromBytes(type, buffer, dataLen);
                if (submapName != null)
                {
                    atomicChange.mergeSubMapEntryRemovedChange(submapName, keyCodesConsumer.getKeyForCode(keyCode),
                        value);
                }
                else
                {
                    atomicChange.mergeEntryRemovedChange(keyCodesConsumer.getKeyForCode(keyCode), value);
                }
            }
        }
    }

    @Override
    public FrameEncodingFormatEnum getFrameEncodingFormat()
    {
        return FrameEncodingFormatEnum.LENGTH_BASED;
    }

    @Override
    public ICodec<char[]> newInstance()
    {
        return new HybridProtocolCodec();
    }
}
