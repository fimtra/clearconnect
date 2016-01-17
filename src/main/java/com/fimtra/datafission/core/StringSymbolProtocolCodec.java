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
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.AbstractValue;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.util.Log;

/**
 * A codec for messages that are sent between a {@link Publisher} and {@link ProxyContext} using a
 * string text protocol with a symbol table for string substitution of record names and keys in
 * atomic changes. This reduces the transmission size of messages once the symbol mapping for a
 * record name or key is sent. Typically a symbol will be between 1-4 chars (but is unlimited). The
 * format of the string in ABNF notation:
 * 
 * <pre>
 *  preamble name-symbol seq [puts] [removes] [sub-map]
 *  
 *  preamble = 0*ALPHA
 *  name-symbol = "|" ( name-symbol-definition | name-symbol-instance )
 *  name-symbol-definition = "n" name name-symbol-instance ; if the symbol for the record has never been sent
 *  name-symbol-instance = "~" symbol-name
 *  name = 1*ALPHA ; the name of the notifying record instance
 *  symbol-name = 1*ALPHA ; the symbol to substitute for the name
 *  seq = "|" scope seq_num
 *  scope = "i" | "d" ; identifies either an image or delta
 *  seq_num = 1*DIGIT ; the sequency number
 *  puts = "|p" 1*key-value-pair
 *  removes = "|r" 1*key-value-pair
 *  sub-map = "|:|" name [puts] [removes]
 *  key-value-pair = "|" key-symbol "=" value
 *  key-symbol = ( key-symbol-definition | key-symbol-instance )
 *  key-symbol-definition =  "n" key key-symbol-instance
 *  key-symbol-instance = "~" symbol-name
 *  key = 1*ALPHA ; the symbol for the key
 *  value = 1*ALPHA
 *  
 *  e.g. |nmyrecord~s!|d322234|p|nkey1~#=value1|~df=value2|r|~$=value5|:|~$|p|~ty=value1
 * </pre>
 * 
 * @author Ramon Servadei
 */
public final class StringSymbolProtocolCodec extends StringProtocolCodec
{
    public static final class MissingKeySymbolMappingException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;
        final String recordName;

        public MissingKeySymbolMappingException(String name, RuntimeException runtimeException)
        {
            super(runtimeException);
            this.recordName = name;
        }
    }

    /**
     * Controls logging of:
     * <ul>
     * <li>key-symbol tx and rx mappings
     * <li>record-symbol tx and rx mappings
     * </ul>
     */
    public static boolean log = Boolean.getBoolean("log." + StringSymbolProtocolCodec.class.getCanonicalName());

    final static Charset ISO_8859_1 = Charset.forName("ISO-8859-1");

    private static final char START_SYMBOL = ' ';
    private static final char END_SYMBOL = 'z';

    /**
     * Stores the mappings of all key-name-to-symbol sent from this VM
     */
    static final ConcurrentMap<String, String> TX_KEY_NAME_TO_SYMBOL = new ConcurrentHashMap<String, String>();
    /**
     * Stores the mappings of all record-name-to-symbol sent from this VM
     */
    static final Map<String, String> TX_RECORD_NAME_TO_SYMBOL = new HashMap<String, String>();

    static char[] NEXT_TX_RECORD_NAME_SYMBOL = new char[] { START_SYMBOL };
    static char[] NEXT_TX_KEY_NAME_SYMBOL = new char[] { START_SYMBOL };

    private final static synchronized String getNextTxKeyNameSymbol()
    {
        findNextValidSymbol(NEXT_TX_KEY_NAME_SYMBOL);

        if (NEXT_TX_KEY_NAME_SYMBOL[NEXT_TX_KEY_NAME_SYMBOL.length - 1] >= END_SYMBOL)
        {
            // resize
            char[] temp = new char[NEXT_TX_KEY_NAME_SYMBOL.length + 1];
            System.arraycopy(NEXT_TX_KEY_NAME_SYMBOL, 0, temp, 0, NEXT_TX_KEY_NAME_SYMBOL.length);
            NEXT_TX_KEY_NAME_SYMBOL = temp;
            NEXT_TX_KEY_NAME_SYMBOL[NEXT_TX_KEY_NAME_SYMBOL.length - 1] = START_SYMBOL;
        }
        return new String(NEXT_TX_KEY_NAME_SYMBOL, 0, NEXT_TX_KEY_NAME_SYMBOL.length);
    }

    private final static synchronized String getNextTxRecordNameSymbol()
    {
        findNextValidSymbol(NEXT_TX_RECORD_NAME_SYMBOL);

        if (NEXT_TX_RECORD_NAME_SYMBOL[NEXT_TX_RECORD_NAME_SYMBOL.length - 1] >= END_SYMBOL)
        {
            // resize
            char[] temp = new char[NEXT_TX_RECORD_NAME_SYMBOL.length + 1];
            System.arraycopy(NEXT_TX_RECORD_NAME_SYMBOL, 0, temp, 0, NEXT_TX_RECORD_NAME_SYMBOL.length);
            NEXT_TX_RECORD_NAME_SYMBOL = temp;
            NEXT_TX_RECORD_NAME_SYMBOL[NEXT_TX_RECORD_NAME_SYMBOL.length - 1] = START_SYMBOL;
        }
        return new String(NEXT_TX_RECORD_NAME_SYMBOL, 0, NEXT_TX_RECORD_NAME_SYMBOL.length);
    }

    /**
     * Populates the char[] with the next symbol, skipping any special chars (see implementation)
     */
    private final static void findNextValidSymbol(final char[] symbolArr)
    {
        // NOTE: there are "reserved" chars:
        // \r
        // \r
        // |
        // =
        // ~
        // \
        boolean notOk = false;
        do
        {
            notOk = false;
            symbolArr[symbolArr.length - 1] = (char) (symbolArr[symbolArr.length - 1] + 1);
            switch(symbolArr[symbolArr.length - 1])
            {
                case '\r':
                case '\n':
                case '|':
                case '=':
                case '\\':
                case '~':
                    notOk = true;
            }
        }
        while (notOk);
    }

    private final static int getByteCountForInteger(final int data)
    {
        final int bytesToWrite;
        // 4 bytes or less
        if ((data & 0x00007fff) == data)
        {
            // 2 bytes or less
            if ((data & 0x000007f) == data)
            {
                // 1 byte
                bytesToWrite = 1;
            }
            else
            {
                // 2 bytes
                bytesToWrite = 2;
            }
        }
        else
        {
            // more than 2 bytes
            if ((data & 0x007fffff) == data)
            {
                // 3 bytes
                bytesToWrite = 3;
            }
            else
            {
                // 4 bytes
                bytesToWrite = 4;
            }
        }
        return bytesToWrite;
    }

    private final static int getByteCountForLong(final long data)
    {
        final int bytesToWrite;
        if ((data & 0x000000007fffffffL) == data)
        {
            // 4 bytes or less
            bytesToWrite = getByteCountForInteger((int) data);
        }
        else
        {
            // more than 4 bytes
            if ((data & 0x00007fffffffffffL) == data)
            {
                // 6 bytes or less
                if ((data & 0x0000007fffffffffL) == data)
                {
                    // 5 bytes
                    bytesToWrite = 5;
                }
                else
                {
                    // 6 bytes
                    bytesToWrite = 6;
                }
            }
            else
            {
                // more than 6 bytes
                if ((data & 0x007fffffffffffffL) == data)
                {
                    // 7 bytes
                    bytesToWrite = 7;
                }
                else
                {
                    // 8 bytes
                    bytesToWrite = 8;
                }
            }
        }
        return bytesToWrite;
    }

    /**
     * Tracks the records that have had key definitions sent.
     * <p>
     * Key=record-key, value=set of records that have had the key definition sent.
     * <p>
     * TODO this is a memory leak for "temporary" records...
     */
    final ConcurrentMap<String, Set<String>> keyDefinitionsSent = new ConcurrentHashMap<String, Set<String>>();
    /**
     * Stores the key name to symbol for messages sent by this instance - the mappings used are also
     * stored in the 'master' {@value #TX_KEY_NAME_TO_SYMBOL}
     */
    final ConcurrentMap<String, String> txKeyNameToSymbol = new ConcurrentHashMap<String, String>();
    /**
     * Stores the record name to symbol for messages sent by this instance - the mappings used are
     * also stored in the 'master' {@value #TX_RECORD_NAME_TO_SYMBOL}
     */
    final ConcurrentMap<String, String> txRecordNameToSymbol = new ConcurrentHashMap<String, String>();

    /**
     * Stores the symbol to key name for messages received by this instance
     */
    final ConcurrentMap<String, String> rxSymbolToKey = new ConcurrentHashMap<String, String>();
    /**
     * Stores the symbol to record name for messages received by this instance
     */
    final ConcurrentMap<String, String> rxSymbolToRecordName = new ConcurrentHashMap<String, String>();

    /**
     * Get the string representing the record changes to transmit to a {@link ProxyContext}.
     * 
     * @param subMapName
     *            the name of the record
     * @param putEntries
     *            the entries that were put
     * @param removedEntries
     *            the entries that were removed
     * @return the string representing the changes
     */
    @Override
    public byte[] getTxMessageForAtomicChange(IRecordChange atomicChange)
    {
        return encodeAtomicChangeWithSymbols(DELIMITER, atomicChange);
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
        return decodeAtomicChangeWithSymbols(decode(data));
    }

    IRecordChange decodeAtomicChangeWithSymbols(char[] decodedMessage)
    {
        String name = null;
        try
        {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            byte[] array = buffer.array();

            char[] tempArr = new char[50];
            final char[][] tokens = findTokens(decodedMessage);
            switch(tokens[1][0])
            {
                case '~':
                    // if its just the symbol, no need to unencode
                    // symbol-instance ~<symbol>
                    name = lookupRecordNameSymbol(new String(tokens[1], 1, tokens[1].length - 1));
                    break;
                case 'n':
                    // symbol-definition: n<name>~<symbol>
                    name = defineRecordNameSymbol(stringFromCharBuffer(tokens[1], 0));
                    break;
                default :
                    // assume its a full name (no symbol table), e.g. RPC results
                    name = new String(tokens[1], 0, tokens[1].length);
            }

            final AtomicChange atomicChange = new AtomicChange(name);

            // set the scope and sequence
            final char[] sequenceToken = tokens[2];
            atomicChange.setScope(sequenceToken[0]);
            atomicChange.setSequence(LongValue.valueOf(sequenceToken, 1, sequenceToken.length - 1).longValue());

            boolean put = true;
            boolean processSubmaps = false;
            String subMapName = null;
            if (tokens.length > 2)
            {
                for (int i = 3; i < tokens.length; i++)
                {
                    if (tokens[i].length == 1)
                    {
                        switch(tokens[i][0])
                        {
                            case PUT_CODE:
                                put = true;
                                break;
                            case REMOVE_CODE:
                                put = false;
                                break;
                            case SUBMAP_CODE:
                                processSubmaps = true;
                                switch(tokens[++i][0])
                                {
                                    case '~':
                                        // if its a symbol, no need to unescape
                                        // symbol-instance ~<symbol>
                                        subMapName =
                                            lookupRecordNameSymbol(new String(tokens[i], 1, tokens[i].length - 1));
                                        break;
                                    case 'n':
                                        // symbol-definition: n<name>~s<symbol>
                                        subMapName = defineRecordNameSymbol(stringFromCharBuffer(tokens[i], 0));
                                        break;
                                    default :
                                        throw new IllegalArgumentException("Could not decode submap name from "
                                            + new String(decodedMessage) + ", submap portion: "
                                            + new String(tokens[++i]));
                                }
                                break;
                            default :
                                break;
                        }
                    }
                    else
                    {
                        char[] chars = tokens[i];
                        char previous = 0;
                        char[] currentTokenChars;
                        for (int j = 0; j < chars.length; j++)
                        {
                            switch(chars[j])
                            {
                                case '=':
                                    // find where the first non-escaped "=" is
                                    if (previous != '\\')
                                    {
                                        currentTokenChars = tokens[i];
                                        if (tempArr.length < currentTokenChars.length)
                                        {
                                            tempArr = new char[currentTokenChars.length];
                                        }
                                        if (processSubmaps)
                                        {
                                            if (put)
                                            {
                                                atomicChange.mergeSubMapEntryUpdatedChange(
                                                    subMapName,
                                                    decodeKeyFromSymbol(currentTokenChars, 0, j),
                                                    decodeValue(currentTokenChars, j + 1, currentTokenChars.length,
                                                        tempArr, buffer, array), null);
                                            }
                                            else
                                            {
                                                atomicChange.mergeSubMapEntryRemovedChange(
                                                    subMapName,
                                                    decodeKeyFromSymbol(currentTokenChars, 0, j),
                                                    decodeValue(currentTokenChars, j + 1, currentTokenChars.length,
                                                        tempArr, buffer, array));
                                            }
                                        }
                                        else
                                        {
                                            if (put)
                                            {
                                                atomicChange.mergeEntryUpdatedChange(
                                                    decodeKeyFromSymbol(currentTokenChars, 0, j),
                                                    decodeValue(currentTokenChars, j + 1, currentTokenChars.length,
                                                        tempArr, buffer, array), null);
                                            }
                                            else
                                            {
                                                atomicChange.mergeEntryRemovedChange(
                                                    decodeKeyFromSymbol(currentTokenChars, 0, j),
                                                    decodeValue(currentTokenChars, j + 1, currentTokenChars.length,
                                                        tempArr, buffer, array));
                                            }
                                        }
                                        j = chars.length;
                                    }
                                    break;
                                default :
                                    previous = chars[j];
                            }
                        }
                    }
                }
            }
            return atomicChange;
        }
        catch (Exception e)
        {
            final RuntimeException runtimeException =
                new RuntimeException("Could not decode '" + new String(decodedMessage) + "'", e);
            if (name != null)
            {
                throw new MissingKeySymbolMappingException(name, runtimeException);
            }
            else
            {
                throw runtimeException;
            }
        }
    }

    private String lookupRecordNameSymbol(String symbol)
    {
        final String name = this.rxSymbolToRecordName.get(symbol);
        if (name == null)
        {
            throw new IllegalArgumentException("No record name for symbol '" + symbol + "'");
        }
        return name;
    }

    private String lookupKeySymbol(String symbol)
    {
        final String name = this.rxSymbolToKey.get(symbol);
        if (name == null)
        {
            throw new IllegalArgumentException("No key for symbol '" + symbol + "'");
        }
        return name;
    }

    /**
     * Given a symbol-definition format "n{name}~{symbol}" this populates the {@link #rxSymbolToKey}
     * map with the mapping.
     * 
     * @return the name
     */
    private String defineKeySymbol(String nameAndSymbol)
    {
        final char[] charArray = nameAndSymbol.toCharArray();
        final StringBuilder sb = new StringBuilder(nameAndSymbol.length());
        String name = null;
        for (int i = 1; i < charArray.length; i++)
        {
            if (charArray[i] == '~')
            {
                name = sb.toString();
                sb.setLength(0);
            }
            else
            {
                sb.append(charArray[i]);
            }
        }
        if (name == null)
        {
            throw new IllegalArgumentException("Incorrect format for key-symbol-definition: " + nameAndSymbol);
        }
        String symbol = sb.toString();
        if (log)
        {
            Log.log(this, "[rx] key-symbol ", symbol, "=", name);
        }
        this.rxSymbolToKey.put(symbol, name);
        return name;
    }

    /**
     * Given a symbol-definition format "n{name}~s{symbol}" this populates the
     * {@link #rxSymbolToRecordName} map with the mapping.
     * 
     * @return the name
     */
    private String defineRecordNameSymbol(String nameAndSymbol)
    {
        final char[] charArray = nameAndSymbol.toCharArray();
        final StringBuilder sb = new StringBuilder(nameAndSymbol.length());
        String name = null;
        for (int i = 1; i < charArray.length; i++)
        {
            if (charArray[i] == '~')
            {
                name = sb.toString();
                sb.setLength(0);
            }
            else
            {
                sb.append(charArray[i]);
            }
        }
        if (name == null)
        {
            throw new IllegalArgumentException("Incorrect format for name-symbol-definition: " + nameAndSymbol);
        }
        final String symbol = sb.toString();
        if (log)
        {
            Log.log(this, "[rx] record-symbol ", symbol, "=", name);
        }
        this.rxSymbolToRecordName.put(symbol, name);
        return name;
    }

    byte[] encodeAtomicChangeWithSymbols(String preamble, IRecordChange atomicChange)
    {
        final AtomicReference<char[]> chars = new AtomicReference<char[]>(new char[StringProtocolCodec.CHARRAY_SIZE]);
        final AtomicReference<char[]> escapedChars =
            new AtomicReference<char[]>(new char[StringProtocolCodec.ESCAPED_CHARRAY_SIZE]);
        final ByteBuffer buffer = ByteBuffer.allocate(8);

        final Map<String, IValue> putEntries = atomicChange.getPutEntries();
        final Map<String, IValue> removedEntries = atomicChange.getRemovedEntries();
        final StringBuilder sb =
            new StringBuilder(30 * (putEntries.size() + removedEntries.size() + atomicChange.getSubMapKeys().size()));
        sb.append(preamble);
        final boolean isImage = atomicChange.getScope() == IRecordChange.IMAGE_SCOPE_CHAR;
        final String name = atomicChange.getName();
        if (name.startsWith(RpcInstance.RPC_RECORD_RESULT_PREFIX, 0))
        {
            sb.append(name);
        }
        else
        {
            appendSymbolForRecordName(name, sb, chars, escapedChars, isImage);
        }
        // add the sequence
        sb.append(DELIMITER).append(atomicChange.getScope()).append(atomicChange.getSequence());
        addEntriesWithSymbols(DELIMITER_PUT_CODE, putEntries, sb, chars, escapedChars, buffer, isImage, name);
        addEntriesWithSymbols(DELIMITER_REMOVE_CODE, removedEntries, sb, chars, escapedChars, buffer, isImage, name);
        IRecordChange subMapAtomicChange;
        for (String subMapKey : atomicChange.getSubMapKeys())
        {
            subMapAtomicChange = atomicChange.getSubMapAtomicChange(subMapKey);
            sb.append(DELIMITER_SUBMAP_CODE);
            appendSymbolForKey(subMapKey, sb, chars, escapedChars, isImage, name);
            addEntriesWithSymbols(DELIMITER_PUT_CODE, subMapAtomicChange.getPutEntries(), sb, chars, escapedChars,
                buffer, isImage, name);
            addEntriesWithSymbols(DELIMITER_REMOVE_CODE, subMapAtomicChange.getRemovedEntries(), sb, chars,
                escapedChars, buffer, isImage, name);
        }
        return sb.toString().getBytes(getCharset());
    }

    private void addEntriesWithSymbols(String changeType, Map<String, IValue> entries, StringBuilder txString,
        AtomicReference<char[]> chars, AtomicReference<char[]> escapedChars, ByteBuffer buffer, boolean isImage,
        String recordName)
    {
        Map.Entry<String, IValue> entry;
        String key;
        IValue value;
        if (entries != null && entries.size() > 0)
        {
            txString.append(changeType);
            for (Iterator<Map.Entry<String, IValue>> it = entries.entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                key = entry.getKey();
                value = entry.getValue();
                txString.append(DELIMITER);
                if (key == null)
                {
                    txString.append(KEY_PREAMBLE);
                }
                else
                {
                    appendSymbolForKey(key, txString, chars, escapedChars, isImage, recordName);
                }

                txString.append(KEY_VALUE_DELIMITER);
                if (value == null)
                {
                    txString.append(NULL_VALUE);
                }
                else
                {
                    switch(value.getType())
                    {
                        case DOUBLE:
                            txString.append(value.getType().getCharCode());
                            // todo want to NOT create a string
                            escape(new String(AbstractValue.toBytes(value, buffer), getCharset()), txString, chars,
                                escapedChars);
                            buffer.clear();
                            break;
                        case LONG:
                            txString.append(value.getType().getCharCode());
                            final byte[] bytes = AbstractValue.toBytes(value, buffer);
                            final int bytesToWrite = getByteCountForLong(value.longValue());
                            // todo want to NOT create a string
                            escape(new String(bytes, 8 - bytesToWrite, bytesToWrite, getCharset()), txString, chars,
                                escapedChars);
                            buffer.clear();
                            break;
                        case BLOB:
                            // note: blob string is "B<hex string for bytes>", e.g. B7366abc4
                            // so no chance of any special chars!
                            txString.append(value.toString());
                            break;
                        case TEXT:
                        default :
                            escape(value.toString(), txString, chars, escapedChars);
                            break;
                    }
                }
            }
        }
    }

    /**
     * Performs unescaping and decoding of a value
     */
    static IValue decodeValue(char[] chars, int start, int end, char[] unescaped, ByteBuffer buffer, byte[] array)
    {
        final int unescapedPtr = doUnescape(chars, start, end, unescaped);

        if (NULL_VALUE_CHARS.length == unescapedPtr)
        {
            boolean isNull = true;
            for (int i = 0; i < unescapedPtr; i++)
            {
                if (NULL_VALUE_CHARS[i] != unescaped[i])
                {
                    isNull = false;
                    break;
                }
            }
            if (isNull)
            {
                return AbstractValue.constructFromCharValue(null, 0);
            }
        }

        int i = 0;
        int j = 0;
        buffer.clear();
        switch(unescaped[0])
        {
            case IValue.LONG_CODE:
                // read in from "compact integral" format
                // ...clear the array first
                for (i = 0; i < array.length; i++)
                {
                    array[i] = 0x0;
                }
                j = 7;
                // start from the end work to the front
                for (i = unescapedPtr - 1; i > 0; i--)
                {
                    array[j--] = (byte) (unescaped[i]);
                }
                return LongValue.valueOf(buffer.getLong());
            case IValue.DOUBLE_CODE:
                // start at 1 to skip the value type code (L/D etc)
                for (i = 1; i < unescapedPtr; i++)
                {
                    try
                    {
                        array[j++] = (byte) (unescaped[i]);
                    }
                    catch (ArrayIndexOutOfBoundsException e)
                    {
                        // todo
                        System.err.println("unescapedPtr=" + unescapedPtr + ", unescaped=" + Arrays.toString(unescaped));
                        throw e;
                    }
                }
                return new DoubleValue(Double.longBitsToDouble(buffer.getLong()));
            default :
                return AbstractValue.constructFromCharValue(unescaped, unescapedPtr);
        }
    }

    /**
     * Appends the name-symbol portion for a record:
     * 
     * <pre>
     *  name-symbol = ( symbol-definition | symbol-instance )
     *  symbol-definition = "n" name symbol-instance ; if the symbol for the record has never been sent
     *  symbol-instance = "~" symbol-name
     *  name = 1*ALPHA ; the name of the notifying record instance
     *  symbol-name = 1*ALPHA ; the symbol for the notifying record instance name
     * </pre>
     */
    void appendSymbolForRecordName(String name, StringBuilder sb, AtomicReference<char[]> chars,
        AtomicReference<char[]> escapedChars, boolean isImage)
    {
        String symbol = this.txRecordNameToSymbol.get(name);
        if (isImage || symbol == null)
        {
            synchronized (this.txRecordNameToSymbol)
            {
                symbol = this.txRecordNameToSymbol.get(name);
                if (symbol == null)
                {
                    // check the 'master' mappings
                    symbol = TX_RECORD_NAME_TO_SYMBOL.get(name);
                    if (symbol == null)
                    {
                        synchronized (TX_RECORD_NAME_TO_SYMBOL)
                        {
                            symbol = TX_RECORD_NAME_TO_SYMBOL.get(name);
                            if (symbol == null)
                            {
                                symbol = getNextTxRecordNameSymbol();
                                if (log)
                                {
                                    Log.log(this, "[MASTER TX] record-symbol ", name, "=", symbol);
                                }
                                TX_RECORD_NAME_TO_SYMBOL.put(name, symbol);
                            }
                        }
                        if (log)
                        {
                            Log.log(this, "[tx] record-symbol ", name, "=", symbol);
                        }
                        this.txRecordNameToSymbol.put(name, symbol);
                    }
                }
                sb.append('n');
                escape(name, sb, chars, escapedChars);
            }
        }
        sb.append('~').append(symbol);
    }

    /**
     * Appends the key-symbol portion for a record:
     * 
     * <pre>
     *  name-symbol = ( symbol-definition | symbol-instance )
     *  symbol-definition = "|n" name symbol-instance ; if the symbol for the record has never been sent
     *  symbol-instance = "|~" symbol-name
     *  name = 1*ALPHA ; the name of the notifying record instance
     *  symbol-name = 1*ALPHA ; the symbol for the notifying record instance name
     * </pre>
     */
    void appendSymbolForKey(String key, StringBuilder txString, AtomicReference<char[]> chars,
        AtomicReference<char[]> escapedChars, boolean isImage, String recordName)
    {
        String symbol = this.txKeyNameToSymbol.get(key);
        if (isImage || symbol == null || keyDefinitionNeedsToBeSent(key, recordName))
        {
            synchronized (this.txKeyNameToSymbol)
            {
                symbol = this.txKeyNameToSymbol.get(key);
                if (symbol == null)
                {
                    // check the 'master' mappings
                    symbol = TX_KEY_NAME_TO_SYMBOL.get(key);
                    if (symbol == null)
                    {
                        synchronized (TX_KEY_NAME_TO_SYMBOL)
                        {
                            symbol = TX_KEY_NAME_TO_SYMBOL.get(key);
                            if (symbol == null)
                            {
                                symbol = getNextTxKeyNameSymbol();
                                if (log)
                                {
                                    Log.log(this, "[MASTER TX] key-symbol ", key, "=", symbol);
                                }
                                TX_KEY_NAME_TO_SYMBOL.put(key, symbol);
                            }
                        }
                    }
                    if (log)
                    {
                        Log.log(this, "[tx] key-symbol ", key, "=", symbol);
                    }
                    this.txKeyNameToSymbol.put(key, symbol);
                }
            }
            txString.append("n");
            escape(key, txString, chars, escapedChars);
        }
        txString.append("~").append(symbol);
    }

    /**
     * @return if the key definition is needed for the record
     */
    private boolean keyDefinitionNeedsToBeSent(String key, String recordName)
    {
        if (recordName.startsWith(RpcInstance.RPC_RECORD_RESULT_PREFIX, 0))
        {
            return true;
        }

        synchronized (this.keyDefinitionsSent)
        {
            Set<String> records = this.keyDefinitionsSent.get(key);
            if (records == null)
            {
                records = new HashSet<String>();
                this.keyDefinitionsSent.put(key, records);
            }
            return records.add(recordName);
        }
    }

    /**
     * Performs unescaping and decoding of a key from its symbol
     */
    String decodeKeyFromSymbol(char[] chars, int start, int end)
    {
        if (end - start == KEY_PREAMBLE_CHARS.length)
        {
            boolean isNull = true;
            for (int i = start; i < end; i++)
            {
                if (chars[i] != KEY_PREAMBLE_CHARS[i - start])
                {
                    isNull = false;
                    break;
                }
            }
            if (isNull)
            {
                return null;
            }
        }
        String key = null;
        switch(chars[0])
        {
            case '~':
                // if its a symbol, no need to unencode
                // symbol-instance ~<symbol>
                key = lookupKeySymbol(new String(chars, start + 1, end - 1));
                break;
            case 'n':
                // symbol-definition: n<name>~s<symbol>
                key = defineKeySymbol(stringFromCharBuffer(chars, start, end));
                break;
            default :
                throw new IllegalArgumentException("Could not decode key from symbol '"
                    + stringFromCharBuffer(chars, start, end) + "'");
        }
        return key;
    }

    static String stringFromCharBuffer(char[] chars, int start)
    {
        final char[] unescaped = new char[chars.length - start];
        final int unescapedPtr = doUnescape(chars, start, chars.length, unescaped);
        // note: this does an array copy when constructing the string
        return new String(unescaped, 0, unescapedPtr);
    }

    static String stringFromCharBuffer(char[] chars, int start, int end)
    {
        final char[] unescaped = new char[end - start];
        final int unescapedPtr = doUnescape(chars, start, end, unescaped);
        // note: this does an array copy when constructing the string
        return new String(unescaped, 0, unescapedPtr);
    }

    @Override
    public ICodec<char[]> newInstance()
    {
        return new StringSymbolProtocolCodec();
    }

    @Override
    public Charset getCharset()
    {
        return ISO_8859_1;
    }
}