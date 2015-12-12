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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
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
    /**
     * Controls logging of:
     * <ul>
     * <li>key-symbol tx and rx mappings
     * <li>record-symbol tx and rx mappings
     * </ul>
     */
    public static boolean log = Boolean.getBoolean("log." + StringSymbolProtocolCodec.class.getCanonicalName());

    private static final char START_SYMBOL = Character.MIN_VALUE;
    private static final char END_SYMBOL = Character.MAX_VALUE;
    // todo memory leak in these...
    // todo concurrency...

    /**
     * Stores the mappings of all key-name-to-symbol sent from this VM
     */
    static final ConcurrentMap<String, String> TX_KEY_NAME_TO_SYMBOL = new ConcurrentHashMap<String, String>();
    /**
     * Stores the mappings of all record-name-to-symbol sent from this VM
     */
    static final ConcurrentMap<String, String> TX_RECORD_NAME_TO_SYMBOL = new ConcurrentHashMap<String, String>();

    static char[] NEXT_TX_RECORD_NAME_SYMBOL = new char[] { START_SYMBOL };
    static char[] NEXT_TX_KEY_NAME_SYMBOL = new char[] { START_SYMBOL };

    private static synchronized String getNextTxKeyNameSymbol()
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

    private static synchronized String getNextTxRecordNameSymbol()
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
    private static void findNextValidSymbol(final char[] symbolArr)
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
        try
        {
            char[] tempArr = new char[50];
            final char[][] tokens = findTokens(decodedMessage);
            final String name;
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
                                                        tempArr), null);
                                            }
                                            else
                                            {
                                                atomicChange.mergeSubMapEntryRemovedChange(
                                                    subMapName,
                                                    decodeKeyFromSymbol(currentTokenChars, 0, j),
                                                    decodeValue(currentTokenChars, j + 1, currentTokenChars.length,
                                                        tempArr));
                                            }
                                        }
                                        else
                                        {
                                            if (put)
                                            {
                                                atomicChange.mergeEntryUpdatedChange(
                                                    decodeKeyFromSymbol(currentTokenChars, 0, j),
                                                    decodeValue(currentTokenChars, j + 1, currentTokenChars.length,
                                                        tempArr), null);
                                            }
                                            else
                                            {
                                                atomicChange.mergeEntryRemovedChange(
                                                    decodeKeyFromSymbol(currentTokenChars, 0, j),
                                                    decodeValue(currentTokenChars, j + 1, currentTokenChars.length,
                                                        tempArr));
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
            throw new RuntimeException("Could not decode '" + new String(decodedMessage) + "'", e);
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
            appendSymbolForRecordName(name, sb, isImage);
        }
        // add the sequence
        sb.append(DELIMITER).append(atomicChange.getScope()).append(atomicChange.getSequence());
        addEntriesWithSymbols(DELIMITER_PUT_CODE, putEntries, sb, isImage);
        addEntriesWithSymbols(DELIMITER_REMOVE_CODE, removedEntries, sb, isImage);
        IRecordChange subMapAtomicChange;
        for (String subMapKey : atomicChange.getSubMapKeys())
        {
            subMapAtomicChange = atomicChange.getSubMapAtomicChange(subMapKey);
            sb.append(DELIMITER_SUBMAP_CODE);
            appendSymbolForRecordName(subMapKey, sb, isImage);
            addEntriesWithSymbols(DELIMITER_PUT_CODE, subMapAtomicChange.getPutEntries(), sb, isImage);
            addEntriesWithSymbols(DELIMITER_REMOVE_CODE, subMapAtomicChange.getRemovedEntries(), sb, isImage);
        }
        return sb.toString().getBytes(UTF8);
    }

    private void addEntriesWithSymbols(String changeType, Map<String, IValue> entries, StringBuilder txString,
        boolean isImage)
    {
        Map.Entry<String, IValue> entry;
        String key;
        IValue value;
        String valueAsString;
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
                    appendSymbolForKey(txString, key, isImage);
                }

                txString.append(KEY_VALUE_DELIMITER);
                if (value == null)
                {
                    txString.append(NULL_VALUE);
                }
                else
                {
                    valueAsString = value.toString();
                    switch(value.getType())
                    {
                        case DOUBLE:
                        case LONG:
                        case BLOB:
                            // longs, doubles and blobs do not need escaping
                            // note: blob string is "B<hex string for bytes>", e.g. B7366abc4
                            txString.append(valueAsString);
                            break;
                        case TEXT:
                        default :
                            escape(valueAsString, txString);
                            break;
                    }
                }
            }
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
    void appendSymbolForRecordName(String name, StringBuilder sb, boolean isImage)
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
                    }
                    if (log)
                    {
                        Log.log(this, "[tx] record-symbol ", name, "=", symbol);
                    }
                    this.txRecordNameToSymbol.put(name, symbol);
                }
            }
            sb.append("n");
            escape(name, sb);
        }
        sb.append("~").append(symbol);
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
    void appendSymbolForKey(StringBuilder txString, String key, boolean isImage)
    {

        String symbol = this.txKeyNameToSymbol.get(key);
        if (isImage || symbol == null)
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
            escape(key, txString);
        }
        txString.append("~").append(symbol);
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
}