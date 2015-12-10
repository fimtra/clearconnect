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
 * record name or key is sent. Typically a symbol will be between 1-4 chars (but is unlimited).
 * 
 * @author Ramon Servadei
 */
public final class StringSymbolProtocolCodec extends StringProtocolCodec
{
    // private static final char START_SYMBOL = (char) 1;
    // private static final char END_SYMBOL = (char) 254;
    private static final char START_SYMBOL = ' ';
    private static final char END_SYMBOL = 'z';
    // todo memory leak in these...
    // todo concurrency...
    /**
     * Stores the symbol to key name
     */
    final ConcurrentMap<String, String> symbolToKey = new ConcurrentHashMap<String, String>();
    /**
     * Stores the key name to symbol
     */
    final ConcurrentMap<String, String> keyNameToSymbol = new ConcurrentHashMap<String, String>();
    /**
     * Stores the symbol to record name
     */
    final ConcurrentMap<String, String> symbolToRecordName = new ConcurrentHashMap<String, String>();
    /**
     * Stores the record name to symbol
     */
    final ConcurrentMap<String, String> recordNameToSymbol = new ConcurrentHashMap<String, String>();

    static char[] nextNameSymbol = new char[] { START_SYMBOL };
    static char[] nextKeySymbol = new char[] { START_SYMBOL };

    /**
     * Get the string representing the record changes to transmit to a {@link ProxyContext}. The
     * format of the string in ABNF notation:
     * 
     * <pre>
     *  preamble name-symbol seq [puts] [removes] [sub-map]
     *  
     *  preamble = 0*ALPHA
     *  name-symbol = "|" ( name-symbol-definition | name-symbol-instance )
     *  name-symbol-definition = "n" name "~" name-symbol-instance ; if the symbol for the record has never been sent
     *  name-symbol-instance = "s" symbol-name
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
     *  key-symbol-definition =  "n" key "~" key-symbol-instance
     *  key-symbol-instance = "s" symbol-name
     *  key = 1*ALPHA ; the symbol for the key
     *  value = 1*ALPHA
     *  
     *  e.g. |nmyrecord~s!|d322234|p|nkey1~s#=value1|sdf=value2|r|s$=value5|:|s$|p|sty=value1
     * </pre>
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
                case 's':
                    // if its just the symbol, no need to unencode
                    // symbol-instance s<symbol>
                    name = lookupRecordNameSymbol(new String(tokens[1], 0, tokens[1].length));
                    break;
                case 'n':
                    // symbol-definition: n<name>~s<symbol>
                    name = defineRecordNameSymbol(stringFromCharBuffer(tokens[1], 0));
                    break;
                default :
                    throw new IllegalArgumentException("Could not handle " + new String(decodedMessage));
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
                                    case 's':
                                        // if its a symbol, no need to unescape
                                        // symbol-instance s<symbol>
                                        subMapName = lookupRecordNameSymbol(new String(tokens[i], 0, tokens[i].length));
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
        final String name = this.symbolToRecordName.get(symbol);
        if (name == null)
        {
            throw new IllegalArgumentException("No record name for symbol '" + symbol + "'");
        }
        return name;
    }

    private String lookupKeySymbol(String symbol)
    {
        final String name = this.symbolToKey.get(symbol);
        if (name == null)
        {
            throw new IllegalArgumentException("No key for symbol '" + symbol + "', symbols:" + symbolToKey);
        }
        return name;
    }

    /**
     * Given a symbol-definition format "n{name}~s{symbol}" this populates the {@link #symbolToKey}
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
                // todo BUG: symbols are stored as s<symbol>
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
        this.symbolToKey.put(sb.toString(), name);
        return name;
    }

    /**
     * Given a symbol-definition format "n{name}~s{symbol}" this populates the
     * {@link #symbolToRecordName} map with the mapping.
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
        this.symbolToRecordName.put(sb.toString(), name);
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
        appendSymbolForRecordName(atomicChange.getName(), sb, isImage);
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
     *  symbol-definition = "n" name "~" symbol-instance ; if the symbol for the record has never been sent
     *  symbol-instance = "s" symbol-name
     *  name = 1*ALPHA ; the name of the notifying record instance
     *  symbol-name = 1*ALPHA ; the symbol for the notifying record instance name
     * </pre>
     */
    void appendSymbolForRecordName(String name, StringBuilder sb, boolean isImage)
    {
        String symbol = this.recordNameToSymbol.get(name);
        if (isImage || symbol == null)
        {
            synchronized (this.recordNameToSymbol)
            {
                symbol = this.recordNameToSymbol.get(name);
                if (symbol == null)
                {
                    // todo must not clash with symbolToREcord for inbound
                    symbol = getNextRecordNameSymbol();
                    this.recordNameToSymbol.put(name, symbol);
                }
            }
            sb.append("n");
            escape(name, sb);
            sb.append("~");
        }
        sb.append("s").append(symbol);
    }

    private synchronized String getNextRecordNameSymbol()
    {
        // NOTE: there are "reserved" chars:
        // \r
        // \r
        // |
        // =
        // ~
        // \

        findNextValidSymbol(this.nextNameSymbol);

        if (this.nextNameSymbol[this.nextNameSymbol.length - 1] >= END_SYMBOL)
        {
            // resize
            char[] temp = new char[this.nextNameSymbol.length + 1];
            System.arraycopy(this.nextNameSymbol, 0, temp, 0, this.nextNameSymbol.length);
            this.nextNameSymbol = temp;
            this.nextNameSymbol[this.nextNameSymbol.length - 1] = START_SYMBOL;
        }
        return new String(this.nextNameSymbol, 0, this.nextNameSymbol.length);
    }

    private synchronized String getNextKeyNameSymbol()
    {
        // NOTE: there are "reserved" chars:
        // \r
        // \r
        // |
        // =
        // ~
        // \

        findNextValidSymbol(this.nextKeySymbol);

        if (this.nextKeySymbol[this.nextKeySymbol.length - 1] >= END_SYMBOL)
        {
            // resize
            char[] temp = new char[this.nextKeySymbol.length + 1];
            System.arraycopy(this.nextKeySymbol, 0, temp, 0, this.nextKeySymbol.length);
            this.nextKeySymbol = temp;
            this.nextKeySymbol[this.nextKeySymbol.length - 1] = START_SYMBOL;
        }
        return new String(this.nextKeySymbol, 0, this.nextKeySymbol.length);
    }

    /**
     * Populates the char[] with the next symbol, skipping any special chars (see implementation)
     */
    static void findNextValidSymbol(final char[] symbolArr)
    {
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
            case 's':
                // if its a symbol, no need to unencode
                // symbol-instance s<symbol>
                key = lookupKeySymbol(new String(chars, start, end));
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
     *  symbol-instance = "|s" symbol-name
     *  name = 1*ALPHA ; the name of the notifying record instance
     *  symbol-name = 1*ALPHA ; the symbol for the notifying record instance name
     * </pre>
     */
    void appendSymbolForKey(StringBuilder txString, String key, boolean isImage)
    {

        String symbol = this.keyNameToSymbol.get(key);
        if (isImage || symbol == null)
        {
            synchronized (this.keyNameToSymbol)
            {
                symbol = this.keyNameToSymbol.get(key);
                if (symbol == null)
                {
                    symbol = getNextKeyNameSymbol();
                    this.keyNameToSymbol.put(key, symbol);
                }
            }
            txString.append("n");
            escape(key, txString);
            txString.append("~");
        }
        txString.append("s").append(symbol);
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