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
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.RpcInstance.Remote;
import com.fimtra.datafission.field.AbstractValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.CharBufferUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;

/**
 * A codec for messages that are sent between a {@link Publisher} and {@link ProxyContext} using a
 * string text protocol. The format of the string in ABNF notation:
 * 
 * <pre>
 *  preamble name seq [puts] [removes] [sub-map]
 *  
 *  preamble = 0*ALPHA
 *  name = "|" 1*ALPHA ; the name of the notifying record instance
 *  seq = "|" scope seq_num
 *  scope = "i" | "d" ; identifies either an image or delta
 *  seq_num = 1*DIGIT ; the sequency number
 *  puts = "|p" 1*key-value-pair
 *  removes = "|r" 1*key-value-pair
 *  sub-map = "|:|" name [puts] [removes]
 *  key-value-pair = "|" key "=" value
 *  key = 1*ALPHA
 *  value = 1*ALPHA
 *  
 *  e.g. |record_name|d322234|p|key1=value1|key2=value2|r|key_5=value5|:|subMap1|p|key1=value1
 * </pre>
 * 
 * @author Ramon Servadei
 */
public class StringProtocolCodec implements ICodec<char[]>
{
    public static final int CHARRAY_SIZE = 32;

    static final byte[] SYNC = "sync".getBytes();
    
    static final char CHAR_TOKEN_DELIM = '|';
    static final char CHAR_KEY_VALUE_SEPARATOR = '=';
    static final char CHAR_ESCAPE = '\\';
    static final char CHAR_SYMBOL_PREFIX = '~';
    static final char CHAR_n = 'n';
    static final char CHAR_r = 'r';

    // these are special chars used by TcpChannel TerminatorBasedReaderWriter.TERMINATOR so need
    // escaping
    static final char CR = '\r';
    static final char LF = '\n';

    static final Charset UTF8 = Charset.forName("UTF-8");

    static final char DELIMITER = '|';
    static final char[] DELIMITER_CHARS = new char[] { DELIMITER };
    static final char PUT_CODE = 'p';
    static final char REMOVE_CODE = 'r';
    static final char SUBMAP_CODE = ':';
    static final char[] DELIMITER_REMOVE_CODE = new char[] { DELIMITER, REMOVE_CODE };
    static final char[] DELIMITER_PUT_CODE = new char[] { DELIMITER, PUT_CODE };
    static final char[] DELIMITER_SUBMAP_CODE = new char[] { DELIMITER, SUBMAP_CODE, DELIMITER };

    static final String RPC_COMMAND = "rpc" + DELIMITER;
    static final char[] RPC_COMMAND_CHARS = RPC_COMMAND.toCharArray();
    static final String RESYNC_COMMAND = "r" + DELIMITER;
    static final char[] RESYNC_COMMAND_CHARS = RESYNC_COMMAND.toCharArray();
    static final String SUBSCRIBE_COMMAND = "s" + DELIMITER;
    static final char[] SUBSCRIBE_COMMAND_CHARS = SUBSCRIBE_COMMAND.toCharArray();
    static final String UNSUBSCRIBE_COMMAND = "u" + DELIMITER;
    static final char[] UNSUBSCRIBE_COMMAND_CHARS = UNSUBSCRIBE_COMMAND.toCharArray();
    static final String IDENTIFY_COMMAND = "i" + DELIMITER;
    static final char[] IDENTIFY_COMMAND_CHARS = IDENTIFY_COMMAND.toCharArray();

    /**
     * This is the ASCII code for STX (0x2). This allows cut-n-paste of text using editors as using
     * the ASCII code for NULL=0x0 causes problems.
     */
    // todo use chars
    static final String NULL_VALUE = new String(new char[] { 0x2 });
    static final char[] NULL_VALUE_CHARS = NULL_VALUE.toCharArray();
    static final String KEY_PREAMBLE = NULL_VALUE;
    static final char[] KEY_PREAMBLE_CHARS = KEY_PREAMBLE.toCharArray();
    static final String DOUBLE_KEY_PREAMBLE = KEY_PREAMBLE + KEY_PREAMBLE;
    static final int DOUBLE_KEY_PREAMBLE_LENGTH = DOUBLE_KEY_PREAMBLE.length();

    boolean synced = false;

    @Override
    public CommandEnum getCommand(char[] decodedMessage)
    {
        // todo optimise this to just scan and process once...
        
        // commands from a client:
        // show = show all record names
        // s|<name>|<name>|... = subscribe for records
        // u|<name>|<name>|... = unsubscribe for records
        // rpc|RPC_details_as_atomic_change = rpc call
        //
        if (isCommand(decodedMessage, SUBSCRIBE_COMMAND_CHARS))
        {
            return CommandEnum.SUBSCRIBE;
        }
        if (isCommand(decodedMessage, UNSUBSCRIBE_COMMAND_CHARS))
        {
            return CommandEnum.UNSUBSCRIBE;
        }
        if (isCommand(decodedMessage, RESYNC_COMMAND_CHARS))
        {
            return CommandEnum.RESYNC;
        }
        if (isCommand(decodedMessage, RPC_COMMAND_CHARS))
        {
            return CommandEnum.RPC;
        }
        if (isCommand(decodedMessage, IDENTIFY_COMMAND_CHARS))
        {
            return CommandEnum.IDENTIFY;
        }
        throw new IllegalArgumentException("Could not interpret command '" + new String(decodedMessage) + "'");
    }

    /**
     * @param message
     * @param commandChars
     * @return
     */
    private static boolean isCommand(char[] message, char[] commandChars)
    {
        if (message.length < commandChars.length)
        {
            return false;
        }
        for (int i = 0; i < commandChars.length; i++)
        {
            if (message[i] != commandChars[i])
            {
                return false;
            }
        }
        return true;
    }

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
        return encodeAtomicChange(DELIMITER_CHARS, atomicChange, getCharset());
    }

    @Override
    public byte[] getTxMessageForSubscribe(String... names)
    {
        return (getEncodedNamesForCommandMessage(SUBSCRIBE_COMMAND, names)).getBytes(getCharset());
    }

    @Override
    public byte[] getTxMessageForUnsubscribe(String... names)
    {
        return (getEncodedNamesForCommandMessage(UNSUBSCRIBE_COMMAND, names)).getBytes(getCharset());
    }

    @Override
    public byte[] getTxMessageForIdentify(String proxyIdentity)
    {
        return (getEncodedNamesForCommandMessage(IDENTIFY_COMMAND, proxyIdentity)).getBytes(getCharset());
    }

    @Override
    public byte[] getTxMessageForResync(String... names)
    {
        return (getEncodedNamesForCommandMessage(RESYNC_COMMAND, names)).getBytes(getCharset());
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
        return decodeAtomicChange(decode(data));
    }

    static IRecordChange decodeAtomicChange(char[] decodedMessage)
    {
        try
        {
            char[] tempArr = new char[50];
            final char[][] tokens = findTokens(decodedMessage);
            final String name = stringFromCharBuffer(tokens[1]);
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
                char[] chars;
                char previous;
                char[] currentTokenChars;
                int j;
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
                                subMapName = stringFromCharBuffer(tokens[++i]);
                                break;
                            default :
                                break;
                        }
                    }
                    else
                    {
                        chars = tokens[i];
                        previous = 0;
                        for (j = 0; j < chars.length; j++)
                        {
                            switch(chars[j])
                            {
                                case CHAR_KEY_VALUE_SEPARATOR:
                                    // find where the first non-escaped "=" is
                                    if (previous != CHAR_ESCAPE)
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
                                                    decodeKey(currentTokenChars, 0, j, true, tempArr),
                                                    decodeValue(currentTokenChars, j + 1, currentTokenChars.length,
                                                        tempArr), null);
                                            }
                                            else
                                            {
                                                atomicChange.mergeSubMapEntryRemovedChange(
                                                    subMapName,
                                                    decodeKey(currentTokenChars, 0, j, true, tempArr),
                                                    decodeValue(currentTokenChars, j + 1, currentTokenChars.length,
                                                        tempArr));
                                            }
                                        }
                                        else
                                        {
                                            if (put)
                                            {
                                                atomicChange.mergeEntryUpdatedChange(
                                                    decodeKey(currentTokenChars, 0, j, true, tempArr),
                                                    decodeValue(currentTokenChars, j + 1, currentTokenChars.length,
                                                        tempArr), null);
                                            }
                                            else
                                            {
                                                atomicChange.mergeEntryRemovedChange(
                                                    decodeKey(currentTokenChars, 0, j, true, tempArr),
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

    static byte[] encodeAtomicChange(char[] preamble, IRecordChange atomicChange, Charset charSet)
    {
        final CharArrayReference chars = new CharArrayReference(new char[CHARRAY_SIZE]);

        final Map<String, IValue> putEntries = atomicChange.getPutEntries();
        final Map<String, IValue> removedEntries = atomicChange.getRemovedEntries();
        final Set<String> subMapKeys = atomicChange.getSubMapKeys();
        final StringBuilder sb =
            new StringBuilder(30 * (putEntries.size() + removedEntries.size() + subMapKeys.size()));
        sb.append(preamble);
        escape(atomicChange.getName(), sb, chars);
        // add the sequence
        sb.append(DELIMITER).append(atomicChange.getScope()).append(atomicChange.getSequence());
        addEntriesToTxString(DELIMITER_PUT_CODE, putEntries, sb, chars);
        addEntriesToTxString(DELIMITER_REMOVE_CODE, removedEntries, sb, chars);
        IRecordChange subMapAtomicChange;
        if (subMapKeys.size() > 0)
        {
            for (String subMapKey : subMapKeys)
            {
                subMapAtomicChange = atomicChange.getSubMapAtomicChange(subMapKey);
                sb.append(DELIMITER_SUBMAP_CODE);
                escape(subMapKey, sb, chars);
                addEntriesToTxString(DELIMITER_PUT_CODE, subMapAtomicChange.getPutEntries(), sb, chars);
                addEntriesToTxString(DELIMITER_REMOVE_CODE, subMapAtomicChange.getRemovedEntries(), sb, chars);
            }
        }
        return sb.toString().getBytes(charSet);
    }

    private static void addEntriesToTxString(char[] changeType, Map<String, IValue> entries, StringBuilder txString,
        CharArrayReference chars)
    {
        Map.Entry<String, IValue> entry;
        String key;
        IValue value;
        int i = 0;
        int last;
        int length;
        char charAt;
        char[] cbuf;
        char[] escapedChars = new char[2];
        boolean needToEscape;
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
                    txString.append(KEY_PREAMBLE_CHARS);
                }
                else
                {
                    cbuf = new char[key.length() + DOUBLE_KEY_PREAMBLE_LENGTH];
                    DOUBLE_KEY_PREAMBLE.getChars(0, DOUBLE_KEY_PREAMBLE_LENGTH, cbuf, 0);
                    key.getChars(0, key.length(), cbuf, DOUBLE_KEY_PREAMBLE_LENGTH);

                    // NOTE: for efficiency, we have *almost* inlined versions of the same escape
                    // switch statements
                    needToEscape = false;
                    length = cbuf.length;
                    for (i = 0; i < length; i++)
                    {
                        charAt = cbuf[i];
                        switch(charAt)
                        {
                            case CR:
                            case LF:
                            case CHAR_ESCAPE:
                            case CHAR_TOKEN_DELIM:
                            case CHAR_KEY_VALUE_SEPARATOR:
                                needToEscape = true;
                                i = length;
                        }
                    }
                    if (needToEscape)
                    {
                        last = 0;
                        for (i = 0; i < length; i++)
                        {
                            charAt = cbuf[i];
                            switch(charAt)
                            {
                                case CR:
                                    txString.append(cbuf, last, i - last);
                                    escapedChars[0] = CHAR_ESCAPE;
                                    escapedChars[1] = CHAR_r;
                                    txString.append(escapedChars, 0, 2);
                                    last = i + 1;
                                    break;
                                case LF:
                                    txString.append(cbuf, last, i - last);
                                    escapedChars[0] = CHAR_ESCAPE;
                                    escapedChars[1] = CHAR_n;
                                    txString.append(escapedChars, 0, 2);
                                    last = i + 1;
                                    break;
                                case CHAR_ESCAPE:
                                case CHAR_TOKEN_DELIM:
                                case CHAR_KEY_VALUE_SEPARATOR:
                                    txString.append(cbuf, last, i - last);
                                    escapedChars[0] = CHAR_ESCAPE;
                                    escapedChars[1] = charAt;
                                    txString.append(escapedChars, 0, 2);
                                    last = i + 1;
                                    break;
                                default :
                            }
                        }
                        txString.append(cbuf, last, length - last);
                    }
                    else
                    {
                        txString.append(cbuf);
                    }
                }

                txString.append(CHAR_KEY_VALUE_SEPARATOR);
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
                            escape(valueAsString, txString, chars);
                            break;
                    }
                }
            }
        }
    }

    /**
     * Escape special chars in the value-to-send, ultimately adding the escaped value into the
     * destination StringBuilder
     */
    static void escape(String valueToSend, StringBuilder dest, CharArrayReference charsRef)
    {
        try
        {
            final int length = valueToSend.length();
            if (charsRef.ref.length < length)
            {
                // resize
                charsRef.ref = (new char[length]);
            }

            final char[] chars = charsRef.ref;
            final char[] escapedChars = new char[2];

            valueToSend.getChars(0, valueToSend.length(), chars, 0);

            char charAt;
            int last = 0;
            for (int i = 0; i < length; i++)
            {
                charAt = chars[i];
                switch(charAt)
                {
                    case CR:
                        dest.append(chars, last, i - last);
                        escapedChars[0] = CHAR_ESCAPE;
                        escapedChars[1] = CHAR_r;
                        dest.append(escapedChars, 0, 2);
                        last = i + 1;
                        break;
                    case LF:
                        dest.append(chars, last, i - last);
                        escapedChars[0] = CHAR_ESCAPE;
                        escapedChars[1] = CHAR_n;
                        dest.append(escapedChars, 0, 2);
                        last = i + 1;
                        break;
                    case CHAR_ESCAPE:
                    case CHAR_TOKEN_DELIM:
                    case CHAR_KEY_VALUE_SEPARATOR:
                    case CHAR_SYMBOL_PREFIX:
                        dest.append(chars, last, i - last);
                        escapedChars[0] = CHAR_ESCAPE;
                        escapedChars[1] = charAt;
                        dest.append(escapedChars, 0, 2);
                        last = i + 1;
                        break;
                    default :
                }
            }
            dest.append(chars, last, length - last);
        }
        catch (Exception e)
        {
            Log.log(StringProtocolCodec.class, "Could not append for " + ObjectUtils.safeToString(valueToSend), e);
        }
    }

    /**
     * Parse the chars and performs unescaping copying into the destination char[]
     * <p>
     * This assumes the destination has sufficient space to accept the chars between start and end.
     * 
     * @return the index in the destination where the unescaped sequence ends
     */
    static int doUnescape(char[] chars, int start, int end, final char[] dest)
    {
        int unescapedPtr = 0;
        for (int i = start; i < end; i++)
        {
            switch(chars[i])
            {
                case CHAR_ESCAPE:
                    i++;
                    if (i < chars.length)
                    {
                        switch(chars[i])
                        {
                            case CHAR_r:
                                dest[unescapedPtr++] = CR;
                                break;
                            case CHAR_n:
                                dest[unescapedPtr++] = LF;
                                break;
                            case CHAR_ESCAPE:
                                dest[unescapedPtr++] = CHAR_ESCAPE;
                                break;
                            case CHAR_TOKEN_DELIM:
                                dest[unescapedPtr++] = CHAR_TOKEN_DELIM;
                                break;
                            case CHAR_KEY_VALUE_SEPARATOR:
                                dest[unescapedPtr++] = CHAR_KEY_VALUE_SEPARATOR;
                                break;
                            case CHAR_SYMBOL_PREFIX:
                                dest[unescapedPtr++] = CHAR_SYMBOL_PREFIX;
                                break;
                        }
                    }
                    break;
                default :
                    dest[unescapedPtr++] = chars[i];
            }
        }
        return unescapedPtr;
    }

    /**
     * Performs unescaping and decoding of a key
     */
    static String decodeKey(char[] chars, int start, int end, boolean hasPreamble, char[] unescaped)
    {
        final int unescapedPtr = doUnescape(chars, start, end, unescaped);

        if (KEY_PREAMBLE_CHARS.length == unescapedPtr)
        {
            boolean isNull = true;
            for (int i = 0; i < unescapedPtr; i++)
            {
                if (KEY_PREAMBLE_CHARS[i] != unescaped[i])
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

        return hasPreamble ? new String(unescaped, DOUBLE_KEY_PREAMBLE_LENGTH, unescapedPtr
            - DOUBLE_KEY_PREAMBLE_LENGTH) : new String(unescaped, 0, unescapedPtr);
    }

    static String encodeValue(IValue value)
    {
        return value == null ? NULL_VALUE : value.toString();
    }

    /**
     * Performs unescaping and decoding of a value
     */
    static IValue decodeValue(char[] chars, int start, int end, char[] unescaped)
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

        return AbstractValue.constructFromCharValue(unescaped, unescapedPtr);
    }

    static String stringFromCharBuffer(char[] chars)
    {
        final char[] unescaped = new char[chars.length];
        final int unescapedPtr = doUnescape(chars, 0, chars.length, unescaped);
        // note: this does an array copy when constructing the string
        return new String(unescaped, 0, unescapedPtr);
    }

    static List<String> getNamesFromCommandMessage(char[] decodedMessage)
    {
        final char[][] tokens = findTokens(decodedMessage);
        final List<String> names = new ArrayList<String>(tokens.length);
        // the first item will be the command - we ignore this
        for (int i = 1; i < tokens.length; i++)
        {
            if (tokens[i] != null && tokens[i].length > 0)
            {
                names.add(stringFromCharBuffer(tokens[i]));
            }
        }
        return names;
    }

    static String getEncodedNamesForCommandMessage(String commandWithDelimiter, String... recordNames)
    {
        final CharArrayReference chars = new CharArrayReference(new char[CHARRAY_SIZE]);

        if (recordNames.length == 0)
        {
            return commandWithDelimiter;
        }
        else
        {
            StringBuilder sb = new StringBuilder(recordNames.length * 20);
            sb.append(commandWithDelimiter);
            escape(recordNames[0], sb, chars);
            for (int i = 1; i < recordNames.length; i++)
            {
                sb.append(DELIMITER);
                escape(recordNames[i], sb, chars);
            }
            return sb.toString();
        }
    }

    /**
     * @return the tokens as an array <code>char[]</code>, first index is the token index
     */
    static char[][] findTokens(final char[] chars)
    {
        int tokenIndex = 0;
        char[][] tokens = new char[10][];
        CharBuffer cbuf = CharBuffer.allocate(CharBufferUtils.BLOCK_SIZE);
        char previous = 0;
        int slashCount = 0;
        for (int i = 0; i < chars.length; i++)
        {
            switch(chars[i])
            {
                case CHAR_TOKEN_DELIM:
                    if (previous != CHAR_ESCAPE ||
                    // the previous was '\' and there was an even number of contiguous slashes
                        (slashCount % 2 == 0))
                    {
                        // an unescaped "|" is a true delimiter so start a new token
                        if (tokenIndex == tokens.length)
                        {
                            // resize
                            char[][] temp = new char[tokens.length + 10][];
                            System.arraycopy(tokens, 0, temp, 0, tokens.length);
                            tokens = temp;
                        }
                        tokens[tokenIndex++] = CharBufferUtils.getCharsFromBufferAndReset(cbuf);
                    }
                    else
                    {
                        // this is an escaped "|" so is part of the data (not a delimiter)
                        cbuf = CharBufferUtils.put(chars[i], cbuf);
                    }
                    slashCount = 0;
                    break;
                case CHAR_ESCAPE:
                    // we need to count how many "\" we have
                    // an even number means they are escaped so a "|" is a token
                    slashCount++;
                    cbuf = CharBufferUtils.put(chars[i], cbuf);
                    break;
                default :
                    slashCount = 0;
                    cbuf = CharBufferUtils.put(chars[i], cbuf);
            }
            previous = chars[i];
        }

        final char[][] retArr = new char[tokenIndex + 1][];
        System.arraycopy(tokens, 0, retArr, 0, tokenIndex);
        retArr[tokenIndex++] = CharBufferUtils.getCharsFromBufferAndReset(cbuf);
        return retArr;
    }

    @Override
    public byte[] getTxMessageForRpc(String rpcName, IValue[] args, String resultRecordName)
    {
        final Map<String, IValue> callDetails = new HashMap<String, IValue>();
        callDetails.put(Remote.RESULT_RECORD_NAME, TextValue.valueOf(resultRecordName));
        callDetails.put(Remote.ARGS_COUNT, LongValue.valueOf(args.length));
        for (int i = 0; i < args.length; i++)
        {
            callDetails.put(Remote.ARG_ + i, args[i]);
        }

        return encodeAtomicChange(RPC_COMMAND_CHARS, new AtomicChange(rpcName, callDetails, ContextUtils.EMPTY_MAP,
            ContextUtils.EMPTY_MAP), getCharset());
    }

    @Override
    public IRecordChange getRpcFromRxMessage(char[] decodedMessage)
    {
        return decodeAtomicChange(decodedMessage);
    }

    @Override
    public List<String> getSubscribeArgumentsFromDecodedMessage(char[] decodedMessage)
    {
        return getNamesFromCommandMessage(decodedMessage);
    }

    @Override
    public List<String> getUnsubscribeArgumentsFromDecodedMessage(char[] decodedMessage)
    {
        return getNamesFromCommandMessage(decodedMessage);
    }
    
    @Override
    public List<String> getResyncArgumentsFromDecodedMessage(char[] decodedMessage)
    {
        return getNamesFromCommandMessage(decodedMessage);
    }
    
    @Override
    public String getIdentityArgumentFromDecodedMessage(char[] decodedMessage)
    {
        return getNamesFromCommandMessage(decodedMessage).get(0);
    }

    @Override
    public char[] decode(byte[] data)
    {
        return getCharset().decode(ByteBuffer.wrap(data)).array();
    }

    @Override
    public FrameEncodingFormatEnum getFrameEncodingFormat()
    {
        return FrameEncodingFormatEnum.TERMINATOR_BASED;
    }

    @Override
    public ICodec<char[]> newInstance()
    {
        return new StringProtocolCodec();
    }

    @Override
    public Charset getCharset()
    {
        return UTF8;
    }

    @Override
    public byte[] getTxMessageForCodecSync()
    {
        return SYNC;
    }

    @Override
    public byte[] handleCodecSyncData(byte[] data)
    {
        if (!this.synced)
        {
            this.synced = true;
            return SYNC;
        }
        return null;
    }

    @Override
    public boolean isSynced()
    {
        return this.synced;
    }
}

/**
 * Utility to hold a char[] ref
 * 
 * @author Ramon Servadei
 */
final class CharArrayReference
{
    char[] ref;

    CharArrayReference(char[] carray)
    {
        this.ref = carray;
    }
}