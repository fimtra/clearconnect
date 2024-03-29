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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.ISessionProtocol;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.RpcInstance.Remote;
import com.fimtra.datafission.core.session.SimpleSessionProtocol;
import com.fimtra.datafission.field.AbstractValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.CharSubArrayKeyedPool;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.StringAppender;
import com.fimtra.util.ThreadUtils;

/**
 * A codec for messages that are sent between a {@link Publisher} and {@link ProxyContext} using a
 * string text protocol. The format of the string in ABNF notation:
 * 
 * <pre>
 *  preamble name seq [puts] [removes] [sub-map]
 *  
 *  preamble       = 0*ALPHA
 *  name           = "|" 1*ALPHA ; the name of the notifying record instance
 *  seq            = "|" scope seq_num
 *  scope          = "i" | "d" ; identifies either an image or delta
 *  seq_num        = 1*DIGIT ; the sequency number
 *  puts           = "|p" 1*key-value-pair
 *  removes        = "|r" 1*remove-key
 *  sub-map        = "|:|" name [puts] [removes]
 *  key-value-pair = "|" key "=" value
 *  remove-key     = "|" key "=" null
 *  key            = 1*ALPHA
 *  value          = 1*ALPHA
 *  
 *  e.g. |record_name|d322234|p|key1=value1|key2=value2|r|key_5=value5|:|subMap1|p|key1=value1
 * </pre>
 * 
 * @author Ramon Servadei
 */
public class StringProtocolCodec implements ICodec<char[]>
{
    public static final int CHARRAY_SIZE = 32;

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
    static final char NULL_CHAR = 0x2;
    static final String NULL_VALUE = new String(new char[] { NULL_CHAR });
    static final int DOUBLE_KEY_PREAMBLE_LENGTH = 2;

    final ISessionProtocol sessionSyncProtocol;
    final Function<ByteBuffer, byte[]> encodedBytesHandler;

    public StringProtocolCodec()
    {
        this(new SimpleSessionProtocol());
    }

    protected StringProtocolCodec(Function<ByteBuffer, byte[]> handler)
    {
        this(new SimpleSessionProtocol(), handler);
    }

    protected StringProtocolCodec(ISessionProtocol sessionSyncProtocol)
    {
        this(sessionSyncProtocol, (encoded) -> Arrays.copyOf(encoded.array(), encoded.limit()));
    }

    protected StringProtocolCodec(ISessionProtocol sessionSyncProtocol,
        Function<ByteBuffer, byte[]> encodedBytesHandler)
    {
        this.sessionSyncProtocol = sessionSyncProtocol;
        this.encodedBytesHandler = encodedBytesHandler;
    }

    @Override
    public final CommandEnum getCommand(char[] decodedMessage)
    {
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

    Function<ByteBuffer, byte[]> getEncodedBytesHandler()
    {
        return this.encodedBytesHandler;
    }

    /**
     * Get the string representing the record changes to transmit to a {@link ProxyContext}.
     * 
     * @return the string representing the changes
     */
    @Override
    public byte[] getTxMessageForAtomicChange(IRecordChange atomicChange)
    {
        return encodeAtomicChange(DELIMITER_CHARS, atomicChange, getCharset(), getEncodedBytesHandler());
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
     * Convert a byte[] created from the {@link #getTxMessageForAtomicChange}
     * method into a {@link AtomicChange} representing the 'puts' and 'removes' to a named record
     * instance.
     * 
     * @param data
     *            the received ByteBuffer
     * @return the converted change from the data
     * @throws RuntimeException
     *             if there is a problem converting
     */
    @Override
    public IRecordChange getAtomicChangeFromRxMessage(ByteBuffer data)
    {
        return decodeAtomicChange(decode(data));
    }

    static class DecodingBuffers
    {
        char[] tempArr;
        int[][] bijTokenOffset;
        int[][] bijTokenLimit;
        int[] bijTokenLen;
    }

    final static ThreadLocal<DecodingBuffers> DECODING_BUFFERS = ThreadLocal.withInitial(() -> {
        ThreadUtils.registerThreadLocalCleanup(StringProtocolCodec.DECODING_BUFFERS::remove);

        final DecodingBuffers instance = new DecodingBuffers();
        instance.tempArr = new char[50];
        instance.bijTokenOffset = new int[1][10];
        instance.bijTokenLimit = new int[1][10];
        instance.bijTokenLen = new int[1];
        return instance;
    });

    @SuppressWarnings("null")
    static IRecordChange decodeAtomicChange(char[] decodedMessage)
    {
        final DecodingBuffers decodingBuffers = DECODING_BUFFERS.get();
        // use bijectional arrays to track the offset+len of each token
        // NOTE: uses a 2d array to as a pointer to a 1d array, this allows methods to resize the 1d
        // array
        final int[][] bijTokenOffset = decodingBuffers.bijTokenOffset;
        final int[][] bijTokenLimit = decodingBuffers.bijTokenLimit;
        final int[] bijTokenLen = decodingBuffers.bijTokenLen;

        try
        {
            findTokens(decodedMessage, bijTokenLen, bijTokenOffset, bijTokenLimit);
            final String name = stringFromCharBuffer(decodedMessage, bijTokenOffset[0][1], bijTokenLimit[0][1]);
            final AtomicChange atomicChange = new AtomicChange(name);

            // optimise the locking for the internal getXXX methods
            synchronized (atomicChange)
            {
                // set the scope and sequence
                atomicChange.setScope(decodedMessage[bijTokenOffset[0][2]]);
                atomicChange.setSequence(LongValue.valueOf(decodedMessage, bijTokenOffset[0][2] + 1,
                    bijTokenLimit[0][2] - bijTokenOffset[0][2] - 1).longValue());

                boolean put = true;
                String subMapName = null;
                AtomicChange target = null;
                int position;
                int len;
                if (bijTokenLen[0] > 2)
                {
                    char previous;
                    int j;
                    for (int i = 3; i < bijTokenLen[0]; i++)
                    {
                        position = bijTokenOffset[0][i];
                        len = bijTokenLimit[0][i] - position;
                        if (len == 1)
                        {
                            switch(decodedMessage[bijTokenOffset[0][i]])
                            {
                                case PUT_CODE:
                                    put = true;
                                    if (subMapName == null)
                                    {
                                        target = atomicChange;
                                    }
                                    else
                                    {
                                        target = atomicChange.internalGetSubMapAtomicChange(subMapName);
                                    }
                                    target.internalGetPutEntries();
                                    break;
                                case REMOVE_CODE:
                                    put = false;
                                    if (subMapName == null)
                                    {
                                        target = atomicChange;
                                    }
                                    else
                                    {
                                        target = atomicChange.internalGetSubMapAtomicChange(subMapName);
                                    }
                                    target.internalGetRemovedEntries();
                                    break;
                                case SUBMAP_CODE:
                                    ++i;
                                    subMapName =
                                        stringFromCharBuffer(decodedMessage, bijTokenOffset[0][i], bijTokenLimit[0][i]);
                                    break;
                                default :
                                    break;
                            }
                        }
                        else
                        {
                            previous = 0;
                            // length must be relative to the position
                            len += position;
                            if (decodingBuffers.tempArr.length < len)
                            {
                                decodingBuffers.tempArr = new char[len];
                            }
                            for (j = position; j < len; j++)
                            {
                                switch(decodedMessage[j])
                                {
                                    case CHAR_KEY_VALUE_SEPARATOR:
                                        // find where the first non-escaped "=" is
                                        if (previous != CHAR_ESCAPE)
                                        {
                                            if (put)
                                            {
                                                target.addEntry_onlyCallFromCodec(
                                                    decodeKey(decodedMessage, position, j, true,
                                                        decodingBuffers.tempArr),
                                                    decodeValue(decodedMessage, j + 1, len, decodingBuffers.tempArr));
                                            }
                                            else
                                            {
                                                target.removeEntry_onlyCallFromCodec(
                                                    decodeKey(decodedMessage, position, j, true,
                                                        decodingBuffers.tempArr),
                                                    decodeValue(decodedMessage, j + 1, len, decodingBuffers.tempArr));
                                            }
                                            j = decodedMessage.length;
                                        }
                                        break;
                                    default :
                                        previous = decodedMessage[j];
                                }
                            }
                            // remove any keys that are in put and removed - leave in removed
                            if (target.putEntries != null && target.removedEntries != null
                                && target.removedEntries.size() > 0)
                            {
                                target.putEntries.keySet().removeAll(target.removedEntries.keySet());
                            }
                        }
                    }
                }
                return atomicChange;
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not decode '" + new String(decodedMessage) + "'", e);
        }
    }

    static class EncodingBuffers
    {
        CharArrayReference charArrayRef;
        CharArrayReference keyCharArrayRef;
        char[] escapedChars;
        StringAppender sb;

        final IdentityHashMap<Charset, CharsetEncoder> encoders = new IdentityHashMap<>(4);

        CharsetEncoder getEncoder(Charset cs)
        {
            CharsetEncoder encoder = this.encoders.get(cs);
            if (encoder == null)
            {
                encoder = cs.newEncoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(
                    CodingErrorAction.REPLACE);
                this.encoders.put(cs, encoder);
            }
            return encoder;
        }
    }

    final static ThreadLocal<EncodingBuffers> ENCODING_BUFFERS = ThreadLocal.withInitial(() -> {
        ThreadUtils.registerThreadLocalCleanup(StringProtocolCodec.ENCODING_BUFFERS::remove);

        final EncodingBuffers instance = new EncodingBuffers();
        instance.sb = new StringAppender(1000);
        instance.charArrayRef = new CharArrayReference(new char[CHARRAY_SIZE]);

        instance.keyCharArrayRef = new CharArrayReference(new char[CHARRAY_SIZE]);
        instance.keyCharArrayRef.ref[0] = NULL_CHAR;
        instance.keyCharArrayRef.ref[1] = NULL_CHAR;

        instance.escapedChars = new char[2];
        instance.escapedChars[0] = CHAR_ESCAPE;

        return instance;
    });

    static byte[] encodeAtomicChange(char[] preamble, IRecordChange atomicChange, Charset charSet,
        Function<ByteBuffer, byte[]> encodedHandler)
    {
        final Map<String, IValue> putEntries;
        final Map<String, IValue> removedEntries;
        final Set<String> subMapKeys;
        // optimise the locking for the internal getXXX methods
        synchronized (atomicChange)
        {
            if (atomicChange instanceof AtomicChange)
            {
                putEntries = ((AtomicChange) atomicChange).internalGetPutEntries();
                removedEntries = ((AtomicChange) atomicChange).internalGetRemovedEntries();
                subMapKeys = ((AtomicChange) atomicChange).internalGetSubMapKeys();
            }
            else
            {
                putEntries = atomicChange.getPutEntries();
                removedEntries = atomicChange.getRemovedEntries();
                subMapKeys = atomicChange.getSubMapKeys();
            }
        }

        final EncodingBuffers encodingBuffers = ENCODING_BUFFERS.get();
        encodingBuffers.sb.setLength(0);
        final CharArrayReference charArrayRef = encodingBuffers.charArrayRef;
        final CharArrayReference keyCharArrayRef = encodingBuffers.keyCharArrayRef;
        final char[] escapedChars = encodingBuffers.escapedChars;
        final StringAppender sb = encodingBuffers.sb;

        sb.append(preamble);
        escape(atomicChange.getName(), sb, charArrayRef, escapedChars);
        // add the sequence
        sb.append(DELIMITER).append(atomicChange.getScope()).append(atomicChange.getSequence());
        addEntriesToTxString(DELIMITER_PUT_CODE, putEntries, sb, charArrayRef, escapedChars, keyCharArrayRef);
        addEntriesToTxString(DELIMITER_REMOVE_CODE, removedEntries, sb, charArrayRef, escapedChars, keyCharArrayRef);
        IRecordChange subMapAtomicChange;
        if (subMapKeys.size() > 0)
        {
            for (String subMapKey : subMapKeys)
            {
                subMapAtomicChange = atomicChange.getSubMapAtomicChange(subMapKey);
                synchronized (subMapAtomicChange)
                {
                    sb.append(DELIMITER_SUBMAP_CODE);
                    escape(subMapKey, sb, charArrayRef, escapedChars);
                    addEntriesToTxString(DELIMITER_PUT_CODE,
                        subMapAtomicChange instanceof AtomicChange
                            ? ((AtomicChange) subMapAtomicChange).internalGetPutEntries()
                            : subMapAtomicChange.getPutEntries(),
                        sb, charArrayRef, escapedChars, keyCharArrayRef);
                    addEntriesToTxString(DELIMITER_REMOVE_CODE,
                        subMapAtomicChange instanceof AtomicChange
                            ? ((AtomicChange) subMapAtomicChange).internalGetRemovedEntries()
                            : subMapAtomicChange.getRemovedEntries(),
                        sb, charArrayRef, escapedChars, keyCharArrayRef);
                }
            }
        }

        try
        {
            return encodedHandler.apply(encodingBuffers.getEncoder(charSet).encode(sb.getCharBuffer()));
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException("Could not encode " + atomicChange, e);
        }
    }

    private static void addEntriesToTxString(final char[] changeType, final Map<String, IValue> entries,
        final StringAppender txString, final CharArrayReference chars, final char[] escapedChars,
        final CharArrayReference keyChars)
    {
        if (entries != null && entries.size() > 0)
        {
            String key;
            IValue value;
            int i;
            int last;
            int length;
            char[] cbuf;
            escapedChars[0] = CHAR_ESCAPE;
            boolean needToEscape;
            txString.append(changeType);

            for (Map.Entry<String, IValue> entry : entries.entrySet())
            {
                key = entry.getKey();
                value = entry.getValue();
                txString.append(DELIMITER);
                if (key == null)
                {
                    txString.append(NULL_CHAR);
                }
                else
                {
                    length = key.length() + DOUBLE_KEY_PREAMBLE_LENGTH;
                    if (keyChars.ref.length < length)
                    {
                        // resize
                        keyChars.ref = new char[length];
                        keyChars.ref[0] = NULL_CHAR;
                        keyChars.ref[1] = NULL_CHAR;
                    }
                    cbuf = keyChars.ref;
                    key.getChars(0, key.length(), cbuf, DOUBLE_KEY_PREAMBLE_LENGTH);

                    // NOTE: for efficiency, we have *almost* inlined versions of the same escape
                    // switch statements
                    needToEscape = false;
                    for (i = 0; i < length; i++)
                    {
                        switch(cbuf[i])
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
                            switch(cbuf[i])
                            {
                                case CR:
                                    txString.append(cbuf, last, i - last);
                                    escapedChars[1] = CHAR_r;
                                    txString.append(escapedChars, 0, 2);
                                    last = i + 1;
                                    break;
                                case LF:
                                    txString.append(cbuf, last, i - last);
                                    escapedChars[1] = CHAR_n;
                                    txString.append(escapedChars, 0, 2);
                                    last = i + 1;
                                    break;
                                case CHAR_ESCAPE:
                                case CHAR_TOKEN_DELIM:
                                case CHAR_KEY_VALUE_SEPARATOR:
                                    txString.append(cbuf, last, i - last);
                                    escapedChars[1] = cbuf[i];
                                    txString.append(escapedChars, 0, 2);
                                    last = i + 1;
                                    break;
                                default:
                            }
                        }
                        txString.append(cbuf, last, length - last);
                    }
                    else
                    {
                        txString.append(cbuf, 0, length);
                    }
                }

                txString.append(CHAR_KEY_VALUE_SEPARATOR);
                if (value == null || changeType == DELIMITER_REMOVE_CODE)
                {
                    txString.append(NULL_CHAR);
                }
                else
                {
                    switch(value.getType())
                    {
                        case DOUBLE:
                        case LONG:
                        case BLOB:
                            // longs, doubles and blobs do not need escaping
                            // note: blob string is "B<hex string for bytes>", e.g. B7366abc4
                            value.appendTo(txString);
                            break;
                        case TEXT:
                        default:
                            txString.append(IValue.TEXT_CODE);
                            escape(value.textValue(), txString, chars, escapedChars);
                            break;
                    }
                }
            }
        }
    }

    /**
     * Escape special chars in the value-to-send, ultimately adding the escaped value into the
     * destination StringAppender
     */
    static void escape(String valueToSend, StringAppender dest, CharArrayReference charsRef, char[] escapedChars)
    {
        try
        {
            final int length = valueToSend.length();
            if (charsRef.ref.length < length)
            {
                // resize
                charsRef.ref = new char[length];
            }

            final char[] chars = charsRef.ref;
            valueToSend.getChars(0, valueToSend.length(), chars, 0);

            int last = 0;
            for (int i = 0; i < length; i++)
            {
                switch(chars[i])
                {
                    case CR:
                        dest.append(chars, last, i - last);
                        escapedChars[1] = CHAR_r;
                        dest.append(escapedChars, 0, 2);
                        last = i + 1;
                        break;
                    case LF:
                        dest.append(chars, last, i - last);
                        escapedChars[1] = CHAR_n;
                        dest.append(escapedChars, 0, 2);
                        last = i + 1;
                        break;
                    case CHAR_ESCAPE:
                    case CHAR_TOKEN_DELIM:
                    case CHAR_KEY_VALUE_SEPARATOR:
                    case CHAR_SYMBOL_PREFIX:
                        dest.append(chars, last, i - last);
                        escapedChars[1] = chars[i];
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

    static final CharSubArrayKeyedPool<String> decodedKeysPool =
        new CharSubArrayKeyedPool<String>("codec-decoded-keys", 0, Record.keysPool)
        {
            @Override
            public String newInstance(String string)
            {
                return string;
            }
        };

    /**
     * Performs unescaping and decoding of a key
     */
    static String decodeKey(char[] chars, int start, int end, boolean hasPreamble, char[] unescaped)
    {
        final int unescapedPtr = doUnescape(chars, start, end, unescaped);

        if (unescapedPtr == 1 && unescaped[0] == NULL_CHAR)
        {
            return null;
        }

        return hasPreamble
            ? decodedKeysPool.get(unescaped, DOUBLE_KEY_PREAMBLE_LENGTH, unescapedPtr - DOUBLE_KEY_PREAMBLE_LENGTH)
            : decodedKeysPool.get(unescaped, 0, unescapedPtr);
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

        if (unescapedPtr == 1 && unescaped[0] == NULL_CHAR)
        {
            return AbstractValue.constructFromCharValue(null, 0);
        }

        return AbstractValue.constructFromCharValue(unescaped, unescapedPtr);
    }

    static String stringFromCharBuffer(char[] chars, int offset, int limit)
    {
        final char[] unescaped = new char[limit - offset];
        final int unescapedPtr = doUnescape(chars, offset, limit, unescaped);
        // note: this does an array copy when constructing the string
        return new String(unescaped, 0, unescapedPtr);
    }

    static List<String> getNamesFromCommandMessage(char[] decodedMessage)
    {
        final int[][] bijTokenOffset = new int[1][10];
        final int[][] bijTokenLimit = new int[1][10];
        final int[] bijTokenLen = new int[1];

        findTokens(decodedMessage, bijTokenLen, bijTokenOffset, bijTokenLimit);

        final List<String> names = new ArrayList<>(bijTokenLen[0]);
        // the first item will be the command - we ignore this
        for (int i = 1; i < bijTokenLen[0]; i++)
        {
            names.add(stringFromCharBuffer(decodedMessage, bijTokenOffset[0][i], bijTokenLimit[0][i]));
        }
        return names;
    }

    static String getEncodedNamesForCommandMessage(String commandWithDelimiter, String... recordNames)
    {
        final CharArrayReference chars = new CharArrayReference(new char[CHARRAY_SIZE]);
        final char[] escapedChars = new char[2];
        escapedChars[0] = CHAR_ESCAPE;

        if (recordNames.length == 0)
        {
            return commandWithDelimiter;
        }
        else
        {
            StringAppender sb = new StringAppender(recordNames.length * 20);
            sb.append(commandWithDelimiter);
            escape(recordNames[0], sb, chars, escapedChars);
            for (int i = 1; i < recordNames.length; i++)
            {
                sb.append(DELIMITER);
                escape(recordNames[i], sb, chars, escapedChars);
            }
            return sb.toString();
        }
    }

    /**
     * Find the indexes of the tokens within the main chars array. Uses bijectional arrays to hold
     * the offset and limit parts in the main chars array for each token found. Note: 2-dimensional
     * arrays used as pointer to the 1-d array to allow pass-back if they are resized.
     */
    static void findTokens(final char[] chars, int[] bijTokenLen, int[][] bijTokenOffset, int[][] bijTokenLimit)
    {
        bijTokenLen[0] = 0;

        int cbufPtr = 0;
        char previous = 0;
        int slashCount = 0;
        int len = chars.length;
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
                        if (bijTokenLen[0] == bijTokenOffset[0].length)
                        {
                            // resize
                            bijTokenOffset[0] = Arrays.copyOf(bijTokenOffset[0], bijTokenOffset[0].length + 10);
                            bijTokenLimit[0] = Arrays.copyOf(bijTokenLimit[0], bijTokenLimit[0].length + 10);
                        }
                        // NOTE: +1 to skip the "|"
                        bijTokenOffset[0][bijTokenLen[0]] = cbufPtr + 1;
                        bijTokenLimit[0][bijTokenLen[0]] = i;
                        bijTokenLen[0]++;
                        cbufPtr = i;
                    }
                    else
                    {
                        // this is an escaped "|" so is part of the data (not a delimiter)
                    }
                    slashCount = 0;
                    break;
                case CHAR_ESCAPE:
                    // we need to count how many "\" we have
                    // an even number means they are escaped so a "|" is a token
                    slashCount++;
                    break;
                case 0:
                    // when decoding a byte[] into a char[], the byte[] and char[] lengths are the
                    // same BUT characters taking up 2 bytes for encoding only take up 1 char so we
                    // end up with trailing 0 in the char[], e.g. '£' = [-62][-93] for bytes but is
                    // 1 char in a char[]
                    len--;
                    break;
                default :
                    slashCount = 0;
            }
            previous = chars[i];
        }

        if (bijTokenLen[0] == bijTokenOffset[0].length)
        {
            // resize
            bijTokenOffset[0] = Arrays.copyOf(bijTokenOffset[0], bijTokenOffset[0].length + 10);
            bijTokenLimit[0] = Arrays.copyOf(bijTokenLimit[0], bijTokenLimit[0].length + 10);
        }
        bijTokenOffset[0][bijTokenLen[0]] = cbufPtr + 1;
        bijTokenLimit[0][bijTokenLen[0]] = len;
        bijTokenLen[0]++;
    }

    @Override
    public byte[] getTxMessageForRpc(String rpcName, IValue[] args, String resultRecordName)
    {
        final AtomicChange atomicChange = new AtomicChange(rpcName);
        final Map<String, IValue> callDetails = atomicChange.internalGetPutEntries();
        callDetails.put(Remote.RESULT_RECORD_NAME, TextValue.valueOf(resultRecordName));
        callDetails.put(Remote.ARGS_COUNT, LongValue.valueOf(args.length));
        for (int i = 0; i < args.length; i++)
        {
            callDetails.put(Remote.ARG_ + i, args[i]);
        }

        return encodeAtomicChange(RPC_COMMAND_CHARS, atomicChange, getCharset(), getEncodedBytesHandler());
    }

    @Override
    public final IRecordChange getRpcFromRxMessage(char[] decodedMessage)
    {
        return decodeAtomicChange(decodedMessage);
    }

    @Override
    public final List<String> getSubscribeArgumentsFromDecodedMessage(char[] decodedMessage)
    {
        return getNamesFromCommandMessage(decodedMessage);
    }

    @Override
    public final List<String> getUnsubscribeArgumentsFromDecodedMessage(char[] decodedMessage)
    {
        return getNamesFromCommandMessage(decodedMessage);
    }

    @Override
    public final List<String> getResyncArgumentsFromDecodedMessage(char[] decodedMessage)
    {
        return getNamesFromCommandMessage(decodedMessage);
    }

    @Override
    public final String getIdentityArgumentFromDecodedMessage(char[] decodedMessage)
    {
        return getNamesFromCommandMessage(decodedMessage).get(0);
    }

    @Override
    public char[] decode(ByteBuffer data)
    {
        return StandardCharsets.UTF_8.decode(this.sessionSyncProtocol.decode(data)).array();
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
        return StandardCharsets.UTF_8;
    }

    @Override
    public final byte[] finalEncode(byte[] data)
    {
        return this.sessionSyncProtocol.encode(data);
    }

    @Override
    public final ISessionProtocol getSessionProtocol()
    {
        return this.sessionSyncProtocol;
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
