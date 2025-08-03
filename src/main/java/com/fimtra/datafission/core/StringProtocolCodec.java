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
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.fimtra.datafission.DataFissionProperties;
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
 * A codec for messages that are sent between a {@link Publisher} and {@link ProxyContext} using a string text
 * protocol. The format of the string in ABNF notation:
 *
 * <pre>
 *  preamble name seq [puts] [removes] *[sub-map]
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
        final DecodingBuffers decodingBuffers = DECODING_BUFFERS.get();
        return decodeAtomicChange(decode(data, decodingBuffers.getDecoder(getCharset())), decodingBuffers);
    }

    static class DecodingBuffers
    {
        char[] keyArr;
        char[] valArr;
        char[] dataArr;

        final IdentityHashMap<Charset, CharsetDecoder> decoders = new IdentityHashMap<>(4);

        CharsetDecoder getDecoder(Charset cs)
        {
            return this.decoders.computeIfAbsent(cs, Charset::newDecoder);
        }
    }

    static final ThreadLocal<DecodingBuffers> DECODING_BUFFERS = ThreadLocal.withInitial(() -> {
        ThreadUtils.registerThreadLocalCleanup(StringProtocolCodec.DECODING_BUFFERS::remove);

        final DecodingBuffers instance = new DecodingBuffers();
        instance.keyArr = new char[50];
        instance.valArr = new char[50];
        return instance;
    });

    static final AtomicChange NULL_CHANGE = new AtomicChange("NULL_CHANGE");
    static final Map<String, IValue> NULL_MAP = new HashMap<>();

    static IRecordChange decodeAtomicChange(char[] decodedMessage, DecodingBuffers decodingBuffers)
    {
        AtomicChange atomicChange = NULL_CHANGE;
        AtomicChange target = NULL_CHANGE;
        String subMapName = null;
        Map<String, IValue> targetMap = NULL_MAP;

        boolean sequenceAndScopeSet = false;
        boolean expectingSubmapName = false;
        char c;
        char previous = 0;
        int slashCount = 0;

        int keyPtr = 0;
        int dataPtr = 0;
        int sectionStart = -1;
        int i = 0;

        // todo this can resolve to rather large arrays being kept - need to optimise somehow
        // belt-n-braces buffer resizing - we assume worst case scenario for the buffer sizes
        final int msgLen = decodedMessage.length;
        if (decodingBuffers.keyArr.length < msgLen)
        {
            decodingBuffers.keyArr = new char[getNewSize(decodingBuffers.keyArr.length, msgLen)];
        }
        if (decodingBuffers.valArr.length < msgLen)
        {
            decodingBuffers.valArr = new char[getNewSize(decodingBuffers.keyArr.length, msgLen)];
        }

        decodingBuffers.dataArr = decodingBuffers.keyArr;

        // Brief description:
        // handle the header and data in dedicated while-loops, breaking when the relevant attributes are complete
        // so we have a while-loop covering each of these sections: preamble, name, scope+sequence, data
        // we scan through the char[] once, breaking out of the while-loop when we have all the data for each section
        // we break out of the while-loop, this is a bit like a goto but not as bad

        // preamble
        while (true)
        {
            c = decodedMessage[i];
            if (c == CHAR_TOKEN_DELIM && (previous != CHAR_ESCAPE ||
                    // the previous was '\' and there was an even number of contiguous slashes
                    ((slashCount & 0x1) == 0)))
            {
                i++;
                break;
            }
            slashCount = (c == CHAR_ESCAPE) ? slashCount + 1 : 0;
            previous = c;
            i++;
        }

        // name
        while (true)
        {
            c = decodedMessage[i];
            if (c == CHAR_TOKEN_DELIM)
            {
                // record names will be resolved multiple times, so use a pool to reduce memory churn
                atomicChange =
                        new AtomicChange(resolvePooledStringNoPreamble(decodingBuffers.dataArr, dataPtr));
                i++;
                sectionStart = i;
                break;
            }
            else if (c == CHAR_ESCAPE)
            {
                dataPtr = handleEscapeChar(decodedMessage[++i], decodingBuffers.dataArr, dataPtr);
            }
            else if (c != 0)
            {
                decodingBuffers.dataArr[dataPtr++] = c;
            }
            i++;
        }

        // optimise the locking for the internal getXXX methods
        synchronized (atomicChange)
        {
            // scope and sequence
            while (i != msgLen)
            {
                c = decodedMessage[i];
                if (c == CHAR_TOKEN_DELIM && (previous != CHAR_ESCAPE ||
                        // the previous was '\' and there was an even number of contiguous slashes
                        ((slashCount & 0x1) == 0)))
                {
                    atomicChange.setScope(decodedMessage[sectionStart++]);
                    atomicChange.setSequence(
                            LongValue.valueOf(decodedMessage, sectionStart, i - (sectionStart))
                                    .longValue());
                    i++;
                    sectionStart = i;
                    sequenceAndScopeSet = true;
                    break;
                }
                slashCount = (c == CHAR_ESCAPE) ? slashCount + 1 : 0;
                previous = c;
                i++;
            }

            decodingBuffers.dataArr = decodingBuffers.keyArr;
            dataPtr = 0;

            target = atomicChange;
            // data
            while (i != msgLen)
            {
                c = decodedMessage[i];
                if (c == CHAR_TOKEN_DELIM)
                {
                    // its the end of a token section "|"
                    if (i - sectionStart == 1)
                    {
                        switch(decodedMessage[i - 1])
                        {
                            case PUT_CODE:
                                targetMap = target.internalGetPutEntries();
                                break;
                            case REMOVE_CODE:
                                targetMap = target.internalGetRemovedEntries();
                                break;
                            case SUBMAP_CODE:
                                // we can't get the submap name just yet, we need to hit a delimiter
                                expectingSubmapName = true;
                                break;
                            default:
                                throw new IllegalArgumentException("Unknown code: " + decodedMessage[i - 1]);
                        }
                    }
                    else
                    {
                        if (expectingSubmapName)
                        {
                            alignRemovedEntries(target);

                            subMapName = resolvePooledStringNoPreamble(decodingBuffers.dataArr, dataPtr);
                            target = atomicChange.internalGetSubMapAtomicChange(subMapName);
                            expectingSubmapName = false;
                        }
                        else
                        {
                            // key=value
                            targetMap.put(resolvePooledStringWithPreamble(decodingBuffers.keyArr, keyPtr),
                                    resolveValue(decodingBuffers.dataArr, dataPtr));

                            decodingBuffers.dataArr = decodingBuffers.keyArr;
                        }
                    }
                    sectionStart = i + 1;
                    dataPtr = 0;
                }
                else if (c == CHAR_KEY_VALUE_SEPARATOR)
                {
                    decodingBuffers.dataArr = decodingBuffers.valArr;
                    keyPtr = dataPtr;
                    dataPtr = 0;
                }
                else if (c == CHAR_ESCAPE)
                {
                    dataPtr = handleEscapeChar(decodedMessage[++i], decodingBuffers.dataArr, dataPtr);
                }
                else if (c != 0)
                {
                    // when decoding a byte[] into a char[], the byte[] and char[] lengths are the
                    // same BUT characters taking up 2 bytes for encoding only take up 1 char so we
                    // end up with trailing 0 in the char[], e.g. '£' = [-62][-93] for bytes but is
                    // 1 char in a char[]
                    decodingBuffers.dataArr[dataPtr++] = c;
                }
                i++;
            }

            // process the last one
            // could be an empty change, e.g. "|record1|i0"
            if (!sequenceAndScopeSet)
            {
                atomicChange.setScope(decodedMessage[sectionStart++]);
                atomicChange.setSequence(LongValue.valueOf(decodedMessage, sectionStart, i - (sectionStart))
                        .longValue());
            }
            // could be a fragmented change, ending with a submap name, e.g. |record1|i0|p|key=value|:|submap
            else if (expectingSubmapName)
            {
                subMapName = resolvePooledStringNoPreamble(decodingBuffers.dataArr, dataPtr);
                target.getSubMapAtomicChange(subMapName);
            }
            else
            {
                targetMap.put(resolvePooledStringWithPreamble(decodingBuffers.keyArr, keyPtr),
                        resolveValue(decodingBuffers.dataArr, dataPtr));
            }

            alignRemovedEntries(target);
        }
        return atomicChange;
    }

    private static void alignRemovedEntries(AtomicChange target)
    {
        // remove any keys that are in put and removed - leave in removed
        if (target.putEntries != null && target.removedEntries != null && !target.removedEntries.isEmpty())
        {
            target.putEntries.keySet()
                    .removeAll(target.removedEntries.keySet());
        }
    }

    private static int getNewSize(int currentLength, int newLength)
    {
        return Math.max(currentLength * 2, newLength);
    }

    private static int handleEscapeChar(char current, char[] data, int dataPtr)
    {
        switch(current)
        {
            case CHAR_r:
                data[dataPtr++] = CR;
                break;
            case CHAR_n:
                data[dataPtr++] = LF;
                break;
            case CHAR_ESCAPE:
                data[dataPtr++] = CHAR_ESCAPE;
                break;
            case CHAR_TOKEN_DELIM:
                data[dataPtr++] = CHAR_TOKEN_DELIM;
                break;
            case CHAR_KEY_VALUE_SEPARATOR:
                data[dataPtr++] = CHAR_KEY_VALUE_SEPARATOR;
                break;
            case CHAR_SYMBOL_PREFIX:
                data[dataPtr++] = CHAR_SYMBOL_PREFIX;
                break;
        }
        return dataPtr;
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
            return this.encoders.computeIfAbsent(cs, c -> c.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE));
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
        final EncodingBuffers encodingBuffers = ENCODING_BUFFERS.get();
        encodingBuffers.sb.setLength(0);
        final CharArrayReference charArrayRef = encodingBuffers.charArrayRef;
        final CharArrayReference keyCharArrayRef = encodingBuffers.keyCharArrayRef;
        final char[] escapedChars = encodingBuffers.escapedChars;
        final StringAppender sb = encodingBuffers.sb;

        sb.append(preamble);
        escape(atomicChange.getName(), sb, charArrayRef, escapedChars);
        // add the sequence
        sb.append(DELIMITER)
                .append(atomicChange.getScope())
                .append(atomicChange.getSequence());
        processPutRemoves(atomicChange, sb, charArrayRef, escapedChars, keyCharArrayRef);

        // sub-maps (if any)
        final Set<String> subMapKeys = atomicChange.getSubMapKeys();
        if (!subMapKeys.isEmpty())
        {
            IRecordChange subMapAtomicChange;
            for (String subMapKey : subMapKeys)
            {
                subMapAtomicChange = atomicChange.getSubMapAtomicChange(subMapKey);
                synchronized (subMapAtomicChange)
                {
                    sb.append(DELIMITER_SUBMAP_CODE);
                    escape(subMapKey, sb, charArrayRef, escapedChars);
                    processPutRemoves(subMapAtomicChange, sb, charArrayRef, escapedChars, keyCharArrayRef);
                }
            }
        }

        try
        {
            return encodedHandler.apply(encodingBuffers.getEncoder(charSet)
                    .encode(sb.getCharBuffer()));
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException("Could not encode " + atomicChange, e);
        }
    }

    private static void processPutRemoves(IRecordChange change, StringAppender sb,
            CharArrayReference charArrayRef, char[] escapedChars, CharArrayReference keyCharArrayRef)
    {
        Map<String, IValue> entries;

        if (change instanceof AtomicChange)
        {
            entries = ((AtomicChange) change).putEntries;
            if (notEmpty(entries))
            {
                addEntriesToTxString(DELIMITER_PUT_CODE, entries, sb, charArrayRef, escapedChars,
                        keyCharArrayRef);
            }
            entries = ((AtomicChange) change).removedEntries;
            if (notEmpty(entries))
            {
                addEntriesToTxString(DELIMITER_REMOVE_CODE, ((AtomicChange) change).removedEntries, sb,
                        charArrayRef, escapedChars, keyCharArrayRef);
            }
        }
        else
        {
            entries = change.getPutEntries();
            if (notEmpty(entries))
            {
                addEntriesToTxString(DELIMITER_PUT_CODE, entries, sb, charArrayRef, escapedChars,
                        keyCharArrayRef);
            }
            entries = change.getRemovedEntries();
            if (notEmpty(entries))
            {
                addEntriesToTxString(DELIMITER_REMOVE_CODE, entries, sb, charArrayRef, escapedChars,
                        keyCharArrayRef);
            }
        }
    }

    private static boolean notEmpty(Map<String, IValue> entries)
    {
        return entries != null && !entries.isEmpty();
    }

    private static void addEntriesToTxString(final char[] changeType, final Map<String, IValue> entries,
        final StringAppender txString, final CharArrayReference chars, final char[] escapedChars,
        final CharArrayReference keyChars)
    {
        String key;
        IValue value;
        int i;
        int last;
        int length;
        char[] cbuf;
        escapedChars[0] = CHAR_ESCAPE;
        txString.append(changeType);

        for (Map.Entry<String, IValue> entry : entries.entrySet())
        {
            key = entry.getKey();
            value = entry.getValue();
            txString.append(DELIMITER);

            // note: key is never null, records do not allow null keys
            length = key.length() + DOUBLE_KEY_PREAMBLE_LENGTH;
            if (keyChars.ref.length < length)
            {
                keyChars.ref = new char[getNewSize(keyChars.ref.length, length)];
                keyChars.ref[0] = NULL_CHAR;
                keyChars.ref[1] = NULL_CHAR;
            }
            cbuf = keyChars.ref;
            key.getChars(0, key.length(), cbuf, DOUBLE_KEY_PREAMBLE_LENGTH);

            last = 0;
            char charAt;
            for (i = 0; i < length; i++)
            {
                charAt = cbuf[i];
                switch(charAt)
                {
                    case CR:
                        escapedChars[1] = CHAR_r;
                        txString.append(cbuf, last, i - last, escapedChars, 0, 2);
                        last = i + 1;
                        break;
                    case LF:
                        escapedChars[1] = CHAR_n;
                        txString.append(cbuf, last, i - last, escapedChars, 0, 2);
                        last = i + 1;
                        break;
                    case CHAR_ESCAPE:
                    case CHAR_TOKEN_DELIM:
                    case CHAR_KEY_VALUE_SEPARATOR:
                        escapedChars[1] = charAt;
                        txString.append(cbuf, last, i - last, escapedChars, 0, 2);
                        last = i + 1;
                        break;
                    default:
                }
            }
            txString.append(cbuf, last, length - last);

            txString.append(CHAR_KEY_VALUE_SEPARATOR);
            if (value == null || changeType == DELIMITER_REMOVE_CODE)
            {
                txString.append(NULL_CHAR);
            }
            else if (value.getType() == IValue.TypeEnum.TEXT)
            {
                txString.append(IValue.TEXT_CODE);
                escape(value.textValue(), txString, chars, escapedChars);
            }
            else
            {
                // longs, doubles and blobs do not need escaping
                // note: blob string is "B<hex string for bytes>", e.g. B7366abc4
                value.appendTo(txString);
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
                charsRef.ref = new char[getNewSize(charsRef.ref.length, length)];
            }

            final char[] chars = charsRef.ref;
            valueToSend.getChars(0, valueToSend.length(), chars, 0);

            char charAt;
            int last = 0;
            for (int i = 0; i < length; i++)
            {
                charAt = chars[i];
                switch(charAt)
                {
                    case CR:
                        escapedChars[1] = CHAR_r;
                        dest.append(chars, last, i - last, escapedChars, 0, 2);
                        last = i + 1;
                        break;
                    case LF:
                        escapedChars[1] = CHAR_n;
                        dest.append(chars, last, i - last, escapedChars, 0, 2);
                        last = i + 1;
                        break;
                    case CHAR_ESCAPE:
                    case CHAR_TOKEN_DELIM:
                    case CHAR_KEY_VALUE_SEPARATOR:
                    case CHAR_SYMBOL_PREFIX:
                        escapedChars[1] = charAt;
                        dest.append(chars, last, i - last, escapedChars, 0, 2);
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
     * Performs unescaping of the chars from start to end, copying the unescaped chars into the output.
     * <p>
     * This assumes the output has sufficient space to accept the chars between start and end.
     * <p>
     * Note that this is not very efficient as this can only be called AFTER parsing the escaped array to find
     * the length to process. Effectively, this resolves to a double-pass operation over the start to end
     * range when you include the calling code.
     *
     * @return the index in the destination where the unescaped sequence ends
     */
    static int unescape(char[] escaped, int start, int end, char[] output)
    {
        int outPtr = 0;
        for (int i = start; i < end; i++)
        {
            if (escaped[i] == CHAR_ESCAPE)
            {
                i++;
                outPtr = handleEscapeChar(escaped[i], output, outPtr);
            }
            else if (escaped[i] != 0)
            {
                output[outPtr++] = escaped[i];
            }
        }
        return outPtr;
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
     * Creates a string from the chars (already unescaped) from position 0 to end
     */
    static String createString(char[] chars, int end)
    {
        if (end == 1 && chars[0] == NULL_CHAR)
        {
            return null;
        }
        // note: this does an array copy when constructing the string...no way to prevent this
        final String s = new String(chars, 0, end);
        return end < DataFissionProperties.Values.STRING_LENGTH_LIMIT_FOR_TEXT_VALUE_POOL ? s.intern() : s;
    }

    /**
     * Performs decoding of a string key with preamble using already unescaped chars
     */
    static String resolvePooledStringWithPreamble(char[] chars, int end)
    {
        if (end == 1 && chars[0] == NULL_CHAR)
        {
            return null;
        }

        return decodedKeysPool.get(chars, DOUBLE_KEY_PREAMBLE_LENGTH, end - DOUBLE_KEY_PREAMBLE_LENGTH);
    }

    /**
     * Performs decoding of a string key using already unescaped chars with no preamble
     */
    static String resolvePooledStringNoPreamble(char[] chars, int end)
    {
        if (end == 1 && chars[0] == NULL_CHAR)
        {
            return null;
        }

        return decodedKeysPool.get(chars, 0, end);
    }

    /**
     * Performs decoding of an already unescaped value
     */
    static IValue resolveValue(char[] chars, int end)
    {
        if (end == 1 && chars[0] == NULL_CHAR)
        {
            return null;
        }

        return AbstractValue.constructFromCharValue(chars, end);
    }

    static List<String> getNamesFromCommandMessage(char[] decodedMessage)
    {
        // the first token will be the command - we ignore this, e.g. [s, |, o, n, e, |, t, w, o, |, t, h, r, e, e]
        int i = 2;
        int keyPtr = 0;

        final List<String> names = new ArrayList<>();
        final DecodingBuffers decodingBuffers = DECODING_BUFFERS.get();

        if (decodingBuffers.keyArr.length < decodedMessage.length)
        {
            decodingBuffers.keyArr = new char[getNewSize(decodingBuffers.keyArr.length, decodedMessage.length)];
        }

        for (; i < decodedMessage.length; i++)
        {
            if (decodedMessage[i] == CHAR_TOKEN_DELIM)
            {
                names.add((createString(decodingBuffers.keyArr, keyPtr)));
                keyPtr = 0;
            }
            else if (decodedMessage[i] == CHAR_ESCAPE)
            {
                i++;
                keyPtr = handleEscapeChar(decodedMessage[i], decodingBuffers.keyArr, keyPtr);
            }
            else if (decodedMessage[i] != 0)
            {
                decodingBuffers.keyArr[keyPtr++] = decodedMessage[i];
            }
        }
        // process the last one
        if (keyPtr > 0)
        {
            names.add((createString(decodingBuffers.keyArr, keyPtr)));
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
        final DecodingBuffers decodingBuffers = DECODING_BUFFERS.get();
        return decodeAtomicChange(decodedMessage, decodingBuffers);
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
    public char[] decode(ByteBuffer data, CharsetDecoder charsetDecoder)
    {
        try
        {
            return charsetDecoder.decode(this.sessionSyncProtocol.decode(data)).array();
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
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
