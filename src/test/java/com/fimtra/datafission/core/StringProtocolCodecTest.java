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

import static com.fimtra.datafission.core.StringProtocolCodec.CHAR_ESCAPE;
import static com.fimtra.datafission.core.StringProtocolCodec.DECODING_BUFFERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.BlobValue;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.util.StringAppender;
import org.junit.Test;

/**
 * Tests for the {@link StringProtocolCodec}
 *
 * @author Ramon Servadei
 */
public class StringProtocolCodecTest extends CodecBaseTest
{
    String name = "myName";

    CharArrayReference chars = new CharArrayReference(new char[StringProtocolCodec.CHARRAY_SIZE]);

    @Override
    ICodec<?> constructCandidate()
    {
        return new StringProtocolCodec();
    }

    @Test
    public void testEscapeUnescape()
    {
        String value = "some value \\|| with | delimiters \\/ |\\ |/";
        StringAppender sb = new StringAppender();
        StringProtocolCodec.escape(value, sb, this.chars, prepareEscapeCharArr());
        assertEquals(value, unescape(sb.toString()));
    }

    private static char[] prepareEscapeCharArr()
    {
        final char[] cs = new char[2];
        cs[0] = CHAR_ESCAPE;
        return cs;
    }

    @Test
    public void testUnmappedChars() throws CharacterCodingException
    {
        final String stringWithUnmapped =
                "bad number:" + new DecimalFormat("0.0#######").format(Double.NaN) + " something";
        final Charset defaultCharset = Charset.defaultCharset();
        StringProtocolCodec.ENCODING_BUFFERS.get().getEncoder(
                defaultCharset == StandardCharsets.UTF_8 ? StandardCharsets.ISO_8859_1 :
                        StandardCharsets.UTF_8).encode(CharBuffer.wrap(stringWithUnmapped));
    }

    @Test
    public void testEscapeUnescapeLength()
    {
        String value = "||||||||";
        StringAppender sb = new StringAppender();
        StringProtocolCodec.escape(value, sb, this.chars, prepareEscapeCharArr());
        assertEquals(value, unescape(sb.toString()));
    }

    @Test
    public void testEscapeUnescapeEndingInSpecialChar()
    {
        String value = "special char ending \\";
        StringAppender sb = new StringAppender();
        StringProtocolCodec.escape(value, sb, this.chars, prepareEscapeCharArr());
        assertEquals(value, unescape(sb.toString()));
    }

    @Test
    public void testEscapeUnescapeCRLF()
    {
        String value = "some value \\|| with \r\n | delimiters \\/ |\\ |/";
        StringAppender sb = new StringAppender();
        StringProtocolCodec.escape(value, sb, this.chars, prepareEscapeCharArr());
        String escaped = sb.toString();
        assertFalse(escaped.contains("\r"));
        assertFalse(escaped.contains("\n"));
        assertEquals(value, unescape(escaped));
    }

    private static String unescape(String escaped)
    {
        final char[] decodedMessage = escaped.toCharArray();
        final char[] dataArr = new char[decodedMessage.length];
        return StringProtocolCodec.createString(dataArr,
                StringProtocolCodec.unescape(decodedMessage, 0, decodedMessage.length, dataArr));
    }


    @Test
    public void testStringWithEscapedCRLF()
    {
        String value = "some value \\|| with \\r\\n | delimiters \\/ |\\ |/";
        StringAppender sb = new StringAppender();
        StringProtocolCodec.escape(value, sb, this.chars, prepareEscapeCharArr());
        assertEquals(value, unescape(sb.toString()));
    }

    @Test
    public void testGetCommandMessageForRecordNames()
    {
        String[] args = new String[] { "one", "two", "three", "|\\|\\||special", "$%£" };
        List<String> result =
            StringProtocolCodec.getNamesFromCommandMessage(StringProtocolCodec.getEncodedNamesForCommandMessage(
                StringProtocolCodec.SUBSCRIBE_COMMAND, args).toCharArray());
        assertEquals(Arrays.toString(args), Arrays.toString(result.toArray(new String[result.size()])));
    }

    @Test
    public void testGetCommandMessageForRecordNames_LARGE()
    {
        String[] args = new String[] { "one", "two",
                "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" };
        List<String> result = StringProtocolCodec.getNamesFromCommandMessage(
                StringProtocolCodec.getEncodedNamesForCommandMessage(StringProtocolCodec.SUBSCRIBE_COMMAND,
                                args)
                        .toCharArray());
        assertEquals(Arrays.toString(args), Arrays.toString(result.toArray(new String[result.size()])));
    }

    @Test
    public void testEncodeDecodeAtomicChange_emptyImage()
    {
        AtomicChange change = new AtomicChange("empty");
        change.setScope(IRecordChange.IMAGE_SCOPE_CHAR);
        change.setSequence(0);

        final IRecordChange result = getRecordChange(change);

        assertEquals(IRecordChange.IMAGE_SCOPE_CHAR, result.getScope());
        assertEquals(0, result.getSequence());
    }

    @Test
    public void testEncodeDecodeAtomicChange_singleTextValue()
    {
        doSingleValueTest("k1", TextValue.valueOf("val1"));
    }

    @Test
    public void testEncodeDecodeAtomicChange_singleTextValue_specialChars()
    {
        doSingleValueTest("k1=", TextValue.valueOf("val1"));
        doSingleValueTest("k1\\=", TextValue.valueOf("val1"));
        doSingleValueTest("k1\\", TextValue.valueOf("val1"));
    }

    @Test
    public void testEncodeDecodeAtomicChange_singleBlobValue()
    {
        doSingleValueTest("k1", BlobValue.valueOf("val1".getBytes()));
    }

    @Test
    public void testEncodeDecodeAtomicChange_singleBlobValue_LARGE()
    {
        doSingleValueTest("k1", BlobValue.valueOf(
                ("012345678901234567890123456789012345678901234567890123456789"
                        + "01234567890123456789012345678901234567890123456789"
                        + "01234567890123456789012345678901234567890123456789"
                        + "0123456789012345678901234567890123456789").getBytes()));
    }

    private void doSingleValueTest(String k1, IValue v1)
    {
        AtomicChange change = new AtomicChange("chg");
        change.mergeEntryUpdatedChange(k1, v1, null);

        final IRecordChange result = getRecordChange(change);

        Context c = new Context("test");
        IRecord rec1 = c.getOrCreateRecord("rec1");
        IRecord rec2 = c.getOrCreateRecord("rec2");

        change.applyTo(rec1);
        result.applyTo(rec2);
        System.err.println(rec2);
        assertEquals(1, rec2.size());
        assertEquals(rec1.asFlattenedMap(), rec2.asFlattenedMap());
    }

    private IRecordChange getRecordChange(AtomicChange change)
    {
        byte[] txMessageForChange =
                this.candidate.finalEncode(this.candidate.getTxMessageForAtomicChange(change));
        return this.candidate.getAtomicChangeFromRxMessage(ByteBuffer.wrap(txMessageForChange));
    }

    @Test
    public void testEncodeDecodeAtomicChange()
    {
        final String k1 = "one$£";
        final String k2 = "two$£";
        final String k3 = "three$£";
        final String k4 = "four$£";
        final LongValue v1 = LongValue.valueOf(1);
        final DoubleValue v2 = DoubleValue.valueOf(2);
        final BlobValue v3 = BlobValue.valueOf("$£some value \\|| with \\r\\n | delimiters \\/ |\\ |/".getBytes());
        final TextValue v4 = TextValue.valueOf("$£23456789-10-0123456789-20-$£23456789-30-0123456789-40-0123456789");

        AtomicChange change = new AtomicChange("change-$£");
        change.mergeEntryUpdatedChange(k1, v1, null);
        change.mergeEntryUpdatedChange(k2, v2, null);
        change.mergeEntryRemovedChange(k3, v3);
        change.mergeEntryRemovedChange(k4, v4);

        final IRecordChange result = getRecordChange(change);

        Map<String, IValue> map1 = new HashMap<>();
        map1.put(k1, v3);
        map1.put(k3, v3);
        map1.put(k4, v4);

        Context c = new Context("test");
        IRecord rec1 = c.getOrCreateRecord("rec1");
        rec1.putAll(map1);

        IRecord rec2 = c.getOrCreateRecord("rec2");
        rec2.putAll(map1);

        change.applyTo(rec1);
        result.applyTo(rec2);
        assertEquals(rec1.asFlattenedMap(), rec2.asFlattenedMap());
        assertFalse(rec1.containsKey(k3));
    }

    @Test
    public void testEncodeDecodeAtomicChange_withSubmapSpecialCharInName()
    {
        final String k1 = "k1";
        final LongValue v1 = LongValue.valueOf(1);

        AtomicChange change = new AtomicChange("change1");
        change.mergeEntryUpdatedChange(k1, v1, null);
        final String subMapKey = "subMap1=1";
        change.mergeSubMapEntryUpdatedChange(subMapKey, k1, v1, null);

        final IRecordChange result = getRecordChange(change);

        Context c = new Context("test");
        IRecord rec1 = c.getOrCreateRecord("rec1");
        IRecord rec2 = c.getOrCreateRecord("rec2");

        change.applyCompleteAtomicChangeToRecord(rec1);
        result.applyCompleteAtomicChangeToRecord(rec2);
        System.err.println(rec2);
        assertEquals(rec1.asFlattenedMap(), rec2.asFlattenedMap());
        assertEquals(v1, rec1.getOrCreateSubMap(subMapKey).get(k1));
        assertEquals(v1, rec2.getOrCreateSubMap(subMapKey).get(k1));
    }

    @Test
    public void testEncodeDecodeAtomicChange_withSubmap()
    {
        final String k1 = "one$£";
        final String k2 = "two$£";
        final String k3 = "three$£";
        final String k4 = "four$£";
        final LongValue v1 = LongValue.valueOf(1);
        final DoubleValue v2 = DoubleValue.valueOf(2);
        final BlobValue v3 = BlobValue.valueOf("$£some value \\|| with \\r\\n | delimiters \\/ |\\ |/".getBytes());
        final TextValue v4 = TextValue.valueOf("$£23456789-10-0123456789-20-$£23456789-30-0123456789-40-0123456789");

        AtomicChange change = new AtomicChange("change-$£");
        change.mergeEntryUpdatedChange(k1, v1, null);
        change.mergeEntryUpdatedChange(k2, v2, null);
        change.mergeEntryRemovedChange(k3, v3);
        change.mergeEntryRemovedChange(k4, v4);
        final String subMapKey = "subMap1-£$";
        change.mergeSubMapEntryUpdatedChange(subMapKey, k1, v1, null);
        change.mergeSubMapEntryUpdatedChange(subMapKey, k2, v2, null);
        change.mergeSubMapEntryRemovedChange(subMapKey, k3, v3);
        change.mergeSubMapEntryRemovedChange(subMapKey, k4, v4);

        final IRecordChange result = getRecordChange(change);

        Map<String, IValue> map1 = new HashMap<>();
        map1.put(k1, v3);
        map1.put(k3, v3);
        map1.put(k4, v4);

        Context c = new Context("test");
        IRecord rec1 = c.getOrCreateRecord("rec1");
        rec1.putAll(map1);
        rec1.getOrCreateSubMap(subMapKey).putAll(map1);

        IRecord rec2 = c.getOrCreateRecord("rec2");
        rec2.putAll(map1);
        rec2.getOrCreateSubMap(subMapKey).putAll(map1);

        change.applyCompleteAtomicChangeToRecord(rec1);
        result.applyCompleteAtomicChangeToRecord(rec2);
        assertEquals(rec1.asFlattenedMap(), rec2.asFlattenedMap());
        assertFalse(rec1.containsKey(k3));
        assertEquals(v2, rec1.getOrCreateSubMap(subMapKey).get(k2));
        assertEquals(v2, rec2.getOrCreateSubMap(subMapKey).get(k2));
    }

    @Test
    public void testFragmentedAtomicChange()
    {
        final String encoded = "|record|d123|:|submap1|p|k1=L0|:|submap2";
        final IRecordChange change =
                StringProtocolCodec.decodeAtomicChange(encoded.toCharArray(), DECODING_BUFFERS.get());
        assertEquals(
                "AtomicChange [name=record, d123 subMapAtomicChanges={submap1=AtomicChange [name=submap1, d123, putEntries={=L0}]}]",
                change.toString());
    }

    @Test
    public void testEncodeDecodeAtomicChange_preamble()
    {
        final String k1 = "one";
        final String k2 = "two";
        final String k3 = "three";
        final String k4 = "four";
        final LongValue v1 = LongValue.valueOf(1);
        final DoubleValue v2 = DoubleValue.valueOf(2);
        final BlobValue v3 =
            BlobValue.valueOf("0123456789-10-0123456789-20-0123456789-30-0123456789-40-0123456789".getBytes());
        final TextValue v4 = TextValue.valueOf("0123456789-10-0123456789-20-0123456789-30-0123456789-40-0123456789");

        AtomicChange change = new AtomicChange("change");
        change.mergeEntryUpdatedChange(k1, v1, null);
        change.mergeEntryUpdatedChange(k2, v2, null);
        change.mergeEntryRemovedChange(k3, v3);
        change.mergeEntryRemovedChange(k4, v4);
        final String subMapKey = "subMap1";
        change.mergeSubMapEntryUpdatedChange(subMapKey, k1, v1, null);
        change.mergeSubMapEntryUpdatedChange(subMapKey, k2, v2, null);
        change.mergeSubMapEntryRemovedChange(subMapKey, k3, v3);
        change.mergeSubMapEntryRemovedChange(subMapKey, k4, v4);

        final StringProtocolCodec codec = new StringProtocolCodec();
        IRecordChange result =
            StringProtocolCodec.decodeAtomicChange(new String(StringProtocolCodec.encodeAtomicChange(
                StringProtocolCodec.RPC_COMMAND_CHARS, change, codec.getCharset(), codec.getEncodedBytesHandler())).toCharArray(), DECODING_BUFFERS.get());

        Map<String, IValue> map1 = new HashMap<>();
        map1.put(k1, v3);
        map1.put(k3, v3);
        map1.put(k4, v4);

        Context c = new Context("test");
        IRecord rec1 = c.getOrCreateRecord("rec1");
        rec1.putAll(map1);
        rec1.getOrCreateSubMap(subMapKey).putAll(map1);

        IRecord rec2 = c.getOrCreateRecord("rec2");
        rec2.putAll(map1);
        rec2.getOrCreateSubMap(subMapKey).putAll(map1);

        change.applyCompleteAtomicChangeToRecord(rec1);
        result.applyCompleteAtomicChangeToRecord(rec2);
        assertEquals(rec1.asFlattenedMap(), rec2.asFlattenedMap());
        assertFalse(rec1.containsKey(k3));
        assertEquals(v2, rec1.getOrCreateSubMap(subMapKey).get(k2));
        assertEquals(v2, rec2.getOrCreateSubMap(subMapKey).get(k2));
    }

    @Test
    public void testEncodeDecodeAtomicChange_preamble_noArgs()
    {
        byte[] txMessageForRpc =
                this.candidate.finalEncode(this.candidate.getTxMessageForRpc("rpcException", new IValue[0],
                "_RPC_rpcException:304189752:1405541086282:1"));
        IRecordChange atomicChangeFromRxMessage = this.candidate.getAtomicChangeFromRxMessage(ByteBuffer.wrap(txMessageForRpc));
        System.err.println(atomicChangeFromRxMessage);
        assertEquals("_RPC_rpcException:304189752:1405541086282:1",
            atomicChangeFromRxMessage.getPutEntries().get(RpcInstance.Remote.RESULT_RECORD_NAME).textValue());
    }

    @Test
    public void testPerformanceOfIndexAndBulkBuffer()
    {
        StringAppender sb = new StringAppender();
        String valueToSend = "0123456789";

        int MAX = 10000;
        for (int i = 0; i < MAX; i++)
        {
            dobulk(sb, valueToSend);
            doswitch(sb, valueToSend);
        }

        long indexThenBulkAdd = 0;
        long bulkStart = System.nanoTime();
        for (int i = 0; i < MAX; i++)
        {
            dobulk(sb, valueToSend);
        }
        System.err.println("Bulk=" + (indexThenBulkAdd += (System.nanoTime() - bulkStart)));

        long scanAndSwitch = 0;
        long swStart = System.nanoTime();
        for (int i = 0; i < MAX; i++)
        {
            doswitch(sb, valueToSend);
        }
        System.err.println("Scan=" + (scanAndSwitch += (System.nanoTime() - swStart)));

        bulkStart = System.nanoTime();
        for (int i = 0; i < MAX; i++)
        {
            dobulk(sb, valueToSend);
        }
        System.err.println("Bulk=" + (indexThenBulkAdd += (System.nanoTime() - bulkStart)));

        swStart = System.nanoTime();
        for (int i = 0; i < MAX; i++)
        {
            doswitch(sb, valueToSend);
        }
        System.err.println("Scan=" + (scanAndSwitch += (System.nanoTime() - swStart)));
    }

    void doswitch(StringAppender sb, String valueToSend)
    {
        sb.setLength(0);
        final char[] chars = valueToSend.toCharArray();
        final int length = chars.length;
        char charAt;
        int last = 0;
        for (int i = 0; i < length; i++)
        {
            charAt = chars[i];
            switch(charAt)
            {
                case '\r':
                    sb.append(chars, last, i - last);
                    sb.append('\\');
                    sb.append('r');
                    last = i + 1;
                    break;
                case '\n':
                    sb.append(chars, last, i - last);
                    sb.append('\\');
                    sb.append('n');
                    last = i + 1;
                    break;
                case '\\':
                case '|':
                case '=':
                    sb.append(chars, last, i - last);
                    sb.append('\\');
                    sb.append(charAt);
                    last = i + 1;
                    break;
                default :
            }
        }
        sb.append(chars, last, length - last);
    }

    void dobulk(StringAppender sb, String valueToSend)
    {
        sb.setLength(0);
        if ((valueToSend.indexOf('\r', 0) == -1) && (valueToSend.indexOf('\n', 0) == -1)
            && (valueToSend.indexOf('\\', 0) == -1) && (valueToSend.indexOf('|', 0) == -1)
            && (valueToSend.indexOf('=', 0) == -1)

        )
        {
            sb.append(valueToSend);
        }
    }
}