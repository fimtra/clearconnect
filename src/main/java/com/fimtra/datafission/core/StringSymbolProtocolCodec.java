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

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecordChange;

/**
 * A codec for messages that are sent between a {@link Publisher} and {@link ProxyContext} using a
 * string text protocol with a symbol table for string substitution of record names and keys in
 * atomic changes. This reduces the transmission size of messages once the symbol mapping for a
 * record name or key is sent.
 * <p>
 * The format of the string in ABNF notation:
 * 
 * <pre>
 *  preamble "|" symbol-element seq [puts] [removes] [sub-map]
 *  
 *  preamble = 0*ALPHA
 *  symbol-element = ( symbol-definition | symbol-instance )
 *  symbol-definition = "n" string symbol-instance ; if the symbol for the record has never been sent
 *  symbol-instance = "~" symbol
 *  string = 1*ALPHA ; the string that will be substituted with the symbol
 *  symbol = 1*ALPHA ; the symbol to substitute for the string
 *  seq = "|" scope seq_num
 *  scope = "i" | "d" ; identifies either an image or delta
 *  seq_num = 1*DIGIT ; the sequency number
 *  puts = "|p" 1*key-value-pair
 *  removes = "|r" 1*key-value-pair
 *  sub-map = "|:|" name [puts] [removes]
 *  key-value-pair = "|" symbol-element "=" value   
 *  value = 1*OCTET
 *  
 *  e.g. |nmyrecord~s!|d322234|p|nkey1~#=value1|~df=value2|r|~$=value5|:|~$|p|~ty=value1
 * </pre>
 * 
 * @author Ramon Servadei
 * @deprecated Will be removed in version 4.0.0
 */
@Deprecated
public final class StringSymbolProtocolCodec extends StringProtocolCodec
{
    public StringSymbolProtocolCodec()
    {
    }

    @Override
    public byte[] getTxMessageForAtomicChange(IRecordChange atomicChange)
    {
        throw new UnsupportedOperationException("Not supported anymore");
    }

    @Override
    public IRecordChange getAtomicChangeFromRxMessage(ByteBuffer data)
    {
        throw new UnsupportedOperationException("Not supported anymore");
    }

    @Override
    public ICodec<char[]> newInstance()
    {
        throw new UnsupportedOperationException("Not supported anymore");
    }

    @Override
    public Charset getCharset()
    {
        throw new UnsupportedOperationException("Not supported anymore");
    }
}