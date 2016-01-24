/*
 * Copyright (c) 2013 Ramon Servadei, Paul Mackinlay, Fimtra
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
package com.fimtra.clearconnect;

import com.fimtra.datafission.ICodec;

/**
 * Enumerates the wire protocol variants
 * 
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public enum WireProtocolEnum
{
    STRING("com.fimtra.datafission.core.StringProtocolCodec"), 
    GZIP("com.fimtra.datafission.core.GZipProtocolCodec"),
    // HYBRID("com.fimtra.datafission.core.StringSymbolProtocolCodec"),
    ;

    private final ICodec<?> codec;

    private WireProtocolEnum(String codecClassName)
    {
        try
        {
            this.codec = (ICodec<?>) Class.forName(codecClassName).newInstance();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not construct " + this, e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> ICodec<T> getCodec()
    {
        // note: codecs are not necessarily stateless so return a new instance each time
        return (ICodec<T>) this.codec.newInstance();
    }
}
