/*
 * Copyright (c) 2013 Ramon Servadei, Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform;

import com.fimtra.datafission.ICodec;

/**
 * Enumerates the wire protocol variants
 * 
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public enum WireProtocolEnum
{
    STRING("com.fimtra.datafission.core.StringProtocolCodec"), GZIP("com.fimtra.datafission.core.GZipProtocolCodec"),
        HYBRID("com.fimtra.datafission.core.HybridProtocolCodec");

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
        return (ICodec<T>) this.codec;
    }
}
