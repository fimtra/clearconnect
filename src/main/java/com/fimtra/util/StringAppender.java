/*
 * Copyright (c) 2019 Ramon Servadei
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
package com.fimtra.util;

import java.nio.CharBuffer;

/**
 * Cut-down version of a {@link StringBuilder} that provides direct access to the backing char[]
 *
 * @author Ramon Servadei
 */
public final class StringAppender
{
    char[] chars;
    int len;

    public StringAppender()
    {
        this(16);
    }

    public StringAppender(int len)
    {
        this.chars = new char[len];
    }

    public void setLength(int len)
    {
        if (len < 0)
        {
            throw new IllegalArgumentException("Negative length not allowed: " + len);
        }
        this.len = len;
    }

    public StringAppender append(long v)
    {
        return append(Long.toString(v));
    }

    public StringAppender append(double v)
    {
        return append(Double.toString(v));
    }

    public StringAppender append(char v)
    {
        resize(1);
        this.chars[this.len++] = v;
        return this;
    }

    public StringAppender append(char[] v)
    {
        final int length = v.length;
        resize(length);
        System.arraycopy(v, 0, this.chars, this.len, length);
        this.len += length;
        return this;
    }

    public StringAppender append(char[] v, int offset, int len)
    {
        if (len < 0)
        {
            throw new IllegalArgumentException("Negative length not allowed: " + len);
        }
        resize(len);
        System.arraycopy(v, offset, this.chars, this.len, len);
        this.len += len;
        return this;
    }

    public StringAppender append(String v)
    {
        if (v == null)
        {
            return append("null");
        }
        final int length = v.length();
        resize(length);
        v.getChars(0, length, this.chars, this.len);
        this.len += length;
        return this;
    }

    private void resize(int length)
    {
        if (this.len + length > this.chars.length)
        {
            final char[] _c = new char[this.chars.length + (length < 9 ? 16 : (length * 2))];
            System.arraycopy(this.chars, 0, _c, 0, this.len);
            this.chars = _c;
        }
    }

    public CharBuffer getCharBuffer()
    {
        return CharBuffer.wrap(this.chars, 0, this.len);
    }

    @Override
    public String toString()
    {
        return new String(this.chars, 0, this.len);
    }
}
