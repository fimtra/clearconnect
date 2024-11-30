package com.fimtra.util;

import java.io.IOException;
import java.io.Writer;

/**
 * Copied from java StringWriter - replaced internal StringBuffer with StringAppender
 *
 * @author Ramon Servadei
 */
public class StringAppenderWriter extends Writer
{
    private final StringAppender buf;

    public StringAppenderWriter()
    {
        this(16);
    }

    public StringAppenderWriter(int initialSize)
    {
        if (initialSize < 0)
        {
            throw new IllegalArgumentException("Negative buffer size");
        }
        buf = new StringAppender(initialSize);
    }

    public void write(int c)
    {
        buf.append((char) c);
    }

    public void write(char cbuf[], int off, int len)
    {
        if ((off < 0) || (off > cbuf.length) || (len < 0) || ((off + len) > cbuf.length) || ((off + len) < 0))
        {
            throw new IndexOutOfBoundsException();
        }
        else if (len == 0)
        {
            return;
        }
        buf.append(cbuf, off, len);
    }

    public void write(String str)
    {
        buf.append(str);
    }

    public void write(String str, int off, int len)
    {
        buf.append(str.substring(off, off + len));
    }

    public StringAppenderWriter append(CharSequence csq)
    {
        if (csq == null)
        {
            write("null");
        }
        else
        {
            write(csq.toString());
        }
        return this;
    }

    public StringAppenderWriter append(CharSequence csq, int start, int end)
    {
        CharSequence cs = (csq == null ? "null" : csq);
        write(cs.subSequence(start, end)
                .toString());
        return this;
    }

    public StringAppenderWriter append(char c)
    {
        write(c);
        return this;
    }

    public String toString()
    {
        return buf.toString();
    }

    public void flush()
    {
    }

    public void close() throws IOException
    {
    }
}
