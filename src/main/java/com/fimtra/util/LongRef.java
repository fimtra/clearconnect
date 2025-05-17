package com.fimtra.util;

/**
 * Basic reference to a native long. This is useful for passing a long by reference to a method.
 *
 * @author Ramon Servadei
 */
public final class LongRef
{
    volatile long value;

    public LongRef(long value)
    {
        this.value = value;
    }

    public long get()
    {
        return value;
    }

    public void set(long value)
    {
        this.value = value;
    }

    public long incrementAndGet()
    {
        final long v = value + 1L;
        value = v;
        return v;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof LongRef))
        {
            return false;
        }

        LongRef longRef = (LongRef) o;
        return value == longRef.value;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(value);
    }

    @Override
    public String toString()
    {
        return Long.toString(value);
    }
}
