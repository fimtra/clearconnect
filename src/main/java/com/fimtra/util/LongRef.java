package com.fimtra.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic reference to a native long. This is useful for passing a long by reference to a method.
 *
 * @author Ramon Servadei
 */
public final class LongRef
{
    final AtomicLong value;

    public LongRef(long value)
    {
        this.value = new AtomicLong(value);
    }

    public long get()
    {
        return value.get();
    }

    public void set(long value)
    {
        this.value.lazySet(value);
    }

    public long incrementAndGet()
    {
        return this.value.incrementAndGet();
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof LongRef))
        {
            return false;
        }

        return value.get() == ((LongRef) o).value.get();
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(value.get());
    }

    @Override
    public String toString()
    {
        return Long.toString(value.get());
    }
}
