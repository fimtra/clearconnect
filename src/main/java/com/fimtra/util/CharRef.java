package com.fimtra.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic reference to a native char. This is useful for passing a char by reference to a method.
 *
 * @author Ramon Servadei
 */
public final class CharRef
{
    final AtomicInteger value;

    public CharRef(char value)
    {
        this.value = new AtomicInteger(value);
    }

    public char get()
    {
        return (char) value.get();
    }

    public void set(char value)
    {
        this.value.lazySet(value);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof CharRef))
        {
            return false;
        }

        return value.get() == ((CharRef) o).value.get();
    }

    @Override
    public int hashCode()
    {
        return value.get();
    }

    @Override
    public String toString()
    {
        return Character.toString(get());
    }
}
