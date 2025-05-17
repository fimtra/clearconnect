package com.fimtra.util;

/**
 * Basic reference to a native char. This is useful for passing a char by reference to a method.
 *
 * @author Ramon Servadei
 */
public final class CharRef
{
    volatile char value;

    public CharRef(char value)
    {
        this.value = value;
    }

    public char get()
    {
        return value;
    }

    public void set(char value)
    {
        this.value = value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof CharRef))
        {
            return false;
        }

        CharRef charRef = (CharRef) o;
        return value == charRef.value;
    }

    @Override
    public int hashCode()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return Character.toString(value);
    }
}
