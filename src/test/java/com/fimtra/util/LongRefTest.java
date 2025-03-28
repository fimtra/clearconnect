package com.fimtra.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Co-pilot generated test class for the class {@link LongRef}
 */
public class LongRefTest
{
    @Test
    public void testConstructorAndGet()
    {
        LongRef ref = new LongRef(123L);
        assertEquals(123L, ref.get());
    }

    @Test
    public void testSet()
    {
        LongRef ref = new LongRef(123L);
        ref.set(456L);
        assertEquals(456L, ref.get());
    }

    @Test
    public void testEquals()
    {
        LongRef ref1 = new LongRef(100L);
        LongRef ref2 = new LongRef(100L);
        LongRef ref3 = new LongRef(200L);

        assertTrue(ref1.equals(ref2));
        assertFalse(ref1.equals(ref3));
        assertFalse(ref1.equals(null));
        assertFalse(ref1.equals(100L));
    }

    @Test
    public void testHashCode()
    {
        LongRef ref = new LongRef(999L);
        assertEquals(Long.hashCode(999L), ref.hashCode());
    }

    @Test
    public void testToString()
    {
        LongRef ref = new LongRef(789L);
        assertEquals("789", ref.toString());
    }
}