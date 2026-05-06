package com.fimtra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Co-pilot generated test class for the class {@link LongRef}
 */
public class LongRefTest
{
    @Test
    public void testIncrementAndGet()
    {
        final long expectedValue = 0;
        final LongRef ref = new LongRef(expectedValue);

        // Initial value check
        assertEquals(expectedValue, ref.get());

        // Increment and get
        ref.incrementAndGet();
        assertTrue(ref.get() == (expectedValue + 1));

        ref.set(0);
        // Multiple increments and checks
        for (int i = 0; i < 10; i++)
        {
            ref.incrementAndGet();
            assertEquals(i + 1, ref.get());
        }
    }

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