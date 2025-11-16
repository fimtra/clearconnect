package com.fimtra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Co-pilot generated test class for the class {@link CharRef}
 */
public class CharRefTest
{

    @Test
    public void testConstructorAndGet()
    {
        CharRef ref = new CharRef('a');
        assertEquals('a', ref.get());
    }

    @Test
    public void testSet()
    {
        CharRef ref = new CharRef('a');
        ref.set('b');
        assertEquals('b', ref.get());
    }

    @Test
    public void testEquals()
    {
        CharRef ref1 = new CharRef('x');
        CharRef ref2 = new CharRef('x');
        CharRef ref3 = new CharRef('y');

        assertTrue(ref1.equals(ref2));
        assertFalse(ref1.equals(ref3));
        assertFalse(ref1.equals(null));
        assertFalse(ref1.equals("x"));
    }

    @Test
    public void testHashCode()
    {
        CharRef ref = new CharRef('z');
        assertEquals('z', ref.hashCode());
    }

    @Test
    public void testToString()
    {
        CharRef ref = new CharRef('c');
        assertEquals("c", ref.toString());
    }
}