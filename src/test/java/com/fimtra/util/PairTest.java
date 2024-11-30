package com.fimtra.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * @author Ramon Servadei
 */
public class PairTest
{
    Pair candidate;

    @Test
    public void testLazyHashCode()
    {
        candidate = new Pair(1, 2);
        assertEquals(1, candidate.getFirst());
        assertEquals(2, candidate.getSecond());
        assertEquals(-1, candidate.hashCode);
        final int hashCode = candidate.hashCode();
        assertEquals(hashCode, candidate.hashCode);
    }
}