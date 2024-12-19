package com.fimtra.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the StringAppenderWriter
 *
 * @author Ramon Servadei
 */
public class StringAppenderWriterTest
{
    StringAppenderWriter candidate;

    @Before
    public void setUp() throws Exception
    {
        candidate = new StringAppenderWriter();
    }

    @Test
    public void testWorks() throws IOException
    {
        candidate.append("one_two").append('_').append("__three", 2, 5);
        candidate.write('_');
        candidate.write("four");
        candidate.write(new char[]{'_', 'f','i','v','e'});
        candidate.write(new char[]{'_', '_','s','i','x'},1,4);
        candidate.write("__seven",1,6);
        final String result = candidate.toString();
        assertEquals("one_two_thr_four_five_six_seven", result);
    }

    @Test
    public void test_nulls() throws IOException
    {
        candidate.write((String)null);
        candidate.append(null);
        final String result = candidate.toString();
        assertEquals("nullnull", result);
    }
}