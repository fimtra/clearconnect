/*
 * Copyright (c) 2013 Ramon Servadei 
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.util.FastDateFormat;

/**
 * Tests the {@link FastDateFormat}
 * 
 * @author Ramon Servadei
 */
public class FastDateFormatTest
{
    FastDateFormat candidate;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new FastDateFormat();
    }

    @After
    public void tearDown() throws Exception
    {
    }

    void test(Calendar c, int year, int month, int day, int hours, int mins, int sec, int millis)
    {
        // -1 to the month as we test with Jan=1
        c.set(year, month - 1, day, hours, mins, sec);
        c.set(Calendar.MILLISECOND, millis);
        String result = this.candidate.yyyyMMddHHmmssSSS(c.getTimeInMillis());
        String expected = FastDateFormat.formatDateTime(year * 10000 + month * 100 + day, hours, mins, sec, millis);
        assertEquals(expected, result);
        System.err.println(result);
    }

    @Test
    public void testFormats()
    {
        Calendar c = new GregorianCalendar();
        // set to 2012 Dec 15 @ 05:02:01
        test(c, 2012, 12, 15, 5, 2, 1, 1);
        // test millisecond changes
        test(c, 2012, 12, 15, 5, 2, 1, 297);
        test(c, 2012, 12, 15, 5, 2, 1, 999);
        // test a seconds change but this is only 2 milils later change
        test(c, 2012, 12, 15, 5, 2, 2, 1);
        test(c, 2012, 12, 15, 5, 2, 2, 999);
        test(c, 2012, 12, 15, 5, 2, 4, 998);
        // test a minute change
        test(c, 2012, 12, 15, 5, 4, 59, 998);
        test(c, 2012, 12, 15, 5, 5, 0, 998);
        test(c, 2012, 12, 15, 5, 6, 0, 998);
        test(c, 2012, 12, 15, 5, 6, 1, 998);
        test(c, 2012, 12, 15, 5, 8, 0, 998);
        test(c, 2012, 12, 15, 5, 20, 4, 998);
        // test over an hour change
        test(c, 2012, 12, 15, 7, 20, 59, 998);
        test(c, 2012, 12, 15, 8, 20, 1, 998);
        test(c, 2012, 12, 15, 9, 20, 4, 998);
        test(c, 2012, 12, 15, 10, 25, 4, 998);

        // test differences for 1 millisecond that push into the next day
        test(c, 2012, 12, 15, 23, 59, 59, 998);
        test(c, 2012, 12, 15, 23, 59, 59, 999);
        test(c, 2012, 12, 16, 0, 0, 0, 0);
    }

    @Test
    public void testFor24HrsIncrementingBy500ms()
    {
        Calendar c = new GregorianCalendar();
        c.set(2012, 12, 15, 5, 2, 1);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-HH:mm:ss:SSS");

        // test for a full 24hrs ticking at 1/2 second
        int testCount = ((24 * 60 * 60 * 2) + 1);
        final long calTime = c.getTimeInMillis();
        Date date = null;
        for (int i = 0; i < testCount; i++)
        {
            date = new Date(calTime + (i * 500) + i);
            String sdf = format.format(date);
            String fdf = this.candidate.yyyyMMddHHmmssSSS(date.getTime());
            assertEquals("at " + i + ", for " + c.getTime() + ", millis=" + c.getTimeInMillis(), sdf, fdf);
        }
    }

    @Test
    public void testFor24HrsIncrementingBy30secs()
    {
        Calendar c = new GregorianCalendar();
        c.set(2012, 12, 15, 5, 2, 1);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-HH:mm:ss:SSS");

        // test for 24hrs+, increments in 30 secs 800ms and add a millisecond for each loop
        int testCount = ((24 * 60 * 60) + 1);
        final long calTime = c.getTimeInMillis();
        Date date = null;
        for (int i = 0; i < testCount; i++)
        {
            date = new Date(calTime + (i * 30000) + 800 + i);
            String sdf = format.format(date);
            String fdf = this.candidate.yyyyMMddHHmmssSSS(date.getTime());
            assertEquals("at " + i + ", for " + c.getTime() + ", millis=" + c.getTimeInMillis(), sdf, fdf);
        }
    }

    @Test
    public void testFor24HrsIncrementingBy30mins()
    {
        Calendar c = new GregorianCalendar();
        c.set(2012, 12, 15, 5, 2, 1);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-HH:mm:ss:SSS");

        // test for 24hrs+, increments in 30 mins
        int testCount = ((24 * 60 * 60) + 1);
        final long calTime = c.getTimeInMillis();
        Date date = null;
        for (int i = 0; i < testCount; i++)
        {
            date = new Date(calTime + (i * 60000 * 30) + 997 + i);
            String sdf = format.format(date);
            String fdf = this.candidate.yyyyMMddHHmmssSSS(date.getTime());
            assertEquals("at " + i + ", for " + c.getTime() + ", millis=" + c.getTimeInMillis(), sdf, fdf);
        }
    }

    @Test
    public void testPerformanceAgainstSimpleDateFormat()
    {
        Calendar c = new GregorianCalendar();
        c.set(2012, 12, 15, 5, 2, 1);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-HH:mm:ss:SSS");

        // perf test, ticking per millis
        int testCount = 100000;
        final long calTime = c.getTimeInMillis();
        Date date = null;
        long now = System.currentTimeMillis();
        for (int i = 0; i < testCount; i++)
        {
            date = new Date(calTime + i);
            format.format(date);
        }
        long sdfTime = System.currentTimeMillis() - now;
        now = System.currentTimeMillis();

        for (int i = 0; i < testCount; i++)
        {
            date = new Date(calTime + i);
            this.candidate.yyyyMMddHHmmssSSS(date.getTime());
        }
        long fdfTime = System.currentTimeMillis() - now;
        now = System.currentTimeMillis();
        System.err.println("SimpleDateFormat took " + sdfTime + ", FastDateFormat took " + fdfTime);
        assertTrue(fdfTime < sdfTime);
    }

    static Calendar cal = new GregorianCalendar();

    private static String getDateTime(long millis)
    {
        synchronized (cal)
        {
            cal.setTimeInMillis(millis);
            int yyyyMMdd =
                cal.get(Calendar.YEAR) * 10000 + (1 + cal.get(Calendar.MONTH)) * 100 + cal.get(Calendar.DAY_OF_MONTH);
            return FastDateFormat.formatDateTime(yyyyMMdd, cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE),
                cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND));
        }
    }

    @Test
    public void testPerformanceAgainstCalendarWithFormatting()
    {
        Calendar c = new GregorianCalendar();
        c.set(2012, 12, 15, 5, 2, 1);

        // perf test, ticking per millis
        int testCount = 100000;
        final long calTime = c.getTimeInMillis();
        Date date = null;
        long now = System.currentTimeMillis();
        for (int i = 0; i < testCount; i++)
        {
            getDateTime(calTime + i);
        }
        long sdfTime = System.currentTimeMillis() - now;
        now = System.currentTimeMillis();

        for (int i = 0; i < testCount; i++)
        {
            date = new Date(calTime + i);
            this.candidate.yyyyMMddHHmmssSSS(date.getTime());
        }
        long fdfTime = System.currentTimeMillis() - now;
        now = System.currentTimeMillis();
        System.err.println("Calendar with formatting took " + sdfTime + ", FastDateFormat took " + fdfTime);
        assertTrue(fdfTime < sdfTime);
    }

}
