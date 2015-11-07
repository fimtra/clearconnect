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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Represents a date in the format <code>yyyyMMdd-HH:mm:ss:SSS</code> <br>
 * E.g. 20121215-21:25:14:580 (or 15-Dec-2012 21:25:14)
 * <p>
 * This is much faster than using a {@link SimpleDateFormat} as it simply performs integer
 * calculation to work out the hours, minutes, seconds or millis that have elapsed since the last
 * call to {@link #yyyyMMddHHmmssSSS(long)}. It relies on the caller passing in what the current
 * time in milliseconds is. However, when the day changes, a full recalculation is performed which
 * is the most expensive operation for this date formatter.
 * <p>
 * <b>THIS IS NOT THREAD SAFE.</b>
 * 
 * @author Ramon Servadei
 */
public final class FastDateFormat
{
    private final static double INVERSE_60000 = 1d / 60000;
    private final static double INVERSE_1000 = 1d / 1000;

    /**
     * @return a string representing the time format <code>yyyyMMdd-HH:mm:ss:SSS</code> for the
     *         passed in arguments
     */
    static String formatDateTime(int yearsMonthsDays, int hours, int mins, int secs, int millis)
    {
        return new StringBuilder(22).append(yearsMonthsDays).append(hours < 10 ? "-0" : "-").append(hours).append(
            mins < 10 ? ":0" : ":").append(mins).append(secs < 10 ? ":0" : ":").append(secs).append(
            millis < 10 ? ":00" : (millis < 100 ? ":0" : ":")).append(millis).toString();
    }

    /** year, month, day of month */
    int yyyyMMdd = -1;
    /** hours */
    int HH = -1;
    /** minutes */
    int mm = -1;
    /** seconds */
    int ss = -1;
    /** millis */
    int SSS = -1;

    long lastTimeMillis = -1;

    final GregorianCalendar cal = new GregorianCalendar();

    /**
     * @return the current time in milliseconds in the format <code>yyyyMMdd-HH:mm:ss:SSS</code>,
     *         e.g. 20121215-21:25:14:580
     */
    public String yyyyMMddHHmmssSSS(long currentTimeMillis)
    {
        long diff = currentTimeMillis - this.lastTimeMillis;
        if (this.lastTimeMillis == -1)
        {
            recalc(currentTimeMillis);
        }
        else
        {
            if (currentTimeMillis < this.lastTimeMillis)
            {
                recalc(currentTimeMillis);
            }
            else
            {
                if (diff < 1000)
                {
                    // less than 1 sec passed
                    this.SSS += diff;
                    if (this.SSS > 999)
                    {
                        this.SSS -= 1000;
                        this.ss++;
                        updateForChangeInSeconds(currentTimeMillis);
                    }
                }
                else
                {
                    if (diff < 60000)
                    {
                        // less than 1 minute passed
                        this.SSS += (int) (diff % 1000);
                        if (this.SSS > 999)
                        {
                            this.SSS -= 1000;
                            this.ss++;
                        }
                        this.ss += (long) (diff * INVERSE_1000);
                        updateForChangeInSeconds(currentTimeMillis);
                    }
                    else
                    {
                        if (diff < 3600000)
                        {
                            // less than 1 hour passed
                            this.SSS += (int) (diff % 1000);
                            if (this.SSS > 999)
                            {
                                this.SSS -= 1000;
                                this.ss++;
                            }
                            this.ss += (long) ((diff % 60000) * INVERSE_1000);
                            if (this.ss > 59)
                            {
                                this.ss -= 60;
                                this.mm++;
                            }
                            this.mm += (long) (diff * INVERSE_60000);
                            if (this.mm > 59)
                            {
                                this.mm -= 60;
                                this.HH++;
                                if (this.HH > 23)
                                {
                                    // recalc when its a new day!
                                    recalc(currentTimeMillis);
                                }
                            }
                        }
                        else
                        {
                            // recalc if > 1 hour has passed
                            recalc(currentTimeMillis);
                        }
                    }
                }
            }
        }
        this.lastTimeMillis = currentTimeMillis;
        return formatDateTime(this.yyyyMMdd, this.HH, this.mm, this.ss, this.SSS);
    }

    private void updateForChangeInSeconds(long currentMillis)
    {
        if (this.ss > 59)
        {
            this.ss -= 60;
            this.mm++;
            if (this.mm > 59)
            {
                this.mm -= 60;
                this.HH++;
                if (this.HH > 23)
                {
                    // recalc when its a new day!
                    recalc(currentMillis);
                }
            }
        }
    }

    private void recalc(long currentMillis)
    {
        this.cal.setTimeInMillis(currentMillis);
        this.yyyyMMdd =
            this.cal.get(Calendar.YEAR) * 10000 + (1 + this.cal.get(Calendar.MONTH)) * 100
                + this.cal.get(Calendar.DAY_OF_MONTH);
        this.HH = this.cal.get(Calendar.HOUR_OF_DAY);
        this.mm = this.cal.get(Calendar.MINUTE);
        this.ss = this.cal.get(Calendar.SECOND);
        this.SSS = this.cal.get(Calendar.MILLISECOND);
    }
}