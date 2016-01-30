/*
 * Copyright (c) 2016 Ramon Servadei 
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
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.core.Context;

/**
 * Tests for the {@link ObjectSerializer}
 * 
 * @author Ramon Servadei
 */
public class ObjectSerializerTest
{

    ObjectSerializer candidate;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new ObjectSerializer();
    }

    @Test
    public void testSimple() throws Exception
    {
        Context c = new Context("test");
        IRecord record = c.getOrCreateRecord("sdf-string");

        SDF sdf = prepareSDF();
        sdf.o = "a string!";
        sdf.i_transient = 12345;
        
        this.candidate.writeObject(sdf, record);
        final SDF got = this.candidate.readObject(record);
        System.err.println(sdf + "->" + got + " from " + record);
        assertEquals("sdf=" + sdf + ", got=" + got, sdf, got);
        assertEquals("transient members should not be sent", 0, got.i_transient);
    }

    @Test
    public void testWithInheritance() throws Exception
    {
        Context c = new Context("test");
        IRecord record = c.getOrCreateRecord("sdf-string");

        SDF2 sdf = prepareSDF2();
        sdf.o = "a string!";

        this.candidate.writeObject(sdf, record);
        final Object got = this.candidate.readObject(record);
        System.err.println(sdf + "->" + got + " from " + record);
        assertEquals("sdf=" + sdf + ", got=" + got, sdf, got);
    }

    @Test(expected = ClassCastException.class)
    public void testWithNonSerializable() throws Exception
    {
        Context c = new Context("test");
        IRecord record = c.getOrCreateRecord("sdf-string");

        SDF sdf = prepareSDF();
        // a Context is not serializable, so this will fail
        sdf.o = c;

        this.candidate.writeObject(sdf, record);
        fail("This test should fail");
    }

    static SDF prepareSDF()
    {
        SDF sdf = new SDF();
        Random rnd = new Random();
        sdf.s = "sdf!!!! " + rnd.nextInt();
        sdf.b = (byte) rnd.nextInt();
        sdf.c = (char) rnd.nextInt();
        sdf.bool = rnd.nextBoolean();
        sdf.i = rnd.nextInt();
        sdf.l = rnd.nextLong();
        sdf.f = rnd.nextFloat();
        sdf.d = rnd.nextDouble();
        sdf.iarr = new int[] { rnd.nextInt(), rnd.nextInt() };
        return sdf;
    }

    static SDF2 prepareSDF2()
    {
        SDF2 sdf = new SDF2();
        Random rnd = new Random();
        sdf.s = "sdf!!!! " + rnd.nextInt();
        sdf.b = (byte) rnd.nextInt();
        sdf.c = (char) rnd.nextInt();
        sdf.bool = rnd.nextBoolean();
        sdf.i = rnd.nextInt();
        sdf.f = rnd.nextFloat();
        sdf.d = rnd.nextDouble();
        sdf.iarr = new int[] { rnd.nextInt(), rnd.nextInt() };
        sdf.setSuperL(rnd.nextLong());
        sdf.setL(rnd.nextLong());
        return sdf;
    }

}

class SDF
{
    transient int i_transient;
    boolean bool;
    char c;
    byte b;
    short sh;
    int i;
    long l;
    float f;
    double d;
    String s;
    int[] iarr;
    Object o;

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.b;
        result = prime * result + (this.bool ? 1231 : 1237);
        result = prime * result + this.c;
        long temp;
        temp = Double.doubleToLongBits(this.d);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + Float.floatToIntBits(this.f);
        result = prime * result + this.i;
        result = prime * result + Arrays.hashCode(this.iarr);
        result = prime * result + (int) (this.l ^ (this.l >>> 32));
        result = prime * result + ((this.s == null) ? 0 : this.s.hashCode());
        result = prime * result + this.sh;
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SDF other = (SDF) obj;
        if (this.b != other.b)
            return false;
        if (this.bool != other.bool)
            return false;
        if (this.c != other.c)
            return false;
        if (Double.doubleToLongBits(this.d) != Double.doubleToLongBits(other.d))
            return false;
        if (Float.floatToIntBits(this.f) != Float.floatToIntBits(other.f))
            return false;
        if (this.i != other.i)
            return false;
        if (!Arrays.equals(this.iarr, other.iarr))
            return false;
        if (this.l != other.l)
            return false;
        if (this.s == null)
        {
            if (other.s != null)
                return false;
        }
        else if (!this.s.equals(other.s))
            return false;
        if (this.sh != other.sh)
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "SDF [b=" + this.b + ", c=" + this.c + ", bool=" + this.bool + ", i=" + this.i + ", l=" + this.l + ", f="
            + this.f + ", d=" + this.d + ", sh=" + this.sh + ", s=" + this.s + ", iarr=" + Arrays.toString(this.iarr)
            + "]";
    }
}

class SDF2 extends SDF
{
    private long l;

    void setL(long l)
    {
        this.l = l;
    }

    void setSuperL(long l)
    {
        super.l = l;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (int) (l ^ (l >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        SDF2 other = (SDF2) obj;
        if (l != other.l)
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "SDF2 [l=" + l + "]:" + super.toString();
    }

}