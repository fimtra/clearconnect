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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link is}
 * 
 * @author Ramon Servadei
 */
public class isTest
{

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for
     * {@link fulmine.util.reference.is#differentClass(java.lang.Object, java.lang.Object)} .
     */
    @Test
    public void testDifferentClass()
    {
        assertTrue(is.differentClass(new Object(), new String()));
        assertFalse(is.differentClass(new Object(), new Object()));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#same(java.lang.Object, java.lang.Object)} .
     */
    @Test
    public void testSame()
    {
        Object o = new Object();
        assertTrue(is.same(o, o));
        assertFalse(is.same(o, new String()));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(java.lang.Object, java.lang.Object)}.
     */
    @Test
    public void testEqObjectObject()
    {
        Object o1 = new String("");
        Object o2 = new String("");
        assertTrue(is.eq(o1, o2));
        assertTrue(is.eq((Object) null, null));
        assertFalse(is.eq(null, o2));
        assertFalse(is.eq(o1, null));
        assertFalse(is.eq(o1, new String("nomatch")));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(boolean, boolean)}.
     */
    @Test
    public void testEqBooleanBoolean()
    {
        assertTrue(is.eq(true, true));
        assertFalse(is.eq(true, false));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(byte, byte)}.
     */
    @Test
    public void testEqByteByte()
    {
        final byte arg1 = (byte) 1;
        final byte arg2 = (byte) 2;
        assertTrue(is.eq(arg1, arg1));
        assertFalse(is.eq(arg2, arg1));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(char, char)}.
     */
    @Test
    public void testEqCharChar()
    {
        final char arg1 = (char) 1;
        final char arg2 = (char) 2;
        assertTrue(is.eq(arg1, arg1));
        assertFalse(is.eq(arg2, arg1));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(short, short)}.
     */
    @Test
    public void testEqShortShort()
    {
        final short arg1 = (short) 1;
        final short arg2 = (short) 2;
        assertTrue(is.eq(arg1, arg1));
        assertFalse(is.eq(arg2, arg1));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(int, int)}.
     */
    @Test
    public void testEqIntInt()
    {
        final int arg1 = 1;
        final int arg2 = 2;
        assertTrue(is.eq(arg1, arg1));
        assertFalse(is.eq(arg2, arg1));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(long, long)}.
     */
    @Test
    public void testEqLongLong()
    {
        final long arg1 = 1l;
        final long arg2 = 2l;
        assertTrue(is.eq(arg1, arg1));
        assertFalse(is.eq(arg2, arg1));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(float, float)}.
     */
    @Test
    public void testEqFloatFloat()
    {
        final float arg1 = 1f;
        final float arg2 = 2f;
        assertTrue(is.eq(arg1, arg1));
        assertFalse(is.eq(arg2, arg1));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(double, double)}.
     */
    @Test
    public void testEqDoubleDouble()
    {
        final double arg1 = 1d;
        final double arg2 = 2d;
        assertTrue(is.eq(arg1, arg1));
        assertFalse(is.eq(arg2, arg1));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(boolean[], boolean[])}.
     */
    @Test
    public void testEqBooleanArrayBooleanArray()
    {
        final boolean[] arg1 = new boolean[] { true, true };
        final boolean[] arg2 = new boolean[] { true, true };
        final boolean[] arg3 = new boolean[] { true, false };
        assertTrue(is.eq(arg1, arg2));
        assertFalse(is.eq(arg2, arg3));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(byte[], byte[])}.
     */
    @Test
    public void testEqByteArrayByteArray()
    {
        final byte[] arg1 = new byte[] { (byte) 2, (byte) 2 };
        final byte[] arg2 = new byte[] { (byte) 2, (byte) 2 };
        final byte[] arg3 = new byte[] { (byte) 2 };
        assertTrue(is.eq(arg1, arg2));
        assertFalse(is.eq(arg2, arg3));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(char[], char[])}.
     */
    @Test
    public void testEqCharArrayCharArray()
    {
        final char[] arg1 = new char[] { (char) 2, (char) 2 };
        final char[] arg2 = new char[] { (char) 2, (char) 2 };
        final char[] arg3 = new char[] { (char) 2 };
        assertTrue(is.eq(arg1, arg2));
        assertFalse(is.eq(arg2, arg3));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(short[], short[])}.
     */
    @Test
    public void testEqShortArrayShortArray()
    {
        final short[] arg1 = new short[] { (short) 2, (short) 2 };
        final short[] arg2 = new short[] { (short) 2, (short) 2 };
        final short[] arg3 = new short[] { (short) 2 };
        assertTrue(is.eq(arg1, arg2));
        assertFalse(is.eq(arg2, arg3));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(int[], int[])}.
     */
    @Test
    public void testEqIntArrayIntArray()
    {
        final int[] arg1 = new int[] { 1, 2 };
        final int[] arg2 = new int[] { 1, 2 };
        final int[] arg3 = new int[] { 2 };
        assertTrue(is.eq(arg1, arg2));
        assertFalse(is.eq(arg2, arg3));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(long[], long[])}.
     */
    @Test
    public void testEqLongArrayLongArray()
    {
        final long[] arg1 = new long[] { 1l, 2l };
        final long[] arg2 = new long[] { 1l, 2l };
        final long[] arg3 = new long[] { 2l };
        assertTrue(is.eq(arg1, arg2));
        assertFalse(is.eq(arg2, arg3));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(float[], float[])}.
     */
    @Test
    public void testEqFloatArrayFloatArray()
    {
        final float[] arg1 = new float[] { 1f, 2f };
        final float[] arg2 = new float[] { 1f, 2f };
        final float[] arg3 = new float[] { 2f };
        assertTrue(is.eq(arg1, arg2));
        assertFalse(is.eq(arg2, arg3));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(double[], double[])}.
     */
    @Test
    public void testEqDoubleArrayDoubleArray()
    {
        final double[] arg1 = new double[] { 1d, 2d };
        final double[] arg2 = new double[] { 1d, 2d };
        final double[] arg3 = new double[] { 2d };
        assertTrue(is.eq(arg1, arg2));
        assertFalse(is.eq(arg2, arg3));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#eq(java.lang.Object[], java.lang.Object[])}
     * .
     */
    @Test
    public void testEqObjectArrayObjectArray()
    {
        final Object[] arg1 = new Object[] { "1", "2" };
        final Object[] arg2 = new Object[] { "1", "2" };
        final Object[] arg3 = new Object[] { "1", "1" };
        assertTrue(is.eq(arg1, arg2));
        assertFalse(is.eq(arg2, arg3));
    }

    /**
     * Test method for
     * {@link fulmine.util.reference.is#deepEq(java.lang.Object[], java.lang.Object[])} .
     */
    @Test
    public void testDeepEq()
    {
        final Object[][] arg1 = new Object[][] { { "1", "2" } };
        final Object[][] arg2 = new Object[][] { { "1", "2" } };
        final Object[][] arg3 = new Object[][] { { "1", "1" }, { "2" } };
        assertTrue(is.deepEq(arg1, arg2));
        assertFalse(is.deepEq(arg2, arg3));
    }

    /**
     * Test method for {@link fulmine.util.reference.is#notNull(java.lang.Object)}.
     */
    @Test
    public void testNotNull()
    {
        assertTrue(is.notNull(""));
        assertFalse(is.notNull(null));
    }

}
