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

import java.util.Arrays;

/**
 * A utility class for performing member comparisons within implementations of
 * the {@link Object#equals(Object)} method. Instead of the usual
 *
 * <pre>
 * if (this.name == null)
 * {
 *     if (other.name != null)
 *     {
 *         return false;
 *     }
 * }
 * else
 * {
 *     if (!name.equals(other.name))
 *     {
 *         return false;
 *     }
 * }
 * if (this.code != other.code)
 *     return false;
 * </pre>
 * <p>
 * this class allows the following
 *
 * <pre>
 * return is.eq(this.name, other.name) &amp;&amp; is.eq(this.code, other.code);
 * </pre>
 *
 * @author Ramon Servadei
 */
public abstract class is
{
    /**
     * Test if the two objects are of the same {@link Class}. <b>This assumes
     * the first argument is not null.</b> Also checks if the second argument is
     * <code>null</code>.
     *
     * @param candidate the first object
     * @param other     the other object
     * @return <code>false</code> if both objects are of the same {@link Class},
     * <code>true</code> if they are not OR the second argument is
     * <code>null</code> (in which case the classes are not equal
     * anyway).
     */
    public static boolean differentClass(Object candidate, Object other)
    {
        return other == null || candidate.getClass() != other.getClass();
    }

    /**
     * Performs a reference check of two objects.
     *
     * @param o1 the first object reference
     * @param o2 the other object reference
     * @return <code>true</code> if the objects references are for the same
     * object
     */
    public static boolean same(Object o1, Object o2)
    {
        return o1 == o2;
    }

    /**
     * Performs an equality check of two objects. Also checks neither is
     * <code>null</code>.
     *
     * @param o1 the first object
     * @param o2 the other object
     * @return <code>true</code> if the objects are equal OR both are
     * <code>null</code>
     */
    public static boolean eq(Object o1, Object o2)
    {
        return (o1 == null && o2 == null) || (o1 != null && o1.equals(o2));
    }

    /**
     * Performs an equality check for two <code>boolean</code> arguments
     *
     * @param b1 the first argument
     * @param b2 the other argument
     * @return <code>true</code> if the arguments are equal
     */
    public static boolean eq(boolean b1, boolean b2)
    {
        return b1 == b2;
    }

    /**
     * Performs an equality check for two <code>byte</code> arguments
     *
     * @param b1 the first argument
     * @param b2 the other argument
     * @return <code>true</code> if the arguments are equal
     */
    public static boolean eq(byte b1, byte b2)
    {
        return b1 == b2;
    }

    /**
     * Performs an equality check for two <code>char</code> arguments
     *
     * @param c1 the first argument
     * @param c2 the other argument
     * @return <code>true</code> if the arguments are equal
     */
    public static boolean eq(char c1, char c2)
    {
        return c1 == c2;
    }

    /**
     * Performs an equality check for two <code>short</code> arguments
     *
     * @param s1 the first argument
     * @param s2 the other argument
     * @return <code>true</code> if the arguments are equal
     */
    public static boolean eq(short s1, short s2)
    {
        return s1 == s2;
    }

    /**
     * Performs an equality check for two <code>integer</code> arguments
     *
     * @param i1 the first argument
     * @param i2 the other argument
     * @return <code>true</code> if the arguments are equal
     */
    public static boolean eq(int i1, int i2)
    {
        return i1 == i2;
    }

    /**
     * Performs an equality check for two <code>long</code> arguments
     *
     * @param l1 the first argument
     * @param l2 the other argument
     * @return <code>true</code> if the arguments are equal
     */
    public static boolean eq(long l1, long l2)
    {
        return l1 == l2;
    }

    /**
     * Performs an equality check for two <code>float</code> arguments
     *
     * @param f1 the first argument
     * @param f2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Float#floatToIntBits(float)
     */
    public static boolean eq(float f1, float f2)
    {
        return Float.floatToIntBits(f1) == Float.floatToIntBits(f2);
    }

    /**
     * Performs an equality check for two <code>double</code> arguments.
     *
     * @param d1 the first argument
     * @param d2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Double#doubleToLongBits(double)
     */
    public static boolean eq(double d1, double d2)
    {
        return Double.doubleToLongBits(d1) == Double.doubleToLongBits(d2);
    }

    /**
     * Performs an equality check for two <code>boolean[]</code> arguments
     *
     * @param a1 the first argument
     * @param a2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Arrays#equals(boolean[], boolean[])
     */
    public static boolean eq(boolean[] a1, boolean[] a2)
    {
        return Arrays.equals(a1, a2);
    }

    /**
     * Performs an equality check for two <code>byte[]</code> arguments
     *
     * @param a1 the first argument
     * @param a2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Arrays#equals(byte[], byte[])
     */
    public static boolean eq(byte[] a1, byte[] a2)
    {
        return Arrays.equals(a1, a2);
    }

    /**
     * Performs an equality check for two <code>char[]</code> arguments
     *
     * @param a1 the first argument
     * @param a2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Arrays#equals(char[], char[])
     */
    public static boolean eq(char[] a1, char[] a2)
    {
        return Arrays.equals(a1, a2);
    }

    /**
     * Performs an equality check for two <code>short[]</code> arguments
     *
     * @param a1 the first argument
     * @param a2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Arrays#equals(short[], short[])
     */
    public static boolean eq(short[] a1, short[] a2)
    {
        return Arrays.equals(a1, a2);
    }

    /**
     * Performs an equality check for two <code>int[]</code> arguments
     *
     * @param a1 the first argument
     * @param a2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Arrays#equals(int[], int[])
     */
    public static boolean eq(int[] a1, int[] a2)
    {
        return Arrays.equals(a1, a2);
    }

    /**
     * Performs an equality check for two <code>long[]</code> arguments
     *
     * @param a1 the first argument
     * @param a2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Arrays#equals(long[], long[])
     */
    public static boolean eq(long[] a1, long[] a2)
    {
        return Arrays.equals(a1, a2);
    }

    /**
     * Performs an equality check for two <code>float[]</code> arguments
     *
     * @param a1 the first argument
     * @param a2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Arrays#equals(float[], float[])
     */
    public static boolean eq(float[] a1, float[] a2)
    {
        return Arrays.equals(a1, a2);
    }

    /**
     * Performs an equality check for two <code>double[]</code> arguments
     *
     * @param a1 the first argument
     * @param a2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Arrays#equals(double[], double[])
     */
    public static boolean eq(double[] a1, double[] a2)
    {
        return Arrays.equals(a1, a2);
    }

    /**
     * Performs an equality check for two <code>Object[]</code> arguments
     *
     * @param a1 the first argument
     * @param a2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Arrays#equals(Object[], Object[])
     */
    public static boolean eq(Object[] a1, Object[] a2)
    {
        return Arrays.equals(a1, a2);
    }

    /**
     * Performs a deep equality check for two <code>Object[]</code> arguments
     *
     * @param a1 the first argument
     * @param a2 the other argument
     * @return <code>true</code> if the arguments are equal
     * @see Arrays#deepEquals(Object[], Object[])
     */
    public static boolean deepEq(Object[] a1, Object[] a2)
    {
        return Arrays.deepEquals(a1, a2);
    }

    /**
     * Checks that the argument is not <code>null</code>
     *
     * @param o the argument
     * @return <code>true</code> if the argument is not <code>null</code>
     */
    public static boolean notNull(Object o)
    {
        return o != null;
    }
}
