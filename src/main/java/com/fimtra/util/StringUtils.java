/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei 
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

import java.util.LinkedList;
import java.util.List;

/**
 * Utility methods for string manipulation.
 * 
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
public abstract class StringUtils
{

    private StringUtils()
    {
        // Not for instantiation
    }

    private static final String emptyString = "";

    /**
     * Find out if a char[] starts with a prefix
     * 
     * @param prefix
     *            the prefix
     * @param other
     *            the char[] to find the prefix in
     * @return <code>true</code> if the char[] starts with prefix
     */
    public final static boolean startsWith(char[] prefix, char[] other)
    {
        if (prefix.length != 0 && prefix.length <= other.length)
        {
            for (int i = 0; i < prefix.length; i++)
            {
                if (other[i] != prefix[i])
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Will return an empty string if str is null, otherwise it will return str with trailing and
     * leading whitespace removed.
     */
    public final static String stripToEmpty(String str)
    {
        if (str == null)
        {
            return emptyString;
        }
        return str.trim();
    }

    /**
     * Returns true if the str is null, empty or contains only whitespace.
     */
    public final static boolean isEmpty(String str)
    {
        String stripped = stripToEmpty(str);
        return stripped.isEmpty();
    }

    /**
     * Split a string into tokens identified by a delimiter character. The delimiter character is
     * 'escaped' by a preceeding occurrence of the delimiter character, e.g:<br>
     * 
     * <pre>
     * If the delimiter is ','
     * "token1,token2" -> "token1" "token2"
     * "token1,,,token2" -> "token1," "token2"
     * </pre>
     * 
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the delimiter character to find in the string
     * @return the string separated into tokens identified by the delimiter, <code>null</code> if
     *         stringToSplit is <code>null</code>
     */
    public final static List<String> split(String stringToSplit, char delimiter)
    {
        if (stringToSplit == null)
        {
            return null;
        }

        final char[] chars = stringToSplit.toCharArray();
        List<String> tokens = new LinkedList<String>();
        StringBuilder sb = new StringBuilder();
        char c;
        boolean lastWasDelimiter = false;
        for (int i = 0; i < chars.length; i++)
        {
            c = chars[i];
            if (c == delimiter)
            {
                if (lastWasDelimiter)
                {
                    // add the token to the buffer
                    sb.append(c);
                    lastWasDelimiter = false;
                }
                else
                {
                    lastWasDelimiter = true;
                }
            }
            else
            {
                if (lastWasDelimiter)
                {
                    // a single token is a separator
                    tokens.add(sb.toString());
                    sb.setLength(0);
                    lastWasDelimiter = false;
                }
                sb.append(c);
            }
        }
        tokens.add(sb.toString());
        return tokens;
    }

    /**
     * Join the list of strings with the passed in delimiter. Any occurrence of the delimiter in the
     * strings is escaped with the delimiter, e.g.:
     * 
     * <pre>
     * If the delimiter char is ','
     * Joining "string with, comma" and "another string" -> "string with,, comma,another string"
     * </pre>
     * 
     * @param stringsToJoin
     *            the list of strings to join
     * @param delimiter
     *            the delimiter to use when joining the strings
     * @return a string that represents the list of strings to join each separated by the delimiter
     *         character, <code>null</code> if stringsToJoin is <code>null</code>
     */
    public final static String join(List<String> stringsToJoin, char delimiter)
    {
        if (stringsToJoin == null)
        {
            return null;
        }

        StringBuilder sb = new StringBuilder(stringsToJoin.size() * 30);
        for (int i = 0; i < stringsToJoin.size(); i++)
        {
            if (i > 0)
            {
                sb.append(delimiter);
            }
            addToBuilderAndEscapeDelimiter(stringsToJoin.get(i), delimiter, sb);
        }
        return sb.toString();
    }

    /**
     * Add the string to the builder and for any occurrence of the escapeChar, the escapeChar is
     * doubled, e.g.:
     * 
     * <pre>
     * If the escape char is ','
     * "string with, comma" -> "string with,, comma"
     * </pre>
     */
    private static void addToBuilderAndEscapeDelimiter(String string, char escapeChar, StringBuilder sb)
    {
        if (string == null)
        {
            sb.append((String) null);
            return;
        }

        char[] chars = string.toCharArray();
        for (int i = 0; i < chars.length; i++)
        {
            if (chars[i] == escapeChar)
            {
                sb.append(escapeChar);
            }
            sb.append(chars[i]);
        }
    }
}
