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
package com.fimtra.datafission;

import com.fimtra.datafission.field.BlobValue;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;

/**
 * An immutable value for a key-value pair held in a record.
 * 
 * @author Ramon Servadei
 */
public interface IValue extends Comparable<IValue>
{
    char LONG_CODE = 'L';
    char DOUBLE_CODE = 'D';
    char TEXT_CODE = 'S';
    char BLOB_CODE = 'B';

    /**
     * Expresses the type of an {@link IValue}
     * 
     * @author Ramon Servadei
     */
    public static enum TypeEnum
    {
        LONG(LONG_CODE), DOUBLE(DOUBLE_CODE), TEXT(TEXT_CODE), BLOB(BLOB_CODE);

        private final String shortString;
        private char charCode;

        private TypeEnum(char charCode)
        {
            this.shortString = new String(new char[] { charCode });
            this.charCode = charCode;
        }

        /**
         * Construct the {@link IValue} that this type represents with the provided value
         */
        public IValue fromString(String value)
        {
            switch(this)
            {
                case DOUBLE:
                    return new DoubleValue(Double.parseDouble(value));
                case LONG:
                    return LongValue.valueOf(Long.parseLong(value));
                case TEXT:
                    return new TextValue(value);
                case BLOB:
                    return new BlobValue(value);
            }
            return null;
        }

        @Override
        public String toString()
        {
            return this.shortString;
        }

        /**
         * @return the char representation of the short string
         */
        public char getCharCode()
        {
            return this.charCode;
        }
    }

    /**
     * @return the type for the value. Use the enum to call the appropriate xxxValue() method.
     */
    TypeEnum getType();

    /**
     * @return the value as a long
     * @throws NumberFormatException
     *             if the type is {@link TypeEnum#TEXT} and the string cannot be converted.
     */
    long longValue();

    /**
     * @return the value as a double
     * @throws NumberFormatException
     *             if the type is {@link TypeEnum#TEXT} and the string cannot be converted.
     */
    double doubleValue();

    /** @return the value as a string, <b>NEVER</b> <code>null</code> */
    String textValue();
    
    /** @return the byte[] representation of this value. */
    byte[] byteValue();
}
