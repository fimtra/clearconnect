/*
 * Copyright (c) 2014 Ramon Servadei 
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
package com.fimtra.datafission.field;

import com.fimtra.datafission.IValue;
import com.fimtra.util.is;

/**
 * The operators for comparing {@link IValue} instances:
 * <ul>
 * <li>{@link #EQS}
 * <li>{@link #NEQ}
 * <li>{@link #LT}
 * <li>{@link #LTE}
 * <li>{@link #GT}
 * <li>{@link #GTE}
 * </ul>
 * 
 * @author Ramon Servadei
 */
public enum ValueComparatorEnum
{
    /**
     * <pre>
     * "==" test
     */
    EQS
    {
        @Override
        public boolean evaluate(IValue test, IValue expect)
        {
            return is.eq(test, expect);
        }
    },
    /**
     * <pre>
     * "!=" test
     */
    NEQ
    {
        @Override
        public boolean evaluate(IValue test, IValue expect)
        {
            return !is.eq(test, expect);
        }
    },
    /**
     * <pre>
     * ">" test
     */
    GT
    {
        @Override
        public boolean evaluate(IValue test, IValue expect)
        {
            if (test != null && expect != null && test.getType() == expect.getType())
            {
                switch(test.getType())
                {
                    case DOUBLE:
                        return test.doubleValue() > expect.doubleValue();
                    case LONG:
                        return test.longValue() > expect.longValue();
                    case TEXT:
                        return test.textValue().compareTo(expect.textValue()) > 0;
                    case BLOB:
                    default :
                        return false;
                }
            }
            return false;
        }
    },
    /**
     * <pre>
     * ">=" test
     */
    GTE
    {
        @Override
        public boolean evaluate(IValue test, IValue expect)
        {
            if (test != null && expect != null && test.getType() == expect.getType())
            {
                switch(test.getType())
                {
                    case DOUBLE:
                        return test.doubleValue() >= expect.doubleValue();
                    case LONG:
                        return test.longValue() >= expect.longValue();
                    case TEXT:
                        return test.textValue().compareTo(expect.textValue()) >= 0;
                    case BLOB:
                    default :
                        return false;
                }
            }
            return false;
        }
    },
/**
     * <pre>
     * "<" test
     */
    LT
    {
        @Override
        public boolean evaluate(IValue test, IValue expect)
        {
            if (test != null && expect != null && test.getType() == expect.getType())
            {
                switch(test.getType())
                {
                    case DOUBLE:
                        return test.doubleValue() < expect.doubleValue();
                    case LONG:
                        return test.longValue() < expect.longValue();
                    case TEXT:
                        return test.textValue().compareTo(expect.textValue()) < 0;
                    case BLOB:
                    default :
                        return false;
                }
            }
            return false;
        }
    },
    /**
     * <pre>
     * "<=" test
     */
    LTE
    {
        @Override
        public boolean evaluate(IValue test, IValue expect)
        {
            if (test != null && expect != null && test.getType() == expect.getType())
            {
                switch(test.getType())
                {
                    case DOUBLE:
                        return test.doubleValue() <= expect.doubleValue();
                    case LONG:
                        return test.longValue() <= expect.longValue();
                    case TEXT:
                        return test.textValue().compareTo(expect.textValue()) <= 0;
                    case BLOB:
                    default :
                        return false;
                }
            }
            return false;
        }
    },
    /**
     * <pre>
     * "HAS" test
     */
    CONTAINS {
    	@Override
    	public boolean evaluate(IValue test, IValue expect) {
    		 if (test != null && expect != null && test.getType() == expect.getType())
             {
                 switch(test.getType())
                 {
                     case DOUBLE:
                         return test.doubleValue() >= expect.doubleValue();
                     case LONG:
                         return test.longValue() >= expect.longValue();
                     case TEXT:
                         return test.textValue().toLowerCase().contains(expect.textValue().toLowerCase());
                     case BLOB:
                     default :
                         return false;
                 }
             }
             return false;
    	}
    };

    /**
     * Evaluate this operator against the test and expect values
     * 
     * @param test
     *            the value to test
     * @param expect
     *            the value to compare against the test
     * @return <code>true</code> if the operator verifies that the test is valid against the
     *         expected value
     */
    public abstract boolean evaluate(IValue test, IValue expect);
}
