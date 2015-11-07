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
package com.fimtra.datafission.core;

import java.util.Map;

import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.TextValue;

/**
 * A status attribute is held in the 'context status' record of an {@link IObserverContext} (see
 * {@link ISystemRecordNames#CONTEXT_STATUS}). Each attribute class has a set of values that
 * describe the states the attribute can have (e.g. Connection can have CONNECTED, DISCONNECTED).
 * 
 * @see IObserverContext#getContextStatusName()
 * @author Ramon Servadei
 */
public interface IStatusAttribute
{
    /**
     * The connection status of a {@link ProxyContext}. This attribute indicates if the remote
     * context is disconnected, connected or reconnecting.
     * 
     * @author Ramon Servadei
     */
    enum Connection implements IStatusAttribute
    {
        DISCONNECTED, CONNECTED, RECONNECTING;
    }

    /**
     * The utility methods for setting and getting status attributes from a context
     * 
     * @author Ramon Servadei
     */
    class Utils
    {
        /**
         * Set the status attribute on the record
         */
        public static void setStatus(IStatusAttribute statusAttribute, Map<String, IValue> record)
        {
            if (record != null)
            {
                record.put(statusAttribute.getClass().getSimpleName().toString(),
                    new TextValue(statusAttribute.toString()));
            }
        }

        /**
         * Get the value of a status attribute from a record
         * 
         * @return the value of the status attribute class from the record
         */
        @SuppressWarnings("unchecked")
        public static <T extends IStatusAttribute> T getStatus(Class<T> attribute, Map<String, IValue> record)
        {
            if (record == null || attribute == null)
            {
                return null;
            }
            final IValue iValue = record.get(attribute.getSimpleName());
            if (iValue == null)
            {
                return null;
            }
            try
            {
                return (T) attribute.getDeclaredField(iValue.textValue()).get(null);
            }
            catch (Exception e)
            {
                e.printStackTrace();
                return null;
            }
        }

        /**
         * Get the status attribute held in a context
         * 
         * @return the value of the status attribute class from the context
         */
        public static IStatusAttribute getStatus(Class<? extends IStatusAttribute> attribute, IObserverContext context)
        {
            return getStatus(attribute, context.getRecord(ISystemRecordNames.CONTEXT_STATUS));
        }
    }
}
