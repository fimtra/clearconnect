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

/**
 * Utility methods for working with {@link Object} instances
 * 
 * @author Ramon Servadei
 */
public abstract class ObjectUtils
{
    private ObjectUtils()
    {

    }

    /**
     * Call the {@link Object#toString()} method on the object, if an exception occurs, a classname
     * based description is returned.
     */
    public static final String safeToString(Object o)
    {
        try
        {
            return o == null ? "null" : o.toString();
        }
        catch (Exception e)
        {
            @SuppressWarnings("null")
            final String fallBack = o.getClass().getName() + "@" + System.identityHashCode(o);
            Log.log(ObjectUtils.class, "Could not get toString for " + fallBack, e);
            return fallBack;
        }
    }
}
