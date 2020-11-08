/*
 * Copyright (c) 2019 Ramon Servadei
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
package com.fimtra.thimble;

import com.fimtra.util.ThreadUtils;

/**
 * Decouples constructing of an {@link IContextExecutor} from the implementation.
 *
 * @author Ramon Servadei
 */
public class ContextExecutorFactory {

    /**
     * Defines if the pooled version is active - all calls to {@link #create(String, int)} will return the
     * pool not a unique instance.
     */
    public static final boolean POOL_ACTIVE = !Boolean.getBoolean("ContextExecutorFactory.poolInactive");

    private static final int CORE_SIZE =
            Integer.parseInt(System.getProperty("ContextExecutorFactory.coreSize", "1"));

    private static class POOL_HOLDER {
        static final ThimbleExecutor POOL = new ThimbleExecutor("thimble", CORE_SIZE);
    }

    /**
     * Create an instance with the passed in name and minimum core thread corePoolSize. If {{@link
     * #POOL_ACTIVE}} then the pooled version is returned.
     */
    public static IContextExecutor create(String name, int corePoolSize)
    {
        return POOL_ACTIVE ? POOL_HOLDER.POOL : new ThimbleExecutor(name, corePoolSize);
    }

    /**
     * Create an instance with the passed in name.
     *
     * @see #create(String, int)
     */
    public static IContextExecutor create(String name)
    {
        return create(name, CORE_SIZE);
    }

    /**
     * Create an instance using the calling class as the name.
     *
     * @see #create(String, int)
     */
    public static IContextExecutor create(int corePoolSize)
    {
        return create(ThreadUtils.getDirectCallingClassSimpleName(), corePoolSize);
    }
}
