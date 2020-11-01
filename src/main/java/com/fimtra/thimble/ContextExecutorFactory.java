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
 * Decouples constructing of an {@link IContextExecutor} from the implementation
 *
 * @author Ramon Servadei
 */
public class ContextExecutorFactory
{
    public static final boolean POOL_ACTIVE = !Boolean.getBoolean("ContextExecutorFactory.poolActive");

    private static class POOL_HOLDER
    {
        static final ThimbleExecutor POOL =
                new ThimbleExecutor("thimble", Runtime.getRuntime().availableProcessors());
    }

    public static IContextExecutor create(String name, int size)
    {
        return POOL_ACTIVE ? POOL_HOLDER.POOL : new ThimbleExecutor(name, size);
    }

    public static IContextExecutor create(String name)
    {
        return create(name, Runtime.getRuntime().availableProcessors());
    }

    public static IContextExecutor create(int size)
    {
        return create(ThreadUtils.getDirectCallingClassSimpleName(), size);
    }
}
