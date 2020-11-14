/*
 * Copyright 2020 Ramon Servadei
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.executors;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.fimtra.executors.gatling.GatlingExecutor;
import com.fimtra.util.LazyObject;
import com.fimtra.util.Log;
import com.fimtra.util.SystemUtils;
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
    public static final boolean POOL_ACTIVE =
            SystemUtils.getProperty("ContextExecutorFactory.poolActive", true);

    private static final int CORE_SIZE = SystemUtils.getPropertyAsInt("ContextExecutorFactory.coreSize", 1);

    private static final LazyObject<GatlingExecutor> POOL_HOLDER =
            new LazyObject<>(() -> new GatlingExecutor("gatling-core", CORE_SIZE){
                @Override
                public void destroy()
                {
                    Log.log(this, "Cannot destroy " + toString());
                }

                @Override
                public void shutdown()
                {
                    Log.log(this, "Cannot shutdown " + toString());
                }

                @Override
                public List<Runnable> shutdownNow()
                {
                    Log.log(this, "Cannot shutdown " + toString());
                    return Collections.emptyList();
                }
            }, (g) -> {
            });

    /**
     * Create an instance with the passed in name and minimum core thread corePoolSize. If {{@link
     * #POOL_ACTIVE}} then the pooled version is returned.
     */
    public static IContextExecutor create(String name, int corePoolSize)
    {
        return POOL_ACTIVE ? POOL_HOLDER.get() : new GatlingExecutor(name, corePoolSize, null);
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

    @SuppressWarnings("unchecked")
    public static Set<IContextExecutor> getExecutors()
    {
        return (Set<IContextExecutor>) GatlingExecutor.getExecutors();
    }

    static final Map<Object, IContextExecutor> CLASS_LEVEL = new HashMap<>();
    static final WeakHashMap<Object, IContextExecutor> AD_HOCS = new WeakHashMap<>();

    public static synchronized IContextExecutor get(Class<?> c)
    {
        return CLASS_LEVEL.computeIfAbsent(c,
                context -> new GatlingExecutor(context.toString(), 1, POOL_HOLDER.get()));
    }

    public static synchronized IContextExecutor get(Object c)
    {
        return AD_HOCS.computeIfAbsent(c,
                context -> new GatlingExecutor(context.toString(), 1, POOL_HOLDER.get()));
    }

    public static synchronized void remove(Object c)
    {
        AD_HOCS.remove(c);
    }
}
