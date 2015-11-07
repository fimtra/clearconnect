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
package com.fimtra.thimble;

import java.util.Map;

/**
 * A task that is run in order, sequentially for a context. The context groups sequential tasks.
 * <p>
 * If many tasks with the same context are submitted to a {@link ThimbleExecutor}, the tasks will
 * run in submission order, sequentially for the task context. Executions of a sequential task may
 * occur in different threads.
 * <p>
 * Multiple contexts can be handled in parallel. This means that there can be multiple sequential
 * tasks executing in parallel but for different contexts.
 * 
 * @author Ramon Servadei
 */
public interface ISequentialRunnable extends Runnable
{
    /**
     * @return the context for this sequential task. This groups sequential tasks together. Grouping
     *         is performed using the {@link Object#hashCode()} and {@link Object#equals(Object)} of
     *         the context (like that of a key in a {@link Map}).
     */
    Object context();
}
