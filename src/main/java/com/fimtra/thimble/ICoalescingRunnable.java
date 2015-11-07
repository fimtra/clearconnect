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

/**
 * A task that can be replaced by a more recent instance for a context. If this task instance is
 * running then no previously submitted tasks for the same context will be run (they were coalesced
 * into the most recent one).
 * <p>
 * This type of task is useful when handling high-frequency updates but only the latest update is
 * needed to be processed at any point in time.
 * <p>
 * A {@link ThimbleExecutor} will only execute one coalescing task for a context at any point in
 * time. Executions of a coalescing task may occur in different threads.
 * 
 * 
 * @author Ramon Servadei
 */
public interface ICoalescingRunnable extends Runnable
{
    /**
     * @return the context for this coalescing task. This groups coalescing tasks together.
     */
    Object context();
}
