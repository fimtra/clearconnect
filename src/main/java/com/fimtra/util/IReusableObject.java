/*
 * Copyright 2021 Ramon Servadei
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

package com.fimtra.util;

/**
 * A tagging interface for an object that is re-used by multiple different threads. Typically, this is
 * retrieved from a {@link MultiThreadReusableObjectPool}. In general, the {@link #reset()} method will have
 * been called (by another thread) prior to using this object. The state will be thread-safe *IF* the object
 * comes from a {@link MultiThreadReusableObjectPool}.
 *
 * @author Ramon Servadei
 */
public interface IReusableObject
{
    /**
     * Reset the object before adding back to the pool
     */
    void reset();
}
