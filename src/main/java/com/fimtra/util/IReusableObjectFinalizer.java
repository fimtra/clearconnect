/*
 * Copyright (c) 2018 Ramon Servadei
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
 * Resets a re-usable object when it is returned to the pool
 * 
 * @see AbstractReusableObjectPool
 * @author Ramon Servadei
 * @param <T>
 */
public interface IReusableObjectFinalizer<T>
{
    void reset(T instance);
}