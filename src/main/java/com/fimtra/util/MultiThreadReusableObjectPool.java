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
 * A pool of re-usable objects that can be accessed by multiple threads.
 * <p>
 * <b>THREAD SAFE</b>
 * 
 * @author Ramon Servadei
 * @param <T>
 */
public final class MultiThreadReusableObjectPool<T> extends AbstractReusableObjectPool<T>
{
    public MultiThreadReusableObjectPool(String name, IReusableObjectBuilder<T> builder,
        IReusableObjectFinalizer<T> finalizer, int maxSize)
    {
        super(name, builder, finalizer, maxSize);
    }

    @Override
    public T get()
    {
        this.lock.lock();
        try
        {
            final T instance = doGet();
            if (instance != null)
            {
                return instance;
            }
        }
        finally
        {
            this.lock.unlock();
        }
        return this.builder.newInstance();
    }

    @Override
    public void offer(T instance)
    {
        this.lock.lock();
        try
        {
            doOffer(instance);
        }
        finally
        {
            this.lock.unlock();
        }
    }
}
