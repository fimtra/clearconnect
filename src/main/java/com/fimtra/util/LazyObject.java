/*
 * Copyright (c) 2016 Ramon Servadei
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
 * Wraps logic to lazily create an object when the {@link #get()} is called.
 *
 * @author Ramon Servadei
 */
public final class LazyObject<T>
{
    /**
     * Encapsulates the logic to construct the object
     *
     * @param <T>
     * @author Ramon Servadei
     */
    public static interface IConstructor<T>
    {
        T construct();
    }

    /**
     * Encapsulates the logic to destroy the object
     *
     * @param <T>
     * @author Ramon Servadei
     */
    public static interface IDestructor<T>
    {
        void destroy(T ref);
    }

    IConstructor<T> constructor;
    IDestructor<T> destructor;
    T ref;

    public LazyObject(IConstructor<T> constructor, IDestructor<T> destructor)
    {
        this.constructor = constructor;
        this.destructor = destructor;
    }

    /**
     * Get the object, constructing it if required
     */
    public synchronized T get()
    {
        if (this.ref == null)
        {
            this.ref = this.constructor.construct();
            this.constructor = null;
        }
        return this.ref;
    }

    public synchronized void destroy()
    {
        if (this.ref != null)
        {
            this.destructor.destroy(this.ref);
            this.ref = null;
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.ref == null) ? 0 : this.ref.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LazyObject<?> other = (LazyObject<?>) obj;
        if (this.ref == null)
        {
            if (other.ref != null)
                return false;
        }
        else if (!this.ref.equals(other.ref))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LazyObject [ref=" + this.ref + "]";
    }

}
