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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A generic component that manages subscription interest. Subscribers are registered with the
 * manager against a subscription key.
 * <p>
 * <b>Calls to methods that add or remove subscribers should be synchronized.</b>
 * 
 * @author Ramon Servadei
 */
public final class SubscriptionManager<SUBSCRIPTION_KEY, SUBSCRIBER>
{
    final Object[] emptyArray;
    final Class<?> subscriberClass;

    /** Tracks the list of observers for a name. The list uses copy-on-write semantics */
    final ConcurrentMap<SUBSCRIPTION_KEY, SUBSCRIBER[]> subscribersPerKey =
        new ConcurrentHashMap<SUBSCRIPTION_KEY, SUBSCRIBER[]>(2);

    /**
     * Construct the subscription manager passing in the class for the subscriber. This is required
     * to ensure correct array component type is used internall.
     * 
     * @param subscriberClass
     */
    public SubscriptionManager(Class<?> subscriberClass)
    {
        super();
        this.subscriberClass = subscriberClass;
        this.emptyArray = (Object[]) Array.newInstance(subscriberClass, 0);
    }

    /**
     * Get the subscribers for the subscription key
     * <p>
     * <b>Calls to this method DO NOT NEED TO BE SYNCHRONIZED, unlike
     * {@link #addSubscriberFor(Object, Object)}.</b>
     * 
     * @param key
     *            the subscription key
     * @return an array of subscribers for the key. This is NOT a copy - DO NOT MESS WITH IT. Will
     *         never be <code>null</code>.
     */
    @SuppressWarnings("unchecked")
    public SUBSCRIBER[] getSubscribersFor(SUBSCRIPTION_KEY key)
    {
        if (key == null)
        {
            return (SUBSCRIBER[]) this.emptyArray;
        }
        final SUBSCRIBER[] current = this.subscribersPerKey.get(key);
        if (current == null)
        {
            return (SUBSCRIBER[]) this.emptyArray;
        }
        return current;
    }

    /**
     * Add the subscriber to the list of subscribers for the key.
     * <p>
     * <b>Calls to this method should be synchronized.</b>
     * 
     * @param key
     *            the subscription key
     * @param subscriber
     *            the subscriber <b>INSTANCE</b> to add
     * @return <code>true</code> if the subscriber was added, <code>false</code> if this
     *         <b>INSTANCE</b> has already been added against this key
     */
    @SuppressWarnings("unchecked")
    public boolean addSubscriberFor(SUBSCRIPTION_KEY key, SUBSCRIBER subscriber)
    {
        SUBSCRIBER[] current = this.subscribersPerKey.get(key);
        if (current == null)
        {
            current = (SUBSCRIBER[]) Array.newInstance(this.subscriberClass, 1);
            current[0] = subscriber;
        }
        else
        {
            if (ArrayUtils.containsInstance(current, subscriber))
            {
                return false;
            }
            SUBSCRIBER[] copy = (SUBSCRIBER[]) Array.newInstance(this.subscriberClass, current.length + 1);
            System.arraycopy(current, 0, copy, 0, current.length);
            copy[current.length] = subscriber;
            current = copy;
        }
        this.subscribersPerKey.put(key, current);
        return true;
    }

    /**
     * Remove a previously added subscriber from the list associated with the subscription key
     * <p>
     * <b>Calls to this method should be synchronized.</b>
     * 
     * @param key
     *            the subscription key
     * @param subscriber
     *            the subscriber <b>INSTANCE</b> to remove
     * @return <code>true</code> if the subscriber was removed, <code>false</code> if the subscriber
     *         <b>INSTANCE</b> was not found for the subscription key
     */
    @SuppressWarnings("unchecked")
    public boolean removeSubscriberFor(SUBSCRIPTION_KEY key, SUBSCRIBER subscriber)
    {
        SUBSCRIBER[] current = this.subscribersPerKey.get(key);
        if (current == null)
        {
            return false;
        }
        List<SUBSCRIBER> list = Arrays.asList(current);
        List<SUBSCRIBER> copy = new ArrayList<SUBSCRIBER>(list);
        final boolean removed = copy.remove(subscriber);
        if (copy.size() > 0)
        {
            this.subscribersPerKey.put(key,
                copy.toArray((SUBSCRIBER[]) Array.newInstance(this.subscriberClass, copy.size())));
        }
        else
        {
            this.subscribersPerKey.remove(key);
        }
        return removed;
    }

    @Override
    public String toString()
    {
        return "SubscriptionManager [" + this.subscribersPerKey + "]";
    }

    /**
     * Remove all subscribers for the key.
     * <p>
     * <b>Calls to this method should be synchronized.</b>
     * 
     * @param key
     *            the subscription key
     * @return the subscribers that were removed, <code>null</code> if there were no subscribers for
     *         this key
     */
    public SUBSCRIBER[] removeSubscribersFor(SUBSCRIPTION_KEY key)
    {
        return this.subscribersPerKey.remove(key);
    }

    /**
     * @return the set of subscription keys for this manager
     */
    public Set<SUBSCRIPTION_KEY> getAllSubscriptionKeys()
    {
        return Collections.unmodifiableSet(this.subscribersPerKey.keySet());
    }

    public void destroy()
    {
        this.subscribersPerKey.clear();
    }
}
