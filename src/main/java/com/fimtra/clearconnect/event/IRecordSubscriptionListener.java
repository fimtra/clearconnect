/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
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
package com.fimtra.clearconnect.event;

import com.fimtra.clearconnect.IPlatformServiceComponent;
import com.fimtra.util.is;

/**
 * A listener that provides notifications when records are subscribed
 * <h2>Threading</h2>
 * <ul>
 * <li>When a listener instance is registered with only one {@link IPlatformServiceComponent}, the
 * callback methods are guaranteed to not execute concurrently. However, they may be executed by
 * different threads.
 * <li>When a listener instance is registered with multiple components, the callback methods may
 * execute concurrently.
 * </ul>
 * 
 * @author Ramon Servadei
 */
public interface IRecordSubscriptionListener
{
    public class SubscriptionInfo
    {
        private final String recordName;
        private final int currentSubscriberCount, previousSubscriberCount;

        public SubscriptionInfo(String recordName, int currentSubscriberCount, int previousSubscriberCount)
        {
            super();
            this.recordName = recordName;
            this.currentSubscriberCount = currentSubscriberCount;
            this.previousSubscriberCount = previousSubscriberCount;
        }

        public String getRecordName()
        {
            return this.recordName;
        }

        public int getCurrentSubscriberCount()
        {
            return this.currentSubscriberCount;
        }

        public int getPreviousSubscriberCount()
        {
            return this.previousSubscriberCount;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + this.currentSubscriberCount;
            result = prime * result + this.previousSubscriberCount;
            result = prime * result + ((this.recordName == null) ? 0 : this.recordName.hashCode());
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
            IRecordSubscriptionListener.SubscriptionInfo other = (IRecordSubscriptionListener.SubscriptionInfo) obj;
            return is.eq(this.currentSubscriberCount, other.currentSubscriberCount)
                && is.eq(this.previousSubscriberCount, other.previousSubscriberCount)
                && is.eq(this.recordName, other.recordName);
        }

        @Override
        public String toString()
        {
            return "SubscriptionInfo [" + this.recordName + ", " + this.currentSubscriberCount + "("
                + this.previousSubscriberCount + ")]";
        }
    }

    /**
     * Called when the named record receives a subscription change (either a subscriber has been
     * added or removed).
     * 
     * @param subscriptionInfo
     *            holds:
     *            <ul>
     *            <li>the name of the record that has a subscription count change
     *            <li>the current number of subscribers
     *            <li>the previous number of subscribers
     *            </ul>
     */
    void onRecordSubscriptionChange(IRecordSubscriptionListener.SubscriptionInfo subscriptionInfo);
}