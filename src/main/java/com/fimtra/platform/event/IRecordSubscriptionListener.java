/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.event;

import com.fimtra.util.is;

/**
 * A listener that provides notifications when records are subscribed
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
            return "SubscriptionInfo [recordName=" + this.recordName + ", currentSubscriberCount="
                + this.currentSubscriberCount + ", previousSubscriberCount=" + this.previousSubscriberCount + "]";
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