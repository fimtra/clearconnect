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
package com.fimtra.platform.event;

import java.util.Set;

import com.fimtra.platform.IDataRadar;
import com.fimtra.util.is;

/**
 * A listener that receives events from one or more {@link IDataRadar} instances.
 * 
 * @author Ramon Servadei
 */
public interface IDataRadarListener
{
    /**
     * Encapsulates a signature match for a record in a service instance
     * 
     * @author Ramon Servadei
     */
    public static final class SignatureMatch
    {
        private final int hashCode;
        private final String recordName;
        private final String serviceInstanceId;

        public SignatureMatch(String recordName, String serviceInstanceId)
        {
            super();
            this.recordName = recordName;
            this.serviceInstanceId = serviceInstanceId.intern();

            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.recordName == null) ? 0 : this.recordName.hashCode());
            result = prime * result + ((this.serviceInstanceId == null) ? 0 : this.serviceInstanceId.hashCode());
            this.hashCode = result;
        }

        @Override
        public int hashCode()
        {
            return this.hashCode;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (is.same(this, obj))
            {
                return true;
            }
            if (is.differentClass(this, obj))
            {
                return false;
            }
            final SignatureMatch other = (SignatureMatch) obj;
            return is.eq(this.recordName, other.recordName) && is.eq(this.serviceInstanceId, other.serviceInstanceId);
        }

        public String getRecordName()
        {
            return this.recordName;
        }

        public String getServiceInstanceId()
        {
            return this.serviceInstanceId;
        }

        @Override
        public String toString()
        {
            return "SignatureMatch [recordName=" + this.recordName + ", serviceInstanceId=" + this.serviceInstanceId
                + "]";
        }
    }

    /**
     * Triggered when data signatures are detected and lost by a data radar
     * 
     * @param radar
     *            the radar the event originated from
     * @param signaturesFound
     *            the signatures that now appear on this radar
     * @param signaturesLost
     *            the signatures that have disappeared from this radar
     */
    void onRadarChange(IDataRadar radar, Set<SignatureMatch> signaturesFound, Set<SignatureMatch> signaturesLost);
}
