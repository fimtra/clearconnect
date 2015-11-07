/*
 * Copyright (c) 2014 Ramon Servadei 
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
package com.fimtra.channel;

import java.util.Arrays;

/**
 * Provides an {@link EndPointAddress} in a round-robin style from an internal static array.
 * 
 * @author Ramon Servadei
 */
public class StaticEndPointAddressFactory implements IEndPointAddressFactory
{
    final EndPointAddress[] endPoints;
    int current;

    public StaticEndPointAddressFactory(EndPointAddress... endPoints)
    {
        this.endPoints = endPoints;
    }

    @Override
    public EndPointAddress next()
    {
        EndPointAddress next = this.endPoints[this.current % this.endPoints.length];
        this.current += 1;
        return next;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.endPoints);
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
        StaticEndPointAddressFactory other = (StaticEndPointAddressFactory) obj;
        if (!Arrays.equals(this.endPoints, other.endPoints))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "StaticEndPointFactory [endPoints=" + Arrays.toString(this.endPoints) + ", current=" + this.current + "]";
    }

}
