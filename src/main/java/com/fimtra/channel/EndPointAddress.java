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

/**
 * Represents the end-point that a channel connects to. An end-point has a node and port.
 * <p>
 * This is synonymous with a socket.
 * 
 * @author Ramon Servadei
 */
public final class EndPointAddress
{
    final String node;
    final int port;

    public EndPointAddress(String node, int port)
    {
        super();
        this.node = node;
        this.port = port;
    }

    public String getNode()
    {
        return this.node;
    }

    public int getPort()
    {
        return this.port;
    }

    @Override
    public String toString()
    {
        return "EndPoint [node=" + this.node + ", port=" + this.port + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.node == null) ? 0 : this.node.hashCode());
        result = prime * result + this.port;
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
        EndPointAddress other = (EndPointAddress) obj;
        if (this.node == null)
        {
            if (other.node != null)
                return false;
        }
        else if (!this.node.equals(other.node))
            return false;
        if (this.port != other.port)
            return false;
        return true;
    }
}
