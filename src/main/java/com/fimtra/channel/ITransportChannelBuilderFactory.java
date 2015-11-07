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
 * A factory that creates {@link ITransportChannelBuilder} objects that build specific
 * {@link ITransportChannel} instances.
 * <p>
 * The factory provides the ability to have a set of redundant connections (e.g. round-robin
 * redundant connections)
 * 
 * @author Ramon Servadei
 */
public interface ITransportChannelBuilderFactory
{
    /**
     * Each call to this method will return the 'next' builder to use.
     * 
     * @return the next builder to use
     */
    ITransportChannelBuilder nextBuilder();
}
