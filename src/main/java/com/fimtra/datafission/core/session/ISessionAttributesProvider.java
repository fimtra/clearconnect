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
package com.fimtra.datafission.core.session;

/**
 * Provides the attributes for initiating a session (e.g. username, password etc). These attributes
 * are scoped to a specific session context.
 * 
 * @see ISessionManager#createSession(String[])
 * @author Ramon Servadei
 */
public interface ISessionAttributesProvider
{
    /**
     * @return the attributes for the session, e.g. username, password and/or other information
     */
    String[] getSessionAttributes();
}
