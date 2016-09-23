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
 * A component that manages session events occurring for connections
 * 
 * @author Ramon Servadei
 */
public interface ISessionManager
{
    /**
     * Called when an inbound connection has been completed. Creating a session is the first action
     * performed on the new connection and can result in the connection either continuing or being
     * disconnected.
     * <p>
     * Given the array of String details, the manager creates a unique session ID to represent the
     * logical session that this represents
     * 
     * @param sessionAttributes
     *            the attributes for the session, e.g. username, password and/or other pieces of
     *            information for the manager to validate/create a session.
     * @return a string describing a unique session within this manager, <code>null</code> if no
     *         session is created (i.e. validation has failed)
     */
    String createSession(String[] sessionAttributes);

    /**
     * Called when a previously valid inbound connection closes. This informs the manager that the
     * session with the sessionId has ended.
     * 
     * @param sessionId
     *            the session ID returned from {@link #createSession(String[])}
     */
    void sessionEnded(String sessionId);
}
