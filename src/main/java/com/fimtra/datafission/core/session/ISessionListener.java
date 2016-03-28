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
 * Receives callbacks when session status changes occur. This is only active on the proxy side of a
 * session context.
 * 
 * @author Ramon Servadei
 */
public interface ISessionListener
{
    /**
     * Called when a session is opened between a proxy and a publisher. Opened means the session is
     * valid and communication can/has begun.
     * 
     * @param sessionContext
     *            the session context for the session ID
     * @param sessionId
     *            the ID of the session that has opened
     */
    void onSessionOpen(String sessionContext, String sessionId);

    /**
     * Called when a session between a proxy and a publisher is closed. Closed means the session is
     * dead and there is no more communication between the end points.
     * 
     * @param sessionContext
     *            the session context for the session ID
     * @param sessionId
     *            the ID of the session that has closed
     */
    void onSessionClosed(String sessionContext, String sessionId);
}
