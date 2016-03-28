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

import java.util.HashMap;
import java.util.Map;

import com.fimtra.datafission.ISessionProtocol;

/**
 * The central point for registering and obtaining {@link ISessionManager},
 * {@link ISessionAttributesProvider} and {@link ISessionListener} instances for specific
 * "session contexts". A session context is identified by a unique name and describes a grouping of
 * one or more sessions that are separated from other sessions in other session contexts. A session
 * exists between a proxy and a publisher.
 * <p>
 * A session is established (synchronised) between a proxy and publisher by the protocol implemented
 * by the {@link ISessionProtocol}.
 * 
 * @author Ramon Servadei
 */
public class SessionContexts
{
    final static Map<String, ISessionManager> managers = new HashMap<String, ISessionManager>();

    final static Map<String, ISessionAttributesProvider> providers = new HashMap<String, ISessionAttributesProvider>();

    final static Map<String, ISessionListener> listeners = new HashMap<String, ISessionListener>();

    private static final String[] DEFAULT_ATTRIBUTES = new String[] { "defaults" };

    private static final ISessionManager DEFAULT_MANAGER = new ISessionManager()
    {
        @Override
        public String createSession(String[] details)
        {
            // TODO NOTE: the default manager provides an OPEN access system
            return "default-session";
        }

        @Override
        public void sessionEnded(String sessionId)
        {
            // noop
        }
    };

    private static final ISessionListener DEFAULT_LISTENER = new ISessionListener()
    {
        @Override
        public void onSessionOpen(String sessionContext, String sessionId)
        {
            // noop
        }

        @Override
        public void onSessionClosed(String sessionContext, String sessionId)
        {
            // noop
        }
    };

    /**
     * Register a manager that operates in the passed in session context.
     * 
     * @param sessionContextName
     *            the name of the session context the manager operates in
     * @param manager
     *            the session manager to register
     */
    public static void registerSessionManager(String sessionContextName, ISessionManager manager)
    {
        synchronized (managers)
        {
            managers.put(sessionContextName, manager);
        }
    }

    /**
     * Register a session attributes provider for the passed in session context.
     * 
     * @param sessionContextName
     *            the name of the session context the attributes provider operates in
     * @param provider
     *            the provider to register
     */
    public static void registerSessionProvider(String sessionContextName, ISessionAttributesProvider provider)
    {
        synchronized (providers)
        {
            providers.put(sessionContextName, provider);
        }
    }

    /**
     * Register a session listener for the passed in session context.
     * 
     * @param sessionContextName
     *            the name of the session context the listener is interested in
     * @param listener
     *            the listener to register
     */
    public static void registerSessionListener(String sessionContextName, ISessionListener listener)
    {
        synchronized (listeners)
        {
            listeners.put(sessionContextName, listener);
        }
    }

    /**
     * Get a session manager for the required session context.
     * 
     * @param sessionContextName
     *            the name of the session context the manager will operate over
     * @return the session manager for the session context
     */
    public static ISessionManager getSessionManager(String sessionContextName)
    {
        final ISessionManager listener;
        synchronized (managers)
        {
            listener = managers.get(sessionContextName);
        }
        if (listener == null)
        {
            return DEFAULT_MANAGER;
        }
        return listener;
    }

    /**
     * Get the session attributes for initiating a session with the passed in session context
     * 
     * @param sessionContextName
     *            the name of the session context the attributes are required for
     * @return the attributes for constructing a session with the session context
     */
    public static String[] getSessionAttributes(String sessionContextName)
    {
        final ISessionAttributesProvider provider;
        synchronized (providers)
        {
            provider = providers.get(sessionContextName);
        }
        if (provider == null)
        {
            return DEFAULT_ATTRIBUTES;
        }
        return provider.getSessionAttributes();
    }

    /**
     * Get the session listener registered for the session context
     * 
     * @param sessionContextName
     *            the name of the session context
     * @return the listener associated with the session context
     */
    public static ISessionListener getSessionListener(String sessionContextName)
    {
        final ISessionListener listener;
        synchronized (listeners)
        {
            listener = listeners.get(sessionContextName);
        }
        if (listener == null)
        {
            return DEFAULT_LISTENER;
        }
        return listener;
    }
}
