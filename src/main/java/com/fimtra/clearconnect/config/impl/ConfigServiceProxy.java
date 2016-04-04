/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei, Fimtra
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.clearconnect.config.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fimtra.clearconnect.IPlatformRegistryAgent;
import com.fimtra.clearconnect.IPlatformServiceProxy;
import com.fimtra.clearconnect.config.ConfigServiceProperties;
import com.fimtra.clearconnect.config.IConfig;
import com.fimtra.clearconnect.config.IConfigManager;
import com.fimtra.clearconnect.config.IConfigServiceProxy;
import com.fimtra.clearconnect.core.PlatformUtils;
import com.fimtra.datafission.IValue;

/**
 * Implementation of {@link IConfigServiceProxy}. This is a tailored view of the config services
 * {@link IPlatformServiceProxy}.
 *
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
public final class ConfigServiceProxy implements IConfigServiceProxy
{
    static final IConfigManager DEFAULT_MANAGER = new IConfigManager()
    {
        @Override
        public boolean deleteMemberConfig(String configKey)
        {
            return false;
        }

        @Override
        public boolean deleteFamilyConfig(String configKey)
        {
            return false;
        }

        @Override
        public boolean createOrUpdateMemberConfig(String configKey, IValue configValue)
        {
            return false;
        }

        @Override
        public boolean createOrUpdateFamilyConfig(String configKey, IValue configValue)
        {
            return false;
        }
    };

    static final IConfig DEFAULT_CONFIG = new IConfig()
    {
        @Override
        public void removeConfigChangeListener(IConfigChangeListener configChangeListener)
        {
        }

        @Override
        public Set<String> getPropertyKeys()
        {
            return Collections.emptySet();
        }

        @Override
        public IValue getProperty(String propertyKey)
        {
            return null;
        }

        @Override
        public void addConfigChangeListener(IConfigChangeListener configChangeListener)
        {

        }
    };

    private static ConfigServiceProxy DEFAULT_PROXY;

    /**
     * Get the default (shared) instance that connects to the ConfigService. This can block waiting
     * for the ConfigService to be available.
     * 
     * @param agent
     *            the agent to use to create the connection
     * @return the proxy
     * @throws RuntimeException
     *             if the standard ConfigService is not running
     */
    public static synchronized ConfigServiceProxy getDefaultInstance(IPlatformRegistryAgent agent)
    {
        if (DEFAULT_PROXY == null)
        {
            agent.waitForPlatformService(IConfigServiceProxy.CONFIG_SERVICE);

            DEFAULT_PROXY = new ConfigServiceProxy(agent.getPlatformServiceProxy(IConfigServiceProxy.CONFIG_SERVICE));
        }
        return DEFAULT_PROXY;
    }

    /**
     * Get the default instance or a dummy if there is no ConfigService running. This does not block
     * to wait for the ConfigService - if it is available it uses it, if it is not then it returns a
     * dummy.
     * 
     * @param agent
     *            the agent to use to create the proxy connection
     * @return the proxy
     */
    public static synchronized IConfigServiceProxy getDefaultInstanceOrDummy(IPlatformRegistryAgent agent)
    {
        if (DEFAULT_PROXY == null)
        {
            final IPlatformServiceProxy platformServiceProxy =
                agent.getPlatformServiceProxy(IConfigServiceProxy.CONFIG_SERVICE);
            if (platformServiceProxy != null)
            {
                DEFAULT_PROXY = new ConfigServiceProxy(platformServiceProxy);
            }
            else
            {
                return new IConfigServiceProxy()
                {
                    @Override
                    public IConfigManager getConfigManager(String serviceFamily, String serviceMember)
                    {
                        return DEFAULT_MANAGER;
                    }

                    @Override
                    public IConfig getConfig(String serviceFamily, String serviceMember)
                    {
                        return DEFAULT_CONFIG;
                    }
                };
            }
        }
        return DEFAULT_PROXY;
    }

    private final IPlatformServiceProxy proxyForConfigService;
    private final Map<String, ConfigManager> configManagers;

    public ConfigServiceProxy(IPlatformServiceProxy proxyForConfigService)
    {
        this.proxyForConfigService = proxyForConfigService;
        this.configManagers = new HashMap<String, ConfigManager>(2);
    }

    @Override
    public IConfig getConfig(String serviceFamily, String serviceMember)
    {
        return new Config(serviceFamily, serviceMember, this.proxyForConfigService);
    }

    @Override
    public IConfigManager getConfigManager(String serviceFamily, String serviceMember)
    {
        String key = PlatformUtils.composePlatformServiceInstanceID(serviceFamily, serviceMember);
        if (!this.configManagers.containsKey(key))
        {
            this.configManagers.put(key, new ConfigManager(this.proxyForConfigService, serviceFamily, serviceMember,
                ConfigServiceProperties.Values.DEFAULT_CONFIG_MANAGER_RPC_TIMEOUT_MILLIS));
        }
        return this.configManagers.get(key);
    }
}
