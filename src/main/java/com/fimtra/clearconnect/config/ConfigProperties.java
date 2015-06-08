/*
 * Copyright (c) 2013 Ramon Servadei, Paul Mackinlay, Fimtra
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
package com.fimtra.clearconnect.config;

import com.fimtra.clearconnect.PlatformCoreProperties;

/**
 * Defines the system-level properties and property keys used for the {@link IConfigServiceProxy}
 * and {@link IConfig}.
 * 
 * @author Ramon Servadei
 */
public abstract class ConfigProperties
{

	/**
	 * The names of config properties
	 * 
	 * @author Paul Mackinlay
	 */
	public static interface Names {

		/**
		 * The system property name to define the directory for local configuration for a service
		 * instance.
		 * 
		 * @see IConfig
		 */
		public static final String PROPERTY_NAME_LOCAL_CONFIG_DIR = BASE + "localConfigDir";
	}

	/**
	 * The values of config properties described in {@link Names}
	 * 
	 * @author Paul Mackinlay
	 */
	public static interface Values {

		/**
		 * The path for the local configuration directory. Defaults to the working directory.
		 * 
		 * @see IConfig
		 */
		public static final String LOCAL_CONFIG_DIR = System.getProperty(Names.PROPERTY_NAME_LOCAL_CONFIG_DIR,
				System.getProperty("user.dir"));
	}

    /**
     * The base token for the name-space for configService specific property names.
     */
    private static final String BASE = PlatformCoreProperties.Names.BASE + "config.";
    
    /**
     * The config key for the service instance host field in the {@link IConfig} for a service
     * instance.
     */
    public static final String CONFIG_KEY_INSTANCE_HOST = "ServiceInstanceHost";
    
    /**
     * The config key for the service instance port field in the {@link IConfig} for a service
     * instance.
     */
    public static final String CONFIG_KEY_INSTANCE_PORT = "ServiceInstancePort";
    
    /**
     * The config key for the service instance redundancy mode field in the {@link IConfig} for a
     * service instance.
     */
    public static final String CONFIG_KEY_INSTANCE_REDUNDANCY_MODE = "ServiceInstanceRedundancyMode";
    
    /**
     * The config key for the service instance wire protocol field in the {@link IConfig} for a
     * service instance.
     */
    public static final String CONFIG_KEY_INSTANCE_WIRE_PROTOCOL = "ServiceInstanceWireProtocol";

    private ConfigProperties()
    {
    }
}
