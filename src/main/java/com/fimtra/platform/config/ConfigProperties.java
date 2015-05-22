/*
 * Copyright (c) 2013 Ramon Servadei, Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config;

import com.fimtra.platform.PlatformCoreProperties;

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
