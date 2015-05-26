/*
 * Copyright (c) 2013 Paul Mackinlay, Ramon Servadei, Fimtra
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
package com.fimtra.platform.config.impl;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.platform.IPlatformServiceProxy;
import com.fimtra.platform.config.ConfigProperties;
import com.fimtra.platform.config.ConfigServiceProperties;
import com.fimtra.platform.config.IConfig;
import com.fimtra.platform.core.PlatformUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.is;

/**
 * The default implementation of {@link IConfig}.
 *
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
final class Config implements IConfig {
	private final File localConfigDir;
	private final String localServiceInstanceIdConfig;
	private final String serviceName;
	private final String serviceInstanceId;
	private final IPlatformServiceProxy proxyForConfigService;
	final ConcurrentMap<String, IValue> properties;
	final List<IConfigChangeListener> configChangeListeners;
	final MasterConfigChangeListener masterConfigChangeListener;

	Config(String serviceName, String serviceMemberName, IPlatformServiceProxy proxyForConfigService) {
		this(ConfigProperties.Values.LOCAL_CONFIG_DIR, serviceMemberName, serviceName, serviceMemberName, proxyForConfigService);
	}

	Config(String localConfigDir, String localServiceInstanceIdConfig, String serviceName, String serviceMemberName,
			IPlatformServiceProxy proxyForConfigService) {
		this.localConfigDir = new File(localConfigDir);
		this.localServiceInstanceIdConfig = localServiceInstanceIdConfig;
		this.serviceName = serviceName;
		this.serviceInstanceId = PlatformUtils.composePlatformServiceInstanceID(this.serviceName, serviceMemberName);
		this.proxyForConfigService = proxyForConfigService;
		this.properties = new ConcurrentHashMap<String, IValue>();
		this.configChangeListeners = new CopyOnWriteArrayList<IConfigChangeListener>();
		this.masterConfigChangeListener = new MasterConfigChangeListener(this.proxyForConfigService, this.serviceInstanceId);

		init();
	}

	public void destroy() {
		try {
			this.proxyForConfigService.removeRecordListener(this.masterConfigChangeListener, this.serviceName);
		} catch (Exception e) {
			Log.log(this, "Could not de-register masterConfigChangeListener for " + this.serviceName, e);
		}
		try {
			this.proxyForConfigService.removeRecordListener(this.masterConfigChangeListener, this.serviceInstanceId);
		} catch (Exception e) {
			Log.log(this, "Could not de-register masterConfigChangeListener for " + this.serviceInstanceId, e);
		}
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		destroy();
	}

	private void init() {
		IRecord serviceConfig = null;
		IRecord serviceInstanceIdConfig = null;
		final Set<String> allRecordNames = this.proxyForConfigService.getAllRecordNames();
		// Layer 1: service family config
		if (allRecordNames.contains(this.serviceName)) {
			serviceConfig = this.proxyForConfigService.getRecordImage(this.serviceName,
					ConfigServiceProperties.Values.DEFAULT_CONFIG_RPC_TIMEOUT_MILLIS);
			if (serviceConfig != null) {
				this.properties.putAll(serviceConfig);
				Log.log(this, "Service config: ", ObjectUtils.safeToString(serviceConfig));
			}
		}
		// Layer 2: service instance Id config
		if (allRecordNames.contains(this.serviceInstanceId)) {
			serviceInstanceIdConfig = this.proxyForConfigService.getRecordImage(this.serviceInstanceId,
					ConfigServiceProperties.Values.DEFAULT_CONFIG_RPC_TIMEOUT_MILLIS);
			if (serviceInstanceIdConfig != null) {
				this.properties.putAll(serviceInstanceIdConfig);
				Log.log(this, "Service instance config: " + serviceInstanceIdConfig.toString());
			}
		}
		// Layer 3: local config
		this.properties.putAll(readLocalProperties());

		// Listen for config record changes. This also calls back if a config record is created.
		this.proxyForConfigService.addRecordListener(this.masterConfigChangeListener, this.serviceName);
		this.proxyForConfigService.addRecordListener(this.masterConfigChangeListener, this.serviceInstanceId);
	}

	private Map<String, IValue> readLocalProperties() {
		ResourceBundle configResources = null;
		Map<String, IValue> localProperties = new HashMap<String, IValue>();
		try {
			ClassLoader classLoader = new URLClassLoader(new URL[] { this.localConfigDir.toURI().toURL() });
			configResources = ResourceBundle.getBundle(this.localServiceInstanceIdConfig, Locale.getDefault(), classLoader);
		} catch (Exception e) {
			// No resource file available: noop
		}
		if (configResources != null) {
			List<String> configKeys = Collections.list(configResources.getKeys());
			if (!configKeys.isEmpty()) {
				Log.log(this, "Using config override file: ", ObjectUtils.safeToString(this.localConfigDir),
						System.getProperty("file.separator"), this.localServiceInstanceIdConfig, ".properties");
				for (String configKey : configKeys) {
					String configValue = configResources.getString(configKey);
					localProperties.put(configKey, new TextValue(configValue));
					Log.log(this, "File config: ", configKey, "=", configValue);
				}
			}
		}
		return localProperties;
	}

	@Override
	public IValue getProperty(String propertyKey) {
		return this.properties.get(propertyKey);
	}

	@Override
	public Set<String> getPropertyKeys() {
		return Collections.unmodifiableSet(this.properties.keySet());
	}

	/**
	 * Adds a listener for config changes. When a config property is deleted the listener will be
	 * notified with a propertyKey and a null property value.
	 */
	@Override
	public void addConfigChangeListener(IConfigChangeListener listener) {
		boolean isListenersChanged = false;
		synchronized (this.configChangeListeners) {
			if (!this.configChangeListeners.contains(listener)) {
				isListenersChanged = this.configChangeListeners.add(listener);
			}
		}
		if (isListenersChanged) {
			// All entries in this.properties are immutable to this is like a deep copy
			Map<String, IValue> propertiesCopy = new HashMap<String, IValue>(this.properties);
			for (Entry<String, IValue> property : propertiesCopy.entrySet()) {
				listener.onPropertyChange(property.getKey(), property.getValue());
			}
		}
	}

	@Override
	public void removeConfigChangeListener(IConfigChangeListener listener) {
		this.configChangeListeners.remove(listener);
	}

	/**
	 * Responsible for updating internal map of properties and notifying of registered listeners.
	 *
	 * @author Ramon Servadei
	 * @author Paul Mackinlay
	 */
	final class MasterConfigChangeListener implements IRecordListener {

		private final IPlatformServiceProxy proxyForConfigService;
		private final String serviceInstanceId;

		public MasterConfigChangeListener(IPlatformServiceProxy proxyForConfigService, String serviceInstanceId) {
			super();
			this.proxyForConfigService = proxyForConfigService;
			this.serviceInstanceId = serviceInstanceId;
		}

		@Override
		public void onChange(IRecord imageCopy, IRecordChange atomicChange) {
			Log.log(this, ObjectUtils.safeToString(atomicChange));
			IRecord memberConfig = null;
			if (isMemberConfiCheckRequired(imageCopy)) {
				memberConfig = this.proxyForConfigService.getRecordImage(this.serviceInstanceId,
						ConfigServiceProperties.Values.DEFAULT_CONFIG_RPC_TIMEOUT_MILLIS);
			}
			for (Entry<String, IValue> entry : atomicChange.getRemovedEntries().entrySet()) {
				String key = entry.getKey();
				Config.this.properties.remove(key);
				if (isConfigChangeApplicable(imageCopy, key, memberConfig)) {
					safeNotifyConfigChanges(key, null);
					Log.log(this, "Config deleted : ", key, "=[null]");
				}
			}
			for (Entry<String, IValue> entry : atomicChange.getPutEntries().entrySet()) {
				String key = entry.getKey();
				IValue value = entry.getValue();
				final IValue put = Config.this.properties.put(key, value);
				if (!is.eq(put, value) && isConfigChangeApplicable(imageCopy, key, memberConfig)) {
					safeNotifyConfigChanges(key, value);
					Log.log(this, "Config updated : ", key, "=", ObjectUtils.safeToString(value));
				}
			}
		}

		private void safeNotifyConfigChanges(String propertyKey, IValue propertyValue) {
			for (IConfigChangeListener listener : Config.this.configChangeListeners) {
				try {
					listener.onPropertyChange(propertyKey, propertyValue);
				} catch (Exception e) {
					Log.log(this,
							"Could not notify " + ObjectUtils.safeToString(listener) + " with property change for "
									+ ObjectUtils.safeToString(propertyKey) + "=" + ObjectUtils.safeToString(propertyValue), e);
				}
			}
		}

		// The config change is not applicable if it is family config and it is overwritten in member config
		private boolean isConfigChangeApplicable(IRecord record, String recordKey, IRecord memberConfig) {
			if (isMemberConfiCheckRequired(record)) {
				if (memberConfig != null && memberConfig.containsKey(recordKey)) {
					Log.log(this, "Family config with key [", recordKey,
							"] is overwritten by member config, notification is not applicable.");
					return false;
				}
			}
			return true;
		}

		private boolean isMemberConfiCheckRequired(IRecord record) {
			String[] instanceNameParts = PlatformUtils.decomposePlatformServiceInstanceID(this.serviceInstanceId);
			if (record != null && record.getName().equals(instanceNameParts[0])) {
				return true;
			}
			return false;
		}
	}

	@Override
	public String toString() {
		return "Config [serviceInstanceId=" + this.serviceInstanceId + ", properties=" + this.properties + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.properties == null) ? 0 : this.properties.hashCode());
		result = prime * result + ((this.serviceInstanceId == null) ? 0 : this.serviceInstanceId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (is.same(this, obj)) {
			return true;
		}
		if (is.differentClass(this, obj)) {
			return false;
		}
		final Config other = (Config) obj;
		return is.eq(this.serviceInstanceId, other.serviceInstanceId) && is.eq(this.properties, other.properties);
	}
}

/**
 * Simple implementation for the ConfigService
 *
 * @author Ramon Servadei
 */
class SimplePropertiesConfig implements IConfig {
	final Properties properties;

	SimplePropertiesConfig(Properties properties) {
		super();
		this.properties = properties;
	}

	@Override
	public IValue getProperty(String propertyKey) {
		final String property = this.properties.getProperty(propertyKey);
		if (property != null) {
			return new TextValue(property);
		}
		return null;
	}

	@Override
	public Set<String> getPropertyKeys() {
		final Set<Object> keySet = this.properties.keySet();
		Set<String> keys = new HashSet<String>();
		for (Object object : keySet) {
			keys.add(ObjectUtils.safeToString(object));
		}
		return keys;
	}

	@Override
	public void addConfigChangeListener(IConfigChangeListener configChangeListener) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void removeConfigChangeListener(IConfigChangeListener configChangeListener) {
		throw new UnsupportedOperationException();
	}

}