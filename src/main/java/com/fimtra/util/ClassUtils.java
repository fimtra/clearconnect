/*
 * Copyright (c) 2014 Paul Mackinlay, Ramon Servadei 
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
package com.fimtra.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Utility methods for Class'
 * 
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
public abstract class ClassUtils {

    /**
     * Filter keys for getting the Fimtra version.
     */
    public static final List<String> fimtraVersionKeys = Arrays.asList("Version", "Built-By");

    private ClassUtils() {
    }

    /**
     * Gets filtered manifest information for the clazz. Only results with the filtered keys will be
     * returned.
     */
    public static final String getFilteredManifestInfo(Class<?> clazz, Collection<String> filterKeys) {
        URL url = clazz.getClassLoader().getResource(JarFile.MANIFEST_NAME);
        return getManifestEntriesAsString(url, filterKeys);
    }

    /**
     * Gets all the manifest information for the clazz.
     */
    public static final String getManifestInfo(Class<?> clazz) {
        return getFilteredManifestInfo(clazz, null);
    }

    /**
     * Get the keys and values out of the manifest pointed to by the Url
     * <p>
     * Format in the string for each entry found is <tt>key: value{newline}</tt>
     * 
     * @return a string with the keys and values found in the manifest Url, or "" (empty string)
     */
    public static final String getManifestEntriesAsString(URL url, Collection<String> manifestKeys)
    {     
        StringBuilder manifestBuilder = new StringBuilder();
        InputStream inputStream = null;
        try {
            inputStream = url.openStream();
            if (inputStream != null) {
                Manifest manifest = new Manifest(inputStream);
                Attributes mainAttribs = manifest.getMainAttributes();
                for (Entry<Object, Object> entry : mainAttribs.entrySet()) {
                    String key = StringUtils.stripToEmpty(ObjectUtils.safeToString(entry.getKey()));
                    if (manifestKeys == null || manifestKeys.contains(key)) {
                        manifestBuilder.append(key).append(": ").append(ObjectUtils.safeToString(entry.getValue())).append(SystemUtils.lineSeparator());
                    }
                }
            }
        } catch (Exception e) {
            // ignore
        } finally {
            try {
                if(inputStream != null)
                {
                    inputStream.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
        return manifestBuilder.toString();
    }
}
