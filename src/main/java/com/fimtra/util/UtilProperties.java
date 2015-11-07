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

/**
 * Defines the properties and property keys used by Util
 * 
 * @author Paul Mackinlay
 * @author Ramon Servadei
 */
public abstract class UtilProperties
{
    private UtilProperties()
    {
    }

    /**
     * The names of the properties
     * 
     * @author Ramon Servadei
     */
    public static interface Names
    {
        String BASE = "util.";

        /**
         * The system property key that defines the log directory.<br>
         * E.g. <code>-Dutil.logDir=/path/to/log/directory</code>
         */
        String SYSTEM_PROPERTY_LOG_DIR = BASE + "logDir";

        /**
         * The system property name to define if log messages are written to std.err (in addition to
         * the log file). <br>
         * <b>SETTING THIS TO TRUE HAS A SEVERE PERFORMANCE IMPACT.</b><br>
         * E.g. <code>-Dutil.logToStdErr=true</code>
         */
        String LOG_TO_STDERR = BASE + "logToStdErr";

        /**
         * The system property name to define if the {@link LowGcLinkedList} should be used by
         * {@link CollectionUtils#newLinkedList()}. <br>
         * E.g. <code>-Dutil.useLowGcLinkedList=true</code>
         */
        String USE_LOW_GC_LINKEDLIST = BASE + "useLowGcLinkedList";

        /**
         * The system property name to define if the thread dumps use the same file or a rolling
         * file.<br>
         * E.g. <code>-Dutil.useRollingThreaddumpFile=true</code>
         */
        String USE_ROLLING_THREADDUMP_FILE = BASE + "useRollingThreaddumpFile";
    }

    /**
     * The values of the properties described in {@link Names}
     * 
     * @author Ramon Servadei
     */
    public static interface Values
    {
        /**
         * Determines if log messages are written to std.err. Default is <code>false</code>
         * 
         * @see Names#LOG_TO_STDERR
         */
        boolean LOG_TO_STDERR = Boolean.parseBoolean(System.getProperty(Names.LOG_TO_STDERR, "false"));

        /**
         * The log directory. Default is <tt>./logs</tt>
         * 
         * @see Names#SYSTEM_PROPERTY_LOG_DIR
         */
        String LOG_DIR = System.getProperty(UtilProperties.Names.SYSTEM_PROPERTY_LOG_DIR, "logs");

        /**
         * Determines if the {@link LowGcLinkedList} is used. Default is <code>true</code>
         * 
         * @see Names#USE_LOW_GC_LINKEDLIST
         */
        boolean USE_LOW_GC_LINKEDLIST = Boolean.parseBoolean(System.getProperty(Names.USE_LOW_GC_LINKEDLIST, "true"));

        /**
         * Defines if a rolling thread dump file is used. Default is <code>false</code>
         * 
         * @see Names#USE_ROLLING_THREADDUMP_FILE
         */
        boolean USE_ROLLING_THREADDUMP_FILE = Boolean.parseBoolean(System.getProperty(Names.USE_ROLLING_THREADDUMP_FILE,
            "false"));
    }

}
