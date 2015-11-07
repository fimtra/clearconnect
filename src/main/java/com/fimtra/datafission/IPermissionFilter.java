/*
 * Copyright (c) 2015 Ramon Servadei 
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
package com.fimtra.datafission;

/**
 * A permission filter is used to examine subscriptions for records and prevent records being
 * subscribed for if the observer does not present a valid permission token. The permission token is
 * arbitrary and up to the permission implementation; its a string that can be interpreted by filter
 * implementations.
 * 
 * @see IObserverContext#addObserver(String, IRecordListener, String...)
 * @author Ramon Servadei
 */
public interface IPermissionFilter
{
    public static final String DEFAULT_PERMISSION_TOKEN = "<null>";

    /**
     * Examine the permission token and assess whether it is valid for a subscription to the record
     * name.
     * 
     * @param permissionToken
     *            the permission token to check
     * @param recordName
     *            the name of the record being subscribed for with the permission token
     * @return <code>true</code> if the record can be subscribed for using the permission token,
     *         <code>false</code> otherwise
     */
    boolean accept(String permissionToken, String recordName);
}
