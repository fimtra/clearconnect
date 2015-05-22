/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.event;

/**
 * A listener that provides notifications when records are added or removed from a platform service
 * component.
 * 
 * @author Ramon Servadei
 */
public interface IRecordAvailableListener
{
    /**
     * Called when a record is added to a platform service component.
     * 
     * @param recordName
     *            the name of the record that was added to the platform service component.
     */
    void onRecordAvailable(String recordName);

    /**
     * Called when a record is removed from a platform service component.
     * 
     * @param recordName
     *            the name of the record that was removed from the platform service component.
     */
    void onRecordUnavailable(String recordName);
}