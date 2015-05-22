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
 * A listener that provides notifications when records are 'connected', 'connecting' or
 * 'disconnected'. Initially a record has a 'connected' state (this reflects the proxy being
 * connected). When a disruption occurs in the connection, each record status moves to 'connecting'.
 * If the connection succeeds, the state moves to 'connected'. If the connection fails, the state
 * moves to 'disconnected'. When a re-connection is attempted by the proxy, the status moves again
 * to 'connecting'.
 * <p>
 * This effectively gives a reflection of the network connection between the component and the
 * service and provides the means to detect when data for a record is valid or stale.
 * 
 * @author Ramon Servadei
 */
public interface IRecordConnectionStatusListener
{
    /**
     * Called when the record is connected and its data can be considered valid
     * 
     * @param recordName
     *            the name of the record that has been connected
     */
    void onRecordConnected(String recordName);

    /**
     * Called when the record is (re)connecting - data is still not considered valid. After this
     * call-back, either {@link #onRecordConnected(String)} or {@link #onRecordDisconnected(String)}
     * will be called, depending on the connection outcome.
     * 
     * @param recordName
     *            the name of the record that is being re-connected
     */
    void onRecordConnecting(String recordName);

    /**
     * Called when a record is disconnected (network connection lost or the remote service is gone)
     * 
     * @param recordName
     *            the name of the record that has been disconnected
     */
    void onRecordDisconnected(String recordName);
}