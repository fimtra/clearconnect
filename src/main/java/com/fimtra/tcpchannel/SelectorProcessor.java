/*
 * Copyright (c) 2013 Ramon Servadei 
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
package com.fimtra.tcpchannel;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.ThreadUtils;

/**
 * A SelectorProcessor handles a single operation (read, write, accept, connect) for many
 * {@link SelectableChannel} objects. It has a single daemon thread that processes a single
 * {@link Selector}.
 * <p>
 * {@link SelectableChannel} objects are registered with the SelectorProcessor along with a
 * {@link Runnable} that will be invoked whenever the operation this SelectorProcessor handles
 * becomes available for the channel.
 * <p>
 * This provides scaling for multiple sockets. Typically there will only be 2 SelectorProcessor
 * objects handling read and write operations across any number of sockets encapsulated by
 * {@link SocketChannel} objects.
 * 
 * @see TcpChannelUtils#READER
 * @see TcpChannelUtils#WRITER
 * @see TcpChannelUtils#ACCEPT_PROCESSOR
 * @see TcpChannelUtils#CONNECTION_PROCESSOR
 * @author Ramon Servadei
 */
final class SelectorProcessor implements Runnable
{
    final Selector selector;

    private final int selectorProcessorOperation;

    private final Thread processor;

    private boolean processorActive;

    /**
     * When registering a {@link SelectableChannel} with the processor's selector, this is used to
     * prevent the processor from blocking the selector to allow the registration to occur.
     */
    volatile CountDownLatch registrationLatch;

    /**
     * @param name
     *            the name for the daemon thread
     * @param operation
     *            one of {@link SelectionKey} operation constants, e.g.
     *            {@link SelectionKey#OP_WRITE}
     */
    SelectorProcessor(String name, int operation)
    {
        super();
        this.processorActive = true;
        this.selectorProcessorOperation = operation;
        try
        {
            this.selector = Selector.open();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not create SelectorProcessor " + name, e);
        }
        this.processor = ThreadUtils.newThread(this, name);
        this.processor.setDaemon(true);
        this.processor.start();
    }

    /**
     * Clean up resources allocated and remove any registered {@link SelectableChannel} objects from
     * the internal {@link Selector}
     */
    public void destroy()
    {
        this.processorActive = false;
        try
        {
            this.selector.close();
        }
        catch (IOException e)
        {
            Log.log(this, this + " encountered problem closing selector", e);
        }
    }

    /**
     * Processes the internal {@link Selector} and executes the {@link Runnable} attachment of any
     * {@link SelectionKey} objects that become selectable by the selector.
     */
    @Override
    public void run()
    {
        try
        {
            Iterator<SelectionKey> iterator;
            Set<SelectionKey> selectedKeys;
            SelectionKey selectionKey;
            while (this.processorActive)
            {
                try
                {
                    this.selector.select();
                    if (!this.processorActive)
                    {
                        return;
                    }
                    if(this.registrationLatch != null)
                    {
                        this.registrationLatch.await();
                    }
                    
                    try
                    {
                        selectedKeys = this.selector.selectedKeys();
                    }
                    catch (ClosedSelectorException e)
                    {
                        return;
                    }
                    iterator = selectedKeys.iterator();
                    while (iterator.hasNext())
                    {
                        selectionKey = iterator.next();
                        try
                        {
                            ((Runnable) selectionKey.attachment()).run();
                        }
                        catch (Exception e)
                        {
                            Log.log(this, this + " could not process the Runnable for " + selectionKey.channel(), e);
                        }
                    }
                    selectedKeys.clear();
                }
                catch (Exception e)
                {
                    Log.log(this, this + " encountered problem during run loop", e);
                }
            }
        }
        finally
        {
            Log.log(this, ObjectUtils.safeToString(this), " has finished");
        }

    }

    @Override
    public String toString()
    {
        return "SelectorProcessor [" + this.processor.getName() + "]";
    }

    /**
     * Cancel the channel's registration with this SelectorProcessor. Internally, the
     * {@link SelectionKey} for the channel argument will be cancelled.
     */
    void cancel(SelectableChannel channel)
    {
        final SelectionKey key = channel.keyFor(this.selector);
        if (key != null)
        {
            key.cancel();
        }
    }

    /**
     * Register the channel with the internal {@link Selector} of this SelectorProcessor. The
     * Runnable argument is attached to the {@link SelectionKey} that the {@link Selector} creates
     * for this registration. This Runnable object will be invoked when the operation that this
     * SelectorProcessor handles becomes available for the channel.
     * 
     * @return <code>true</code> if the registration completed, <code>false</code> if the internal
     *         selector is not open
     * @throws ClosedChannelException
     */
    synchronized boolean register(SelectableChannel channel, Runnable operationTask) throws ClosedChannelException
    {
        if (!this.selector.isOpen())
        {
            return false;
        }
        try
        {
            this.registrationLatch = new CountDownLatch(1);
            this.selector.wakeup();
            channel.register(this.selector, this.selectorProcessorOperation, operationTask);
        }
        finally
        {
            this.registrationLatch.countDown();
            this.registrationLatch = null;
        }
        return true;
    }

    /**
     * Set the interest operation for the channel to match the interest operation of this
     * SelectorProcessor and then wake up the selector so that the interest can be processed.
     * <p>
     * Effectively, if this SelectorProcessor handles write operations, calling this method tells
     * the SelectorProcessor that this channel wants to write data.
     * 
     * @throws CancelledKeyException
     *             if the channel has no registration with this SelectorProcessor. This can also
     *             indicate that the channel is closed.
     */
    void setInterest(SelectableChannel channel) throws CancelledKeyException
    {
        final SelectionKey keyFor = channel.keyFor(this.selector);
        if (keyFor == null)
        {
            throw new CancelledKeyException();
        }
        keyFor.interestOps(this.selectorProcessorOperation);
        this.selector.wakeup();
    }

    /**
     * Opposite of {@link #setInterest(SelectableChannel)}. After setting interest we need to reset
     * the interest. So, for example, when a channel has completed writing all its data, this method
     * is called to reset the channel's interest in writing.
     * 
     * @throws CancelledKeyException
     *             if the channel has no registration with this SelectorProcessor. This can also
     *             indicate that the channel is closed.
     */
    void resetInterest(SelectableChannel channel) throws CancelledKeyException
    {
        final SelectionKey keyFor = channel.keyFor(this.selector);
        if (keyFor == null)
        {
            throw new CancelledKeyException();
        }
        keyFor.interestOps(0);
    }

}