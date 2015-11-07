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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.tcpchannel.SelectorProcessor;

/**
 * Tests for the {@link SelectorProcessor}
 * 
 * @author Ramon Servadei
 */
public class TestSelectorProcessor
{
    private static final int OPERATION = SelectionKey.OP_WRITE;

    SelectorProcessor candidate;

    SocketChannel channel;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new SelectorProcessor("test", OPERATION);
        this.channel = SocketChannel.open();
        this.channel.configureBlocking(false);
        assertTrue(this.candidate.selector.isOpen());
        assertTrue(this.candidate.register(this.channel, null));
        assertTrue(this.channel.keyFor(this.candidate.selector).isValid());
    }

    @After
    public void tearDown() throws Exception
    {
        this.candidate.destroy();
    }

    @Test
    public void testSetInterest()
    {
        this.candidate.setInterest(this.channel);
        assertEquals(OPERATION, this.channel.keyFor(this.candidate.selector).interestOps());

        this.candidate.setInterest(this.channel);
        assertEquals(OPERATION, this.channel.keyFor(this.candidate.selector).interestOps());
    }

    @Test
    public void testResetInterest()
    {
        this.candidate.setInterest(this.channel);

        this.candidate.resetInterest(this.channel);
        assertEquals(0, this.channel.keyFor(this.candidate.selector).interestOps());

        this.candidate.resetInterest(this.channel);
        assertEquals(0, this.channel.keyFor(this.candidate.selector).interestOps());
    }

    @Test
    public void testCancel()
    {
        this.candidate.cancel(this.channel);
        checkIsCancelled();

        this.candidate.cancel(this.channel);
        checkIsCancelled();
    }

    @Test
    public void testDestroy() throws ClosedChannelException
    {
        this.candidate.destroy();
        assertFalse(this.candidate.selector.isOpen());
        assertFalse(this.candidate.register(this.channel, null));
        checkIsCancelled();

        this.candidate.destroy();
        assertFalse(this.candidate.selector.isOpen());
        assertFalse(this.candidate.register(this.channel, null));
        checkIsCancelled();
    }

    void checkIsCancelled()
    {
        final SelectionKey keyFor = this.channel.keyFor(this.candidate.selector);
        if (keyFor != null)
        {
            assertFalse(keyFor.isValid());
        }
        try
        {
            this.candidate.setInterest(this.channel);
            fail("should throw CancelledKeyException");
        }
        catch (CancelledKeyException e)
        {
        }
        try
        {
            this.candidate.resetInterest(this.channel);
            fail("should throw CancelledKeyException");
        }
        catch (CancelledKeyException e)
        {
        }
    }

}
