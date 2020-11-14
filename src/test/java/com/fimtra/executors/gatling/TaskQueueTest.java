package com.fimtra.executors.gatling;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link com.fimtra.executors.gatling.TaskQueue}
 *
 * @author ramon
 */
public class TaskQueueTest
{
    TaskQueue candidate;

    @Before
    public void setUp() throws Exception
    {
        candidate = new TaskQueue("lasers");
    }

    @Test
    public void testNotDraining()
    {
        Runnable r = () -> {

        };

        assertFalse(candidate.isNotDraining_callWhilstHoldingLock());
        assertFalse(candidate.isNotDraining_callWhilstHoldingLock());

        candidate.queue.add(r);
        assertTrue(candidate.isNotDraining_callWhilstHoldingLock());
        // same size since last call
        assertTrue(candidate.isNotDraining_callWhilstHoldingLock());

        // drain to 0
        candidate.queue.poll();
        assertFalse(candidate.isNotDraining_callWhilstHoldingLock());

        candidate.queue.add(r);
        assertTrue(candidate.isNotDraining_callWhilstHoldingLock());
        // increase since last call
        candidate.queue.add(r);
        assertTrue(candidate.isNotDraining_callWhilstHoldingLock());

        // remove 1 - q is draining
        candidate.queue.poll();
        assertFalse(candidate.isNotDraining_callWhilstHoldingLock());

        // not draining again
        assertTrue(candidate.isNotDraining_callWhilstHoldingLock());

        candidate.queue.add(r);
        candidate.queue.add(r);
        candidate.queue.add(r);
        assertTrue(candidate.isNotDraining_callWhilstHoldingLock());

        // full drain
        candidate.queue.clear();
        assertFalse(candidate.isNotDraining_callWhilstHoldingLock());

    }
}
