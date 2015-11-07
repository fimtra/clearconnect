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
package com.fimtra.util;

/**
 * Utility methods for tests
 * 
 * @author Ramon Servadei
 */
public abstract class TestUtils
{
    private TestUtils()
    {
    }

    /**
     * Thrown from {@link TestUtils#waitForEvent(EventChecker)} when an event fails to occur.
     * 
     * @author Ramon Servadei
     */
    public static final class EventFailedException extends AssertionError
    {
        private static final long serialVersionUID = 1L;

        EventFailedException(String message)
        {
            super(message);
        }
    }

    /**
     * Exposes the expected event and the current event to test during the
     * {@link TestUtils#waitForEvent(EventChecker)} method.
     * 
     * @author Ramon Servadei
     */
    public static interface EventChecker
    {
        Object expect();

        Object got();
    }

    public static interface EventCheckerWithFailureReason extends EventChecker
    {
        String getFailureReason();
    }

    /**
     * Wait for the event to occur by periodically polling the event checker
     * 
     * @throws EventFailedException
     *             if the event fails to occur
     */
    public static final void waitForEvent(EventChecker check) throws InterruptedException, EventFailedException
    {
        waitForEvent(check, 30000);
    }

    /**
     * Wait for the event to occur by periodically polling the event checker for the number of
     * loops. Each loop pauses 1ms between.
     * 
     * @throws EventFailedException
     *             if the event fails to occur
     */
    public static final void waitForEvent(EventChecker check, int maxLoops) throws InterruptedException, EventFailedException
    {
        int loops = 0;
        while (check(check) && loops++ < maxLoops)
        {
            Thread.sleep(1);
        }
        if (check(check))
        {
            throw new EventFailedException("expected="
                + check.expect()
                + ", but got="
                + check.got()
                + (check instanceof EventCheckerWithFailureReason ? " "
                    + ((EventCheckerWithFailureReason) check).getFailureReason() : ""));
        }
    }

    private static boolean check(EventChecker check)
    {
        return !is.eq(check.got(), check.expect());
    }
}
