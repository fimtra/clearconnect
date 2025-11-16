/*
 * Copyright 2020 Ramon Servadei
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fimtra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

/**
 * @author Ramon Servadei
 */
public class SystemUtilsTest {

    @Test
    public void testGetProperty()
    {
        assertTrue(SystemUtils.getProperty("notThere", true));
        assertEquals(1, SystemUtils.getPropertyAsInt("notThere", 1));
        assertEquals(Long.MAX_VALUE, SystemUtils.getPropertyAsLong("notThere", Long.MAX_VALUE));

        // set some values
        final int expectInt = new Random().nextInt();
        final int expectLong = new Random().nextInt();
        final boolean expectBool = false;
        final String intPropKey = "SystemUtilsTest" + "int";
        final String longPropKey = "SystemUtilsTest" + "long";
        final String boolPropKey = "SystemUtilsTest" + "bool";
        System.setProperty(intPropKey, "" + expectInt);
        System.setProperty(longPropKey, "" + expectLong);
        System.setProperty(boolPropKey, "" + expectBool);

        // test reading
        assertEquals(expectInt, SystemUtils.getPropertyAsInt(intPropKey, -1));
        assertEquals(expectLong, SystemUtils.getPropertyAsLong(longPropKey, -1));

        assertEquals(expectBool, SystemUtils.getProperty(boolPropKey, true));
        assertEquals(expectBool, SystemUtils.getProperty(boolPropKey, false));
    }
}
