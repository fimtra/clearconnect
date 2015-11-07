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
package com.fimtra.datafission.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.AtomicChange;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.datafission.core.RpcInstance.Remote;
import com.fimtra.datafission.field.DoubleValue;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;

/**
 * Tests for the {@link RpcInstance}
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings({ "unused" })
public class RpcInstanceTest
{

    private static final String[] ARG_NAMES = new String[] { "a double", "long1", "someTextArg" };

    @Before
    public void setUp() throws Exception
    {
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testDecodeArgs()
    {
        Map<String, IValue> args = new HashMap<String, IValue>();
        args.put(RpcInstance.Remote.ARGS_COUNT, LongValue.valueOf(2));
        args.put(RpcInstance.Remote.ARG_ + "0", LongValue.valueOf(0));
        args.put(RpcInstance.Remote.ARG_ + "1", LongValue.valueOf(1));
        args.put(RpcInstance.Remote.ARG_ + "2", LongValue.valueOf(2));

        IRecordChange change = new AtomicChange("lasers", args, null, null);
        IValue[] decodeArgs = Remote.decodeArgs(change);

        assertEquals(2, decodeArgs.length);
        assertEquals(LongValue.valueOf(0), decodeArgs[0]);
        assertEquals(LongValue.valueOf(1), decodeArgs[1]);
    }

    @Test
    public void testConstructDefinitionFromInstance()
    {
        String name = "testConstructDefinitionFromInstance";
        RpcInstance candidate = new RpcInstance(TypeEnum.LONG, name, TypeEnum.DOUBLE, TypeEnum.LONG, TypeEnum.TEXT);
        RpcInstance other =
            RpcInstance.constructInstanceFromDefinition(name, RpcInstance.constructDefinitionFromInstance(candidate));
        assertEquals(candidate, other);
    }

    @Test
    public void testConstructDefinitionFromInstanceWithIncorrectArgNamesCount()
    {
        String name = "testConstructDefinitionFromInstance";
        RpcInstance candidate =
            new RpcInstance(TypeEnum.LONG, name, new String[] { "only one!" }, TypeEnum.DOUBLE, TypeEnum.LONG,
                TypeEnum.TEXT);
        RpcInstance other =
            RpcInstance.constructInstanceFromDefinition(name, RpcInstance.constructDefinitionFromInstance(candidate));
        assertEquals(candidate, other);
        // for incorrect arg name counts, they are not resolved
        assertFalse(Arrays.equals(candidate.getArgNames(), other.getArgNames()));
    }

    @Test
    public void testConstructDefinitionFromInstanceWithArgNames()
    {
        String name = "testConstructDefinitionFromInstance";
        RpcInstance candidate =
            new RpcInstance(TypeEnum.LONG, name, ARG_NAMES, TypeEnum.DOUBLE, TypeEnum.LONG, TypeEnum.TEXT);
        RpcInstance other =
            RpcInstance.constructInstanceFromDefinition(name, RpcInstance.constructDefinitionFromInstance(candidate));
        assertArrayEquals(ARG_NAMES, other.getArgNames());
        assertEquals(candidate, other);
    }

    @Test
    public void testConstructDefinitionFromInstanceWithNoArgs()
    {
        String name = "testConstructDefinitionFromInstanceWithNoArgs";
        RpcInstance candidate = new RpcInstance(TypeEnum.LONG, name);
        RpcInstance other =
            RpcInstance.constructInstanceFromDefinition(name, RpcInstance.constructDefinitionFromInstance(candidate));
        assertEquals(candidate, other);
    }

    @Test
    public void testConstructDefinitionFromInstanceWithSomeNullArgs()
    {
        String name = "testConstructDefinitionFromInstanceWithSomeNullArgs";
        try
        {
            new RpcInstance(TypeEnum.LONG, name, TypeEnum.DOUBLE, null, TypeEnum.TEXT);
            fail();
        }
        catch (IllegalArgumentException e)
        {
        }
    }

    @Test
    public void testConstructDefinitionFromInstanceWithNullRetArgs()
    {
        String name = "testConstructDefinitionFromInstanceWithNullRetArgs";
        try
        {
            new RpcInstance(null, name, TypeEnum.DOUBLE, TypeEnum.TEXT);
            fail();
        }
        catch (IllegalArgumentException e)
        {
        }
    }

    @Test
    public void testExecute() throws TimeOutException, ExecutionException
    {
        IValue result =
            new RpcInstance(new IRpcExecutionHandler()
            {
                @Override
                public IValue execute(IValue... args) throws TimeOutException, ExecutionException
                {
                    return new TextValue(args[0].textValue() + args[1].textValue() + args[2].textValue());
                }
            }, TypeEnum.TEXT, "getSomething", TypeEnum.TEXT, TypeEnum.DOUBLE, TypeEnum.DOUBLE).execute(new TextValue(
                "text"), new DoubleValue(Double.NaN), new DoubleValue(3));
        assertEquals("textNaN3.0", result.textValue());
    }

    @Test(expected = ExecutionException.class)
    public void testExecuteIncorrectTypes() throws TimeOutException, ExecutionException
    {
        IValue result =
            new RpcInstance(new IRpcExecutionHandler()
            {
                @Override
                public IValue execute(IValue... args) throws TimeOutException, ExecutionException
                {
                    return new TextValue(args[0].textValue() + args[1].textValue() + args[2].textValue());
                }
            }, TypeEnum.TEXT, "getSomething", ARG_NAMES, TypeEnum.TEXT, TypeEnum.DOUBLE, TypeEnum.DOUBLE).execute(
                new TextValue("text"), new DoubleValue(Double.NaN), LongValue.valueOf(3));
    }
}
