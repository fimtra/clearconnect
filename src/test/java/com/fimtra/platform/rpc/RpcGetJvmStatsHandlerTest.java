/*
 * Copyright (c) 2014 Paul Mackinlay, Fimtra
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
package com.fimtra.platform.rpc;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValue;

import static org.junit.Assert.assertTrue;

/**
 * @author Paul Mackinlay
 */
public class RpcGetJvmStatsHandlerTest {

	private RpcGetJvmStatsHandler rpcGetJvmStatsHandler;

	@Before
	public void setUp() {
		this.rpcGetJvmStatsHandler = new RpcGetJvmStatsHandler();
	}

	@Test
	public void shouldGetJvmStats() throws TimeOutException, ExecutionException {
		IValue value = this.rpcGetJvmStatsHandler.execute();
		assertTrue(!value.textValue().isEmpty());
		System.out.println(value.textValue());
	}
}
