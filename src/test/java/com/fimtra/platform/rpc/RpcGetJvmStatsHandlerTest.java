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
