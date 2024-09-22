package com.fimtra.datafission.core;

import java.util.Map;
import java.util.WeakHashMap;

import com.fimtra.channel.ITransportChannel;
import com.fimtra.datafission.IValue;

/**
 * Provides access to the calling context of an RPC. <p> Only valid for using during:
 * <br>{@link RpcInstance.IRpcExecutionHandler#execute(IValue...)}
 * <br>or
 * <br>{@link RpcInstance.IRpcExecutionHandler#executeNoResponse(IValue...)}
 *
 * @author Ramon Servadei
 */
public abstract class RpcCallingContext
{
    private static final Map<Thread, String> RPC_CALLER_ENDPOINT = new WeakHashMap<>();

    static void set(String endPointDescription)
    {
        RPC_CALLER_ENDPOINT.put(Thread.currentThread(), endPointDescription);
    }

    static void remove()
    {
        RPC_CALLER_ENDPOINT.remove(Thread.currentThread());
    }

    /**
     * Get the end-point description of the remote host invoking the currently executing RPC.
     * <p>
     * ONLY VALID TO CALL DURING EXECUTION OF {@link RpcInstance.IRpcExecutionHandler#execute(IValue...)} or
     * {@link RpcInstance.IRpcExecutionHandler#executeNoResponse(IValue...)}
     *
     * @return the caller end-point description (e.g. serverHost + ":" + serverPort), or null if called
     * outside of the RPC execution
     * @see ITransportChannel#getEndPointDescription()
     */
    public static String getCallerEndpointDescription()
    {
        return RPC_CALLER_ENDPOINT.get(Thread.currentThread());
    }

    private RpcCallingContext()
    {
        // not for instantiation
    }
}
