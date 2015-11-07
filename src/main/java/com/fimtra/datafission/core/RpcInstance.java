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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.channel.ITransportChannel;
import com.fimtra.datafission.DataFissionProperties;
import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IPublisherContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.util.Log;

/**
 * The standard implementation of an {@link IRpcInstance}
 * <p>
 * The logic for executing is encapsulated in an {@link IRpcExecutionHandler} that is provided to
 * the instance.
 * <p>
 * The {@link Context} that hosts this RPC instance will execute remote calls to this
 * <b>sequentially, per connected {@link ProxyContext}</b> - see
 * {@link Publisher#rpc(Object, ITransportChannel)}. Parallel execution of an RPC instance can only
 * occur when invoked from multiple proxy contexts.
 * <p>
 * Instances are equal by name, arguments and return type. This enables 'overloading' of RPC names.
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings("rawtypes")
public final class RpcInstance implements IRpcInstance
{
    static final String RPC_RECORD_RESULT_PREFIX = "_RPC_";
    static final TextValue NO_ACK = TextValue.valueOf(RPC_RECORD_RESULT_PREFIX);

    /**
     * Handles the execution of the RPC.
     * <p>
     * <b>Implementations MUST be thread-safe.<b>
     * 
     * @author Ramon Servadei
     */
    public static interface IRpcExecutionHandler
    {
        /**
         * Handle an invokation of an RPC.
         * <p>
         * <b>This MUST be thread-safe.</b>
         * <p>
         * <b>Note:</b> the arguments will exactly match that of the {@link RpcInstance} that it is
         * registered with. The {@link RpcInstance#execute(IValue...)} method will throw an
         * {@link ExecutionException} if the argument types or numbers do not match the RPC
         * definition of the instance.
         * 
         * @param args
         *            the argument to execute the RPC with
         * @return the result, can be <code>null</code>
         * @return the response from the RPC
         * @throws TimeOutException
         *             if no response is received after a time
         * @throws ExecutionException
         *             if the RPC encounters an exception during execution
         */
        IValue execute(IValue... args) throws TimeOutException, ExecutionException;
    }

    /**
     * This class holds the caller and receiver logic (each in separate inner classes) for handling
     * RPCs. The key principles for the RPC model are:
     * 
     * <pre>
     * 1. A caller sends the RPC name and arguments to a receiver. 
     * 2. The caller thread will block for a configurable time until the RPC starts and completes. 
     * 3. The receiver sends back 2 messages; 
     *      a. First when it starts execution of the RPC 
     *      b. Second when it completes execution of the RPC and returns any result
     *      c. Messages are sent back to the caller addressed to a specially named temporary record that uniquely identifies the RPC call instance.
     * </pre>
     * 
     * This mechanism allows the caller to know when the RPC starts which can be important for RPCs
     * that would take a long time to complete.
     * 
     * @author Ramon Servadei
     */
    static class Remote
    {
        static final String ARG_ = "A";
        static final String ARGS_COUNT = "A";
        static final String RESULT_RECORD_NAME = "R";

        private static final String EXCEPTION = "EXCEPTION";
        private static final String RESULT = "RESULT";

        static IValue[] decodeArgs(IRecordChange callDetails)
        {
            Map<String, IValue> argsMap = callDetails.getPutEntries();
            int argCount = (int) argsMap.get(ARGS_COUNT).longValue();
            IValue[] args = new IValue[argCount];
            for (int i = 0; i < argCount; i++)
            {
                args[i] = argsMap.get(ARG_ + i);
            }
            return args;
        }

        static String decodeResultRecordName(IRecordChange callDetails)
        {
            return callDetails.getPutEntries().get(RESULT_RECORD_NAME).textValue();
        }

        static String decodeRpcName(IRecordChange callDetails)
        {
            return callDetails.getName();
        }

        /**
         * Receives RPC calls, executes them and then sends back the result to the caller.
         * 
         * @param T
         *            the object protocol, see {@link ICodec}
         * @see Caller
         * @author Ramon Servadei
         */
        static class CallReceiver
        {
            private final ICodec codec;
            private final IObserverContext context;
            private final ITransportChannel caller;

            public CallReceiver(ICodec codec, ITransportChannel caller, IObserverContext context)
            {
                super();
                this.codec = codec;
                this.caller = caller;
                this.context = context;
            }

            /**
             * This executes a single RPC with arguments.
             * 
             * @param data
             *            the RPC with arguments.
             */
            @SuppressWarnings("unchecked")
            public void execute(Object data)
            {
                final IRecordChange callDetails = this.codec.getRpcFromRxMessage(data);
                final String rpcName = decodeRpcName(callDetails);
                final IValue[] args = decodeArgs(callDetails);
                final String resultRecordName = decodeResultRecordName(callDetails);

                if (NO_ACK.textValue().equals(resultRecordName))
                {
                    try
                    {
                        this.context.getRpc(rpcName).execute(args);
                    }
                    catch (Exception e)
                    {
                        Log.log(CallReceiver.class, "Exception handling NO-ACK RPC: " + callDetails, e);
                    }
                }
                else
                {
                    final Map<String, IValue> resultEntries = new HashMap<String, IValue>(2);
                   
                    // tell the remote caller we have started
                    Log.log(CallReceiver.class, "(->) STARTED ", resultRecordName);
                    this.caller.sendAsync(this.codec.getTxMessageForAtomicChange(new AtomicChange(resultRecordName,
                        resultEntries, ContextUtils.EMPTY_MAP, ContextUtils.EMPTY_MAP)));

                    try
                    {
                        IValue result = this.context.getRpc(rpcName).execute(args);
                        resultEntries.put(RESULT, result);
                    }
                    catch (Exception e)
                    {
                        resultEntries.put(EXCEPTION, new TextValue(e.toString()));
                        Log.log(CallReceiver.class, "Exception handling RPC: " + callDetails, e);
                    }

                    Log.log(CallReceiver.class, "(->) FINISHED ", resultRecordName, ", ",
                        ContextUtils.mapToString(resultEntries));
                    this.caller.sendAsync(this.codec.getTxMessageForAtomicChange(new AtomicChange(resultRecordName,
                        resultEntries, ContextUtils.EMPTY_MAP, ContextUtils.EMPTY_MAP)));
                }
            }
        }

        /**
         * Calls an RPC on a remote receiver. This handles the necessary plumbing to send the
         * details of the RPC to the remote receiver for invoking.
         * 
         * @param T
         *            the object protocol, see {@link ICodec}
         * @see CallReceiver
         * @author Ramon Servadei
         */
        static class Caller implements IRpcExecutionHandler
        {
            private final String rpcName;
            private final ICodec codec;
            private final ITransportChannel callReceiver;
            private final IPublisherContext context;
            private final AtomicReference<Long> remoteExecutionStartTimeoutMillis;
            private final AtomicReference<Long> remoteExecutionCompletedTimeoutMillis;

            public Caller(String rpcName, ICodec codec, ITransportChannel callReceiver, IPublisherContext context,
                AtomicReference<Long> remoteExecutionStartTimeoutMillis,
                AtomicReference<Long> remoteExecutionCompletedTimeoutMillis)
            {
                super();
                this.rpcName = rpcName;
                this.codec = codec;
                this.callReceiver = callReceiver;
                this.context = context;
                this.remoteExecutionStartTimeoutMillis = remoteExecutionStartTimeoutMillis;
                this.remoteExecutionCompletedTimeoutMillis = remoteExecutionCompletedTimeoutMillis;
            }

            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                final CountDownLatch executionStartedLatch = new CountDownLatch(1);
                final CountDownLatch executionCompleteLatch = new CountDownLatch(1);
                final AtomicReference<Map<String, IValue>> result = new AtomicReference<Map<String, IValue>>();
                final boolean noAck = args.length == 0 ? false : args[args.length - 1] == NO_ACK;
                final String resultMapName =
                    noAck ? NO_ACK.textValue() : RPC_RECORD_RESULT_PREFIX + this.rpcName + ":"
                        + System.identityHashCode(this) + ":" + System.currentTimeMillis() + ":"
                        + Thread.currentThread().getId();

                final IRecordListener resultHandler = new IRecordListener()
                {
                    @Override
                    public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                    {
                        if (atomicChange.getPutEntries().containsKey(RESULT)
                            || atomicChange.getPutEntries().containsKey(EXCEPTION))
                        {
                            result.set(atomicChange.getPutEntries());
                            executionCompleteLatch.countDown();
                        }
                        else
                        {
                            executionStartedLatch.countDown();
                        }
                    }
                };

                if (noAck)
                {
                    IValue[] callArgs = new IValue[args.length - 1];
                    System.arraycopy(args, 0, callArgs, 0, args.length - 1);
                    Log.log(Caller.class, "(->) CALLING RPC (no ack) ", this.rpcName);
                    this.callReceiver.sendAsync(this.codec.getTxMessageForRpc(this.rpcName, callArgs, resultMapName));
                    return null;
                }
                else
                {
                    try
                    {
                        this.context.addObserver(resultHandler, resultMapName);
                        Log.log(Caller.class, "(->) CALLING RPC ", resultMapName);
                        if (ContextUtils.isCoreThread() || ContextUtils.isRpcThread())
                        {
                            Log.log(this,
                                "WARNING: RPC being called using a core/RPC thread - this can lead to a stall.");
                        }
                        this.callReceiver.sendAsync(this.codec.getTxMessageForRpc(this.rpcName, args, resultMapName));

                        try
                        {
                            // wait for acknowledgement that execution has started
                            if (!executionStartedLatch.await(this.remoteExecutionStartTimeoutMillis.get().longValue(),
                                TimeUnit.MILLISECONDS))
                            {
                                throw new TimeOutException("The RPC execution did not start after "
                                    + this.remoteExecutionStartTimeoutMillis.get().longValue() + "ms");
                            }
                            // wait for completion
                            if (!executionCompleteLatch.await(
                                this.remoteExecutionCompletedTimeoutMillis.get().longValue(), TimeUnit.MILLISECONDS))
                            {
                                throw new TimeOutException("The RPC has started but has not completed after "
                                    + this.remoteExecutionCompletedTimeoutMillis.get().longValue()
                                    + "ms, is more time needed to allow for completion?");
                            }
                        }
                        catch (InterruptedException e)
                        {
                            throw new ExecutionException("Local thread interrupted: " + e.getMessage());
                        }

                        if (result.get() == null)
                        {
                            throw new ExecutionException("No result received");
                        }

                        Map<String, IValue> resultMap = result.get();
                        IValue exception = resultMap.get(EXCEPTION);
                        if (exception != null)
                        {
                            throw new ExecutionException(exception.textValue());
                        }
                        return resultMap.get(RESULT);
                    }
                    finally
                    {
                        this.context.removeObserver(resultHandler, resultMapName);
                        this.context.removeRecord(resultMapName);
                    }
                }
            }
        }
    }

    /**
     * @return the string representation of the instance definition (its return type and argument
     *         types)
     */
    public static String constructDefinitionFromInstance(IRpcInstance instance)
    {
        final StringBuilder args = new StringBuilder();
        final StringBuilder argNames = new StringBuilder();
        final boolean argNamesExist =
            instance.getArgNames() == null ? false : instance.getArgNames().length == instance.getArgTypes().length;
        if (instance.getArgTypes() != null)
        {
            for (int i = 0; i < instance.getArgTypes().length; i++)
            {
                if (i > 0)
                {
                    args.append(ARG_SEPARATOR);
                    if (argNamesExist)
                    {
                        argNames.append(ARG_SEPARATOR);
                    }
                }
                args.append(instance.getArgTypes()[i].name());
                if (argNamesExist)
                {
                    argNames.append(instance.getArgNames()[i]);
                }
            }
        }
        return ARGS + args.toString() + (argNamesExist ? (ARG_NAMES + argNames.toString()) : "") + RETURNS
            + instance.getReturnType().name() + CLOSE_CHAR;
    }

    /**
     * @return an {@link RpcInstance} representing the rpcName and definition
     * @see #constructDefinitionFromInstance(RpcInstance)
     */
    public static RpcInstance constructInstanceFromDefinition(String name, String definition)
    {
        final int indexOfArgNames = definition.indexOf(ARG_NAMES);
        final int indexOfRet = definition.indexOf(RETURNS);
        if (indexOfRet > -1)
        {
            String args;
            String argNamesString = null;
            if (indexOfArgNames > -1)
            {
                args = definition.substring(ARGS.length(), indexOfArgNames);
                argNamesString = definition.substring(indexOfArgNames + ARG_NAMES.length(), indexOfRet);
            }
            else
            {
                args = definition.substring(ARGS.length(), indexOfRet);
            }
            String ret = definition.substring(indexOfRet + RETURNS.length(), definition.length() - CLOSE_CHAR.length());
            String[] tokens = args.split(ARG_SEPARATOR);
            TypeEnum[] argTypes = new TypeEnum[tokens.length];
            if (tokens.length == 1 && "".equals(tokens[0]))
            {
                argTypes = new TypeEnum[0];
            }
            else
            {
                for (int i = 0; i < tokens.length; i++)
                {
                    if (tokens[i] == null || "".equals(tokens[i]))
                    {
                        throw new IllegalStateException("Received a null or blank argument in the RPC definition: "
                            + definition);
                    }
                    argTypes[i] = TypeEnum.valueOf(tokens[i]);
                }
            }
            final String[] argNames;
            if (argNamesString != null)
            {
                argNames = argNamesString.split(ARG_SEPARATOR);
            }
            else
            {
                argNames = null;
            }
            final RpcInstance rpcInstance = new RpcInstance(TypeEnum.valueOf(ret), name, argNames, argTypes);
            return rpcInstance;
        }
        return null;
    }

    /**
     * @return <code>true</code> if the record name is that of an RPC result
     */
    static boolean isRpcResultRecord(String name)
    {
        return name.startsWith(RPC_RECORD_RESULT_PREFIX, 0);
    }

    private static final String ARG_SEPARATOR = ",";
    private static final String ARGS = "args{";
    private static final String ARG_NAMES = "},argNames{";
    private static final String RETURNS = "},returns{";
    private static final String CLOSE_CHAR = "}";

    private final TypeEnum retType;
    private final TypeEnum[] argTypes;
    private final String[] argNames;
    private final String name;

    private IRpcExecutionHandler handler;
    /**
     * The timeout (in milliseconds) for the remote execution to begin (the time we wait to get an
     * acknowledgement that the RPC has started)
     */
    final AtomicReference<Long> remoteExecutionStartTimeoutMillis;
    /**
     * The maximum time (in milliseconds) allowed for the RPC to execute before a
     * {@link TimeOutException} is raised. This time begins as soon as the execution start is
     * acknowledged by the remote executor.
     */
    final AtomicReference<Long> remoteExecutionDurationTimeoutMillis;

    public RpcInstance(TypeEnum retType, String name, TypeEnum... argTypes)
    {
        this(null, retType, name, null, argTypes);
    }

    public RpcInstance(TypeEnum retType, String name, String[] argNames, TypeEnum... argTypes)
    {
        this(null, retType, name, argNames, argTypes);
    }

    public RpcInstance(IRpcExecutionHandler handler, TypeEnum retType, String name, TypeEnum... argTypes)
    {
        this(handler, retType, name, null, argTypes);
    }

    public RpcInstance(IRpcExecutionHandler handler, TypeEnum retType, String name, String[] argNames,
        TypeEnum... argTypes)
    {
        super();
        this.name = name;
        this.retType = retType;
        this.argNames = argNames;
        if (retType == null)
        {
            throw new IllegalArgumentException("Cannot have a null return type");
        }
        this.argTypes = argTypes;
        for (int i = 0; i < argTypes.length; i++)
        {
            if (argTypes[i] == null)
            {
                throw new IllegalArgumentException("Some argTypes were null:" + Arrays.toString(argTypes));
            }
        }
        this.remoteExecutionStartTimeoutMillis =
            new AtomicReference<Long>(DataFissionProperties.Values.RPC_EXECUTION_START_TIMEOUT_MILLIS);
        this.remoteExecutionDurationTimeoutMillis =
            new AtomicReference<Long>(DataFissionProperties.Values.RPC_EXECUTION_DURATION_TIMEOUT_MILLIS);
        setHandler(handler);
    }

    public void setHandler(IRpcExecutionHandler handler)
    {
        this.handler = handler;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.argTypes);
        result = prime * result + ((this.name == null) ? 0 : this.name.hashCode());
        result = prime * result + ((this.retType == null) ? 0 : this.retType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RpcInstance other = (RpcInstance) obj;
        if (!Arrays.equals(this.argTypes, other.argTypes))
            return false;
        if (this.name == null)
        {
            if (other.name != null)
                return false;
        }
        else if (!this.name.equals(other.name))
            return false;
        if (this.retType != other.retType)
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "RpcInstance [" + this.name + ARG_SEPARATOR + constructDefinitionFromInstance(this) + "]";
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public TypeEnum getReturnType()
    {
        return this.retType;
    }

    @Override
    public TypeEnum[] getArgTypes()
    {
        return this.argTypes;
    }

    @Override
    public String[] getArgNames()
    {
        return this.argNames;
    }

    /**
     * @throws ExecutionException
     *             if the argument types or numbers do not match the RPC definition of the instance.
     */
    @Override
    public IValue execute(IValue... args) throws TimeOutException, ExecutionException
    {
        try
        {
            checkRpcArgs(args);
            return this.handler.execute(args);
        }
        catch (TimeOutException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            Log.log(RpcInstance.class,
                "Could not execute " + this.toString() + " with arguments: " + Arrays.toString(args), e);
            throw new ExecutionException(e.getMessage());
        }
    }

    @Override
    public void executeNoResponse(IValue... args) throws TimeOutException, ExecutionException
    {
        try
        {
            // NOTE: it is ok to call this using a core/RPC thread - this never blocks

            checkRpcArgs(args);
            IValue[] argsToSend = new IValue[args.length + 1];
            System.arraycopy(args, 0, argsToSend, 0, args.length);
            argsToSend[args.length] = (NO_ACK);
            this.handler.execute(argsToSend);
        }
        catch (TimeOutException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            Log.log(RpcInstance.class,
                "Could not execute " + this.toString() + " with arguments: " + Arrays.toString(args), e);
            throw new ExecutionException(e.getMessage());
        }
    }

    private void checkRpcArgs(IValue... args) throws ExecutionException
    {
        if (args.length != getArgTypes().length)
        {
            throw new ExecutionException("Incorrect number of arguments; expected types " + toString((getArgTypes())));
        }
        for (int i = 0; i < args.length; i++)
        {
            if (args[i].getType() != getArgTypes()[i])
            {
                throw new ExecutionException("Incorrect argument type at index " + i + ", expecting a "
                    + getArgTypes()[i].name());
            }
        }
    }

    private static String toString(TypeEnum[] typeEnums)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < typeEnums.length; i++)
        {
            if (i > 0)
            {
                sb.append(", ");
            }
            TypeEnum typeEnum = typeEnums[i];
            sb.append(typeEnum.name());
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void setRemoteExecutionStartTimeoutMillis(long remoteExecutionStartTimeoutMillis)
    {
        this.remoteExecutionStartTimeoutMillis.set(Long.valueOf(remoteExecutionStartTimeoutMillis));
    }

    @Override
    public void setRemoteExecutionDurationTimeoutMillis(long remoteExecutionDurationTimeoutMillis)
    {
        this.remoteExecutionDurationTimeoutMillis.set(Long.valueOf(remoteExecutionDurationTimeoutMillis));
    }
}
