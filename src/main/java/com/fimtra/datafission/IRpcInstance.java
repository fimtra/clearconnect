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
package com.fimtra.datafission;

import com.fimtra.datafission.IValue.TypeEnum;

/**
 * An instance of a specific remote procedure call (RPC). The instance can be called multiple times.
 * 
 * @author Ramon Servadei
 */
public interface IRpcInstance
{
    /**
     * Thrown when an RPC is invoked but does not exist
     * 
     * @author Ramon Servadei
     */
    public static class NotFoundException extends Exception
    {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Thrown when there is a timeout when executing an RPC
     * 
     * @author Ramon Servadei
     */
    public static class TimeOutException extends Exception
    {
        private static final long serialVersionUID = 1L;

        public TimeOutException(String message)
        {
            super(message);
        }
    }

    /**
     * Thrown when an RPC encounters an exception during its execution
     * 
     * @author Ramon Servadei
     */
    public static class ExecutionException extends Exception
    {
        private static final long serialVersionUID = 1L;

        public ExecutionException(String message)
        {
            super(message);
        }
        
        public ExecutionException(Exception cause)
        {
            super(cause);
        }
    }

    /**
     * Get the name of the RPC. This is what is used to locate this instance.
     * 
     * @return the name of the RPC
     */
    String getName();

    /**
     * Get the argument types for the RPC. These must match what is expected in the
     * {@link #execute(IValue...)} method.
     * 
     * @return the argument types for the RPC
     */
    TypeEnum[] getArgTypes();

    /**
     * Get the names/description of the arguments. The index of each string refers to the
     * corresponding index in {@link #getArgTypes()}
     * 
     * @return the argument names/description for the argument types from {@link #getArgTypes()}
     */
    String[] getArgNames();

    /**
     * Get the return type for the RPC
     * 
     * @return the return type
     */
    TypeEnum getReturnType();

    /**
     * @param remoteExecutionStartTimeoutMillis
     *            the timeout (in milliseconds) for the remote execution to begin (the time we wait
     *            to get an acknowledgement that the RPC has started)
     */
    void setRemoteExecutionStartTimeoutMillis(long remoteExecutionStartTimeoutMillis);

    /**
     * 
     * @param remoteExecutionDurationTimeoutMillis
     *            the maximum time (in milliseconds) allowed for the RPC to execute before a
     *            {@link TimeOutException} is raised. This time begins as soon as the execution
     *            start is acknowledged by the remote executor.
     */
    void setRemoteExecutionDurationTimeoutMillis(long remoteExecutionDurationTimeoutMillis);

    /**
     * Execute this RPC using the arguments.
     * <p>
     * <b>This method is guaranteed by implementations to be thread-safe.</b>
     * 
     * @param args
     *            the argument to execute the RPC with
     * @return the result, can be <code>null</code>
     * @return the response from the RPC
     * @throws IRpcInstance.TimeOutException
     *             if no response is received after a time
     * @throws IRpcInstance.ExecutionException
     *             if the RPC encounters an exception during execution
     */
    IValue execute(IValue... args) throws TimeOutException, ExecutionException;

    /**
     * Execute the RPC with no requirement for any response. Essentially a fire-and-forget type
     * action.
     * 
     * @see #execute(IValue...)
     */
    void executeNoResponse(IValue... args) throws TimeOutException, ExecutionException;
}
