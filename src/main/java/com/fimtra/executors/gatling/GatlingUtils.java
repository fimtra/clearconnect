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

package com.fimtra.executors.gatling;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.executors.IContextExecutor;
import com.fimtra.executors.ISequentialRunnable;

/**
 * @author Ramon Servadei
 */
class GatlingUtils {

    static <V> Callable<V> createContextCallable(Callable<V> task, Object context,
            GatlingExecutor gatlingExecutor)
    {
        return new DelayedResult<V>(task, null, context, gatlingExecutor);

    }

    static <V> Callable<V> createContextCallable(Runnable task, V result, Object context,
            GatlingExecutor gatlingExecutor)
    {
        return new DelayedResult<V>(task, result, context, gatlingExecutor);
    }

    static Runnable createContextRunnable(Runnable task, Object context, GatlingExecutor executor)
    {
        return () -> executor.execute(new ISequentialRunnable() {
            @Override
            public Object context()
            {
                return context == null ? task : context;
            }

            @Override
            public void run()
            {
                task.run();
            }
        });
    }

    static class DelayedResult<T> implements Callable<T> {

        final Object task;
        final Object context;
        final IContextExecutor executor;
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<T> result = new AtomicReference<>();
        final AtomicReference<Exception> exception = new AtomicReference<>();

        boolean cancelled;

        DelayedResult(Object task, T result, Object context, IContextExecutor executor)
        {
            this.context = context;
            this.task = task;
            this.executor = executor;
            this.result.set(result);
        }

        @Override
        public T call() throws Exception
        {
            return _call();
        }

        T _call() throws Exception
        {
            this.executor.execute(new ISequentialRunnable() {
                @Override
                public Object context()
                {
                    return DelayedResult.this.context == null ? DelayedResult.this.task :
                            DelayedResult.this.context;
                }

                @Override
                public void run()
                {
                    try
                    {
                        if (DelayedResult.this.task instanceof Runnable)
                        {
                            ((Runnable) DelayedResult.this.task).run();
                        }
                        else
                        {
                            DelayedResult.this.result.set((T) ((Callable) DelayedResult.this.task).call());
                        }
                    }
                    catch (Exception e)
                    {
                        DelayedResult.this.exception.set(e);
                    }
                    finally
                    {
                        DelayedResult.this.latch.countDown();
                    }
                }
            });

            return getT();
        }

        private T getT() throws TimeoutException, InterruptedException, ExecutionException
        {
            return getT(0, TimeUnit.MILLISECONDS);
        }

        private T getT(long timeout, TimeUnit unit)
                throws TimeoutException, InterruptedException, ExecutionException
        {
            if (timeout > 0)
            {
                if (!this.latch.await(timeout, unit))
                {
                    throw new TimeoutException();
                }
            }
            else
            {
                this.latch.await();
            }

            if (this.exception.get() != null)
            {
                throw new ExecutionException(this.exception.get());
            }
            return this.result.get();
        }
    }
}

