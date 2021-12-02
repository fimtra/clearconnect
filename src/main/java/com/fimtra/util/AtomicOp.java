/*
 * Copyright (c) 2019 Ramon Servadei
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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Ramon Servadei
 */
public final class AtomicOp
{
    final Lock lock;

    public AtomicOp()
    {
        this(new ReentrantLock());
    }

    public AtomicOp(Lock lock)
    {
        this.lock = lock;
    }

    public void execute(Runnable r)
    {
        this.lock.lock();
        try
        {
            r.run();
        }
        finally
        {
            this.lock.unlock();
        }
    }
}