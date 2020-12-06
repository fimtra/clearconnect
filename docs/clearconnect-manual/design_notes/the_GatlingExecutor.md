# The GatlingExecutor

![gatling-s](../assets/gatling-s.png)

## In a nutshell

The GatlingExecutor is a multi-threaded, elastic, executor implementation that ensures in-order execution of ordered-tasks and skipping of out-of-date tasks. Tasks (*Runnables*) need to be tagged as sequential or coalescing. Standard *Runnables* are executed with no ordering or skipping.

### Why Gatling?

A Gatling cannon achieves high rates of fire due to the rotating barrel which allows rifle barrel reloading for the next rotation whilst one is firing. The GatlingExecutor has a task queue that rotates after task as it is is executed. So in abstract terms, the operations are similar. Its also an awesome name :) 

## Overview

Three types of tasks can be submitted to the GatlingExecutor:

1. *Runnable* - a task that can be run independently of other tasks with no ordering or skipping.
2. *SequentialRunnable* - these are tasks that need to be executed in the order they are submitted. Tasks have a defined **context** (more on that in a bit) which defines the sequence; different contexts will have sequences independently maintained.
3. *CoalescingRunnable* - these are tasks that can be skipped if newer tasks of the same context are submitted.

### Task Context

A fundamental principle of multi-threaded event processing systems is that all events (tasks) have a context (they just don't know it or expose it!). Task context defines the parallelism of task processing; *generally* tasks of different contexts can be processed concurrently but tasks of the same context cannot. This is why we end up seeing many single-thread executors in a multi-threaded system; each single-thread executor represents a single *context* of processing. The GatlingExecutor binds tasks to contexts to achieve multi-task processing without losing ordering and without the need for multiple executors.

To further help understand what we mean by context; if you have a conversation with 2 people at the same time, you need to keep track of each conversation within the context of the person you are conversing with. You can imagine that each person's replies are tasks for you to process that are "tagged" with the person's name (and you have to handle them in the order they arrive); the person's name can be viewed as the *context*. In this way you can have simultaneous conversations (theoretically anyway) - the GatlingExecutor is quite similar in how it handles tasks for different contexts.

### Typical scenarios

**SequentialRunnables** are used for situations where every event must be processed in a specific order (the event sequences may also occur within different contexts). A typical scenario would be a server processing messages from multiple clients; the server must handle the messages from each client (the *contexts*) in the order they are received (the *sequence*) otherwise the client-server protocol will breakdown. A lot of multi-threaded systems use multiple single-threaded executors to achieve sequential processing *per context* at the expense of thread context switching. A GatlingExecutor achieves the same behaviour and sometimes with fewer threads needed and possibly improved throughput (more later).

**CoalescingRunnables** are used for events that can be skipped if a later event occurs before previous ones have been processed. An example of this is in financial data processing where price changes occur so frequently (every 10ms, say) and processing every change would lead to huge backlogs in data processing. In this situation a pricing system only needs to process the **latest** received price before looping again to process the next received **latest** price.

### What are the benefits?

The principle benefit of the GatlingExecutor is the replacement of multiple single-threaded executors with a single executor instance. This translates to fewer threads in the application, which is more computationally efficient due to less CPU context switching for multiple thread handling.

#### Benchmark comparison test

As a test (somewhat academic but it serves to illustrate the point), assume an application had to process events, in-order, from 16 different sources, each source generating 50,000 events. Traditionally this would be implemented with a thread/executor per source to handle the events, thus guaranteeing ordering. If we model the events as incrementing a counter (thus each event has equal latency) we can express this scenario in code as follows:

```Java
static long getExecutorTime(int LOOP_MAX, int contextCount) throws InterruptedException
{
    CountDownLatch executorLatch = new CountDownLatch(1);
    AtomicLong[] executorCounters = new AtomicLong[contextCount];
    ExecutorService[] executors = new ExecutorService[contextCount];
    Runnable[] executorRunnables = new Runnable[contextCount];

    // create components
    for (int i = 0; i < contextCount; i++)
    {
        executorCounters[i] = new AtomicLong(0);
        executors[i] = Executors.newSingleThreadExecutor();
        int finalI = i;
        executorRunnables[i] = () -> {
            // simulate some work
            AtomicLong someWork = new AtomicLong();
            for (int j = 0; j < 1000; j++)
            {
                someWork.incrementAndGet();
            }
            if (executorCounters[finalI].incrementAndGet() == LOOP_MAX)
            {
                executorLatch.countDown();
            }
        };
    }

    // run the test
    long executorTime = System.nanoTime();
    for (int j = 0; j < LOOP_MAX; j++)
    {
        for (int i = 0; i < contextCount; i++)
        {
            executors[i].execute(executorRunnables[i]);
        }
    }
    System.err.println("executors loop done, waiting...");
    executorLatch.await();
    executorTime = System.nanoTime() - executorTime;
    System.err.println("executors finished");
    for (int i = 0; i < contextCount; i++)
    {
        executors[i].shutdown();
    }
    return executorTime;
}
```

The equivalent of the above scenario using a GatlingExecutor:

```java
static long getGatlingTime(int LOOP_MAX, int contextCount) throws InterruptedException
{
    CountDownLatch gatlingLatch = new CountDownLatch(1);
    AtomicLong[] gatlingCounters = new AtomicLong[contextCount];
    ISequentialRunnable[] gatlingRunnables = new ISequentialRunnable[contextCount];
    IContextExecutor gatling = new GatlingExecutor("test-vs-exec",
                                                   // start with 2 threads
                                                   2);

    // create gatling components
    for (int i = 0; i < contextCount; i++)
    {
        int finalI = i;
        gatlingCounters[i] = new AtomicLong(0);
        gatlingRunnables[i] = new ISequentialRunnable() {
            @Override
            public void run()
            {
                // simulate some work
                AtomicLong someWork = new AtomicLong();
                for (int j = 0; j < 1000; j++)
                {
                    someWork.incrementAndGet();
                }
                if (gatlingCounters[finalI].incrementAndGet() == LOOP_MAX)
                {
                    gatlingLatch.countDown();
                }
            }

            @Override
            public Object context()
            {
                return finalI;
            }
        };
    }

    // run the test
    long gatlingTime = System.nanoTime();
    for (int j = 0; j < LOOP_MAX; j++)
    {
        for (int i = 0; i < contextCount; i++)
        {
            gatling.execute(gatlingRunnables[i]);
        }
    }
    System.err.println("Gatling loop done, waiting...");
    gatlingLatch.await();
    gatlingTime = System.nanoTime() - gatlingTime;
    System.err.println("Gatling finished: " + gatling);
    gatling.destroy();
    return gatlingTime;
}
```

This code fragment runs the above scenarios against each other:

```Java
int LOOP_MAX = 50000;
int contextCount = 16;
long executorTime = getExecutorTime(LOOP_MAX, contextCount);
long gatlingTime = getGatlingTime(LOOP_MAX, contextCount);
System.err.println("executorTime=" + (long) (executorTime / 1_000_000d);
System.err.println(" gatlingTime=" + (long) (gatlingTime / 1_000_000d);  
```

#### Results

The results of running this on windows 10, Intel Core i7 8750H @~3Ghz (12 core)

|       | Executor Time (ms) | Gatling Time (ms) | Gatling threads created |
| ----- | ------------------ | ----------------- | ----------------------- |
| Run 1 | 695                | 610               | 3                       |
| Run 2 | 644                | 524               | 4                       |
| Run 3 | 711                | 529               | 4                       |
| Run 4 | 726                | 465               | 3                       |

On Ubuntu 20.04, Intel Core i3 M380 @2.53Ghz (4 core)

|       | Executor Time (ms) | Gatling Time (ms) | Gatling threads created |
| ----- | ------------------ | ----------------- | ----------------------- |
| Run 1 | 3106               | 2594              | 9                       |
| Run 2 | 3491               | 2037              | 9                       |
| Run 3 | 3266               | 1613              | 7                       |
| Run 4 | 3301               | 1840              | 8                       |

On Windows 10, Intel Core i3 M380 @2.53Ghz (4 core, same host as Ubuntu test)

|       | Executor Time (ms) | Gatling Time (ms) | Gatling threads created |
| ----- | ------------------ | ----------------- | ----------------------- |
| Run 1 | 4194               | 2025              | 8                       |
| Run 2 | 4166               | 2361              | 10                      |
| Run 3 | 4772               | 1883              | 8                       |
| Run 4 | 4671               | 1759              | 8                       |

On Ubuntu 20.04, Intel Core i5 4200U @1.6Ghz (4 core)

|       | Executor Time (ms) | Gatling Time (ms) | Gatling threads created |
| ----- | ------------------ | ----------------- | ----------------------- |
| Run 1 | 3594               | 1349              | 6                       |
| Run 2 | 3293               | 1718              | 7                       |
| Run 3 | 3223               | 1785              | 8                       |
| Run 4 | 3517               | 1370              | 6                       |

The common theme for the results is that the GatlingExecutor achieves the same results but with fewer threads and comparable or even improved times. Variation will occur due to OS activities but, in general, the result themes should be consistent across multiple host/OS configurations.

These results also highlight how context switching between threads using a traditional thread-per-context implementation can be slower than using a context-based task allocation framework using a (smaller) pool of available threads (whilst still maintaining in-order task execution). Its not a simple matter to re-design an application to achieve such a goal but the GatlingExecutor can provide real benefit here as the existing executors can be replaced with wrappers delegating to a single GatlingExecutor instance.

By the way, these results are not meant to show any deficiencies in the standard executor, they are aimed to illustrate how context switching across multiple threads can impact throughput of processing. 

## How it works

The core of the GatlingExecutor is a queue framework that is context aware and type aware. The following series of frames illustrates the conceptual function of the queuing internals. For reference:

| Icon | Meaning |
| ----------- | -------------- |
| ![coalescing task](../assets/coalescing%20task.PNG) | A coalescing task (diamond) |
| ![sequential task](../assets/sequential%20task.PNG) | A sequential task (square) | 
| ![runnable task](../assets/runnable%20task.PNG) | A runnable task (triangle) |
| ![main queue element](../assets/main%20queue%20element.PNG) | A main queue element |
| ![local queue element](../assets/local%20queue%20element.PNG) | A local queue element |
| ![element with sequential context tasks](../assets/element%20with%20sequential%20context%20tasks.PNG) | A main queue element holding a sequential context that has 2 tasks |

### Main queue vs local queue

The queue management is broken down into 2 levels; a main queue for the GatlingExecutor and a local queue per thread (a bit like L2/L1 caches in a CPU). Each queue holds an ordered list of elements and every element holds only **one type** of task for a single **context**.

The main queue is where new tasks are added, it provides a staging area to feed the threads. Each element in a queue holds only 1 type of task; coalescing, sequential or runnable.

When an element is taken from the main queue for processing, the element moves onto the local queue of the thread (not a *thread-local*). The element stays in the local queue until it is finished (if the element holds a sequential context, the element can have multiple tasks to process)

The principle purpose of having the main/local queue split is to reduce contention on the main queue, especially for sequential context processing where the element is returned to the queue until it rotates round again.

### Adding to the queue

New tasks are always added to the main queue. The general rules for inserting into the **main queue** are:

- Tasks that have no context represented in the queue are added into a new element at the back of the queue.
- For tasks that have a context in the queue:
  - Sequential tasks of the same context are added to an internal list in the appropriate queue element.
  - Coalescing tasks of the same context simply overwrite in the queue element.

*Note* if an element is in a local queue and a task of the element's context is added to the executor, it will be added to the element in the local queue (frame 3 shows this).

A standard runnable has no explicit context, it is its own context so is always (no exception) added into a new element at the back of the main queue.

In frame 1 below, the main queue has 6 elements composed of sequential, coalescing and runnable task types. The contexts are represented as colours and labelled to indicate the context and sequence, e.g. purple context has tasks p1, p2, p3 whilst blue context has b1, b2 (and b3 later on).

![gatling%20seq%201](../assets/gatling%20seq%201.PNG)

Frame 1 shows 3 tasks being added:

- The sequential task p3 is added to the list of sequential tasks for the purple context. 
- The runnable task 3 is added into a new element at the back of the queue. 
- The coalescing task g2 will overwrite the existing coalescing task g1 for the green context.

The next frame shows the queue population after the 3 adds complete.

### Executing tasks from the queue

Generally, the thread(s) in a GatlingExecutor follow a set cycle of *poll-execute-push*:

1. *poll* the main queue (this is optimised for lock contention in the code, for simplicity just assume we poll)
   - if empty, poll the local queue
2. *execute* the task in the element
3. *push* the element to the local queue if there are remaining tasks in the element (generally only applies for sequential contexts)

The push back onto the queue for sequential contexts with multiple tasks prevents **thread starvation** of other tasks and gives fair access to computation for all tasks in the local queue. The sequential context will have its next task computed when the element rotates to the head of the queue.

Frame 2 below shows thread T1 starting its poll-execute-push cycle:

![gatling%20seq%202](../assets/gatling%20seq%202.PNG)

In frame 3, T1 has polled the main queue and popped the blue sequential context element; T1 executes the first task b1. At the same time as this happens, a 3rd blue sequential task b3 is added; this will be add at the back of the list of tasks in the element. Thread T2 polls (note coalescing task g3 is being added and will replace g2).

![gatling%20seq%203](../assets/gatling%20seq%203.PNG)

In frame 4 T1 pushes the blue context element to the back of its local queue; blue context still has 2 sequential tasks; b2 and b3. T2 executes coalescing context g3 (note g2 was skipped as it was replaced by g3).

![gatling%20seq%203](../assets/gatling%20seq%204.PNG)

Frame 5 shows T1 goes back to polling to start a new cycle. T2 does not push because the element is empty after executing the coalescing task g3.

![gatling%20seq%203](../assets/gatling%20seq%205.PNG)

Frame 6 shows T1 in the execute part of the poll-execute-push cycle, T2 in in the poll phase.

![gatling%20seq%203](../assets/gatling%20seq%206.PNG)

Frame 7 has T1 in the push phase of its cycle, T2 in the execute phase.

![gatling%20seq%203](../assets/gatling%20seq%207.PNG)

Frame 8 shows T1 starting a new cycle, T2 will also start a new cycle when T1 finishes its poll.

![gatling%20seq%203](../assets/gatling%20seq%208.PNG)

Frames 9 and 10 show the continuation until the main queue is drained.

![gatling%20seq%209-10](../assets/gatling%20seq%209-10.PNG)

### Task transfer

Now in frame 11 we see something new; a checker thread transfers one of the elements from the local queue of T1 to T2. This is an important feature of the queue internals; active task transfer of pending local queue elements. The checker thread runs every 250ms (configurable) and will transfer elements from busy threads to free threads.

![gatling%20seq%2011](../assets/gatling%20seq%2011.PNG)

The remaining frames 12 -12d show the sequential contexts being executed in a poll-execute-push cycle across the two threads.

![gatling%20seq%2012a-d](../assets/gatling%20seq%2012a-d.PNG)



### The net effect

Here we can see what the net effect of the poll-execute-push cycles across the two threads is;

- The blue (b1-b3) and purple (p1-p3) sequential contexts had their tasks executed in the correct order.
  - **note**: the blue context (b1-b3), whilst still executed in order, switches from T1 to T2.
- The coalescing contexts (g3, y1) only had the most recent submitted task executed.
- The standard runnable tasks were arbitrarily executed.

![gatling%20seq%203](../assets/gatling%20net%20effect.PNG)

#### More threads

If there were 4 threads for the above scenario we could expect all tasks to be finished in 1/2 the time and possibly in the following sequence:

![gatling net effect 4 threads](../assets/gatling%20net%20effect%204%20threads.PNG)

#### Thread-locals

Sequential tasks can move between threads, depending on the queue scheduling and task transfer timings. This has an impact if any of the tasks use thread-locals; if the sequential tasks switch threads (like b2, b3 in the diagram above), state held in thread-locals will not be visible to the remaining tasks of sequential context.

### The thread pool

Queuing logic that is context aware and task aware solves the problem of maintaining task order when sharing multiple threads for task processing and offers *some* throughput improvement (using coalescing type tasks). However, task throughput for multiple contexts will still be governed by how many threads are available, especially if some tasks are slow to complete (e.g. perform I/O operations). The GatlingExecutor offers a solution to this by using an *elastic* thread-pool.

The GatlingExecutor has a fixed number of *core* threads; these remain alive for the lifetime of the executor. Extra threads (auxiliary threads) can be spawned as needed (the elastic nature of the pool) and these will terminate after 10 seconds (configurable). 

#### Core count bump up

If auxiliary threads are created too often, the core count will increase by 1 for every 10 times (configurable) that the core count is exceeded (so a minimum of 100 seconds before increasing if an extra thread was needed every 10 seconds).

#### Thread spawning conditions

The checker thread performs task transfer duties as well as thread spawning. Auxiliary threads will be spawned for any of these conditions during a check cycle:

1. A single thread is spawned to transfer one element from the local queue of one busy thread if all other threads (including other auxiliary) are busy
2. If any thread is in a blocked state, its entire local queue is transferred to another thread or, if there are no available threads, a new thread is spawned and given the local queue.

## Summary

Hopefully you have not found this too dry and have an understanding of the fundamental operation of the GatlingExecutor. It has a specific aim; to reduce the number of threads in a multi-threaded runtime whilst achieving the same in-order task execution and processing latency. This can be applied to many systems that operate on a thread/executor-per-context basis. Every application is different and some will benefit more than others. In the end, good multi-thread application performance comes from good system design; the GatlingExecutor just helps to make the design simpler.



