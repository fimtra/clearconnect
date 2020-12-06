## Task Queue Concepts

ClearConnect uses a bespoke threading model based on a task queue that makes efficient use of available threads. Real-time data systems require tasks in different contexts to be executed in-order; many systems designs end up having single-threaded executors to handle task processing for a single context, leading to a proliferation of threads.

The ClearConnect platform addresses this by having a small (elastic) pool of threads that execute tasks from a specialised task queue that manages two genres of tasks:

*   Sequential tasks; these are tasks that are maintained in sequence with respect to their _context_.
*   Coalescing tasks; these are tasks that can replace earlier, un-executed tasks, in the same _context_.

Both task genres declare the context they are bound to. These two task genres ensure in-order execution of tasks and skipping of old, out-of-date tasks using a single pool of threads. 

The threading model is called the GatlingExecutor and is described [here](../design_notes/the_GatlingExecutor.md).

