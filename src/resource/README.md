# Resource

Resource accounting, quotas, and admission-control components belong here.

The package provides shard-local byte budgets with abortable bounded waiting,
move-only reservation units, hysteretic pressure observation over committed
bytes, ordered reclaim callbacks behind Seastar's public asynchronous reclaimer,
and FIFO work queues with independent item, byte, and producer-waiter limits.
Queue workers use the configured workload scheduling class, cap concurrency and
error reporting, and drain all owned fibers during shutdown.

A byte budget distinguishes bytes owned by live reservation units from bytes it
has already committed to a waiting caller that has not resumed yet, so its
accounting and its pressure signal stay exact while a release is being handed
over. A work queue reserves one producer-waiter slot for the producer holding
the admission turn, keeping the two suspended-producer populations within one
combined bound.
