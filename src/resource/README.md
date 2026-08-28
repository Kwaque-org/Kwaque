# Resource

Resource accounting, quotas, and admission-control components belong here.

Eight internal workload classes separate foreground, consensus-critical,
replication, metadata, repair, compaction, offload, and maintenance work. The
consensus-critical lane has the highest CPU share and is reserved for
heartbeats, lease renewal/fencing, and authoritative failure response so bulk
work cannot manufacture control-plane failure under overload.

The package partitions shard memory into hard class budgets backed directly by
native Seastar semaphores. Components cache the semaphore supplied by their
workload lease and use `try_get_units`/`get_units` without a generic reservation
wrapper. Reserved bytes are derived from capacity minus the native counter;
that remains exact while `signal()` hands units directly to a readied waiter.
Components must bound their own pending waits rather than adding an unbounded
generic admission layer.

Future reclaimable components own Seastar's public asynchronous reclaimer
directly and keep their pressure policy local. FIFO work queues provide the
bounded component boundary: independent item, byte, and
producer-waiter limits, typed queue outcomes, configured scheduling classes,
bounded worker concurrency/error reporting, and complete fiber draining. A
managed queue draws native units from the same per-class semaphore as direct
reservations and reserves one waiter slot for the producer holding its admission
turn. Its optional worker set is owned by the queue, is started at most once,
and retains each item's memory units until that item's handler completes.

Components obtain one move-only workload lease during startup. It supplies the
copyable scheduling/SMP handles and a shard-local native memory semaphore while
preventing manager/registry teardown. The lease must outlive all units and
pending waits obtained from those handles. Components drain their gates and
queues, return memory units, and release the lease before the shard manager and
process registry stop.
