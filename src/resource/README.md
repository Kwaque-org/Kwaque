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
Per-queue worker, producer-waiter, and manual-consumer ceilings also prevent task
metadata from becoming an unaccounted memory multiplier. Manual consumers default
to at most 64 pending waits, with a configurable limit from zero to 64. Saturation
returns `consumer_waiters_exhausted` before another wait is retained; a zero limit
permits ready pops only. Cancellation and completion return capacity. Managed
consumers instead use their configured worker count as the bound.

Started workers are required until queue shutdown. Optional item failures must
be explicitly recognized by the owner's failure classifier before bounded error
reporting can continue processing. An unclassified failure or a throwing classifier
or reporter is fatal, including when the reporting budget is exhausted.

Components obtain one move-only workload lease during startup. It supplies the
copyable scheduling/SMP handles and a shard-local native memory semaphore while
preventing manager/registry teardown. The lease must outlive all units and
pending waits obtained from those handles. Components drain their gates and
queues, return memory units, and release the lease before the shard manager and
process registry stop.
