# Runtime

Reactor lifecycle, scheduling, and asynchronous execution components belong here.

Runtime mechanisms remain native Seastar operations. Kwaque adds only the
contracts needed to keep those operations bounded and ownership-safe: always-on
shard affinity, explicitly reviewed cross-shard values, point-to-point bounded
byte copies, task admission/draining, and managed sharded-service lifecycle.

Operational failures contain a stable Kwaque error, operation kind, and at most
four numeric context fields. They carry no borrowed category or message storage,
so a typed result may cross a shard only when its successful value is also an
approved owned value. Cross-shard invocation retains callable and argument state
through asynchronous completion unless a function pointer's declared parameters
prove the direct value-only native path safe.

Production operation counters retain direct fixed-width updates. Adapters and
returned handles use a shard-checked lightweight native shared owner only for
the lifetime of the counter block, ensuring an accepted file or network
operation cannot reference statistics destroyed with a backend. Each adapter
caches the retained block's direct pointer at construction, so operation updates
perform no reference-count or duplicate affinity work.

Closing a task scope first closes admission and requests abort, then waits for
all accepted work. `admission_closed()` reports only the first condition; the
future returned by `close()` is the drain-completion boundary.

The production environment is one shard-local composition root for the task
scope, runtime adapters, resource manager, event sink, and their metrics. It
uses the same explicit constructed/starting/started/stopping/stopped lifecycle
as the simulation environment. Shutdown closes component admission and drains
capability and workload leases before destroying their owners. Terminal
lifecycle reporting and event-sink shutdown complete before the resource
manager releases its process-registry lease. Fault configuration and probe
ownership are absent from the production type.

Runtime capability leases remain unavailable until environment startup has
fully prepared every adapter, resource owner, metric, and ready event. They are
closed before teardown and cannot be reacquired after failed startup or stop.

Logical file operations and network writes may span many fragments, but each
physical file read, network read result, or staging allocation is capped at 128
KiB. Common one-chunk native I/O keeps its direct continuation path; larger or
contended state machines remain coroutines. Directory results use chunked
storage, and the explicit point-to-point cross-shard byte value has the same
contiguous ceiling. Waiting-task and worker ceilings are kept independently of
byte admission so small requests cannot create an excessive fiber population.
