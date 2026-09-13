# Runtime

Reactor lifecycle, scheduling, and asynchronous execution components belong here.

Runtime mechanisms remain native Seastar operations. Kwaque adds only the
contracts needed to keep those operations bounded and ownership-safe: always-on
shard affinity, explicitly reviewed cross-shard values, point-to-point bounded
byte copies, task admission/draining, and managed sharded-service lifecycle.

Operational failures contain a stable Kwaque error, operation kind, and at most
four numeric context fields. They carry no borrowed category or message storage,
so a typed result may cross a shard only when its successful value is also an
approved owned value. Cross-shard invocation submits an owning callable and
argument tuple that the native runtime retains through asynchronous completion,
on both local and remote targets. Detached task-scope calls retain their own
coroutine owner because they do not use native cross-shard submission.

Managed sharded services clean native construction state even when construction
fails before local startup. They stop every successfully started service even
if its abort hook reports an error, preserving the first error. A failing local
start must clean its own partial state, and a constructed service must be safely
destructible without calling stop. Constructor arguments must have nonthrowing
moves for native callback submission; copy and allocation failures are cleaned
up. Concrete lifecycle owners enforce shard
affinity before member destruction.

SMP service groups bound executing remote requests. Components must separately
bound source-side pending tasks and retained bytes before submitting work; a gate
alone tracks lifetime and does not provide that admission bound. Finite lifecycle
fan-out is bounded by the configured shard population.

Production operation counters retain direct fixed-width updates. Adapters and
returned handles use a shard-checked lightweight native shared owner only for
the lifetime of the counter block, ensuring an accepted file or network
operation cannot reference statistics destroyed with a backend. Each adapter
caches the retained block's direct pointer at construction, so operation updates
perform no reference-count or duplicate affinity work.

Closing a task scope first closes admission and requests abort, then waits for
all accepted work. `admission_closed()` reports only the first condition; the
future returned by `close()` is the drain-completion boundary.

A scope can retain an owner callback that receives its first failure immediately,
before close. Required workers select `task_lifetime::until_abort`; returning
before cancellation is fatal. Production environment tasks handle expected
operational errors before returning to their scope, whose terminal callback
aborts on an escaping failure. A callback that throws is itself fatal. Native
adapters translate recognized operational exceptions; unrelated exceptions keep
their identity through cleanup and reach the owning service.

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

Path and DNS-name factories borrow input for validation, then create bounded
owned strings; caller string capacity is not retained. Directory results
validate entries and transfer their values into fresh bounded descriptor
storage. DNS result factories reject excess retained vector capacity. Native
allocation failures in these allocating factories remain exceptional.
