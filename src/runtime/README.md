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

Closing a task scope first closes admission and requests abort, then waits for
all accepted work. `admission_closed()` reports only the first condition; the
future returned by `close()` is the drain-completion boundary.
