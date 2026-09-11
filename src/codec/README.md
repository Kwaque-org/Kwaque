# Codec

`limits.h` defines fixed codec acceptance ceilings and validated immutable limits.
Configure smaller values through `limits::make`; zero and values above the
profile ceilings are rejected. `config()` returns an owning snapshot, and
`intersect()` combines policies without allowing a child to widen its parent.

Configured maxima are independent ceilings. Actual live usage must also fit the
remaining operation budget. `remaining_operation_bytes` checks the sum of all
supplied charges against both the operation ceiling and the explicit parent
remainder, returning a new remainder without mutating or reserving anything.

Charge served backing capacities, including simultaneous old/new allocations.
`decoded_metadata` includes decoded descriptors, auxiliary/share bookkeeping and
metadata migration; the separate payload fields cover only non-decoded costs.
This accounting bounds supplied live costs, not allocator caches or shard RSS.
Owners must charge every allocation and propagate the actual parent remainder.

`validate_allocation` checks each served/reserved contiguous capacity.
`validate_buffer` checks conservative backing/fragment accounting against a
caller-supplied complete-buffer logical cap. A body cap excludes its enclosing
header, while page and checkpoint caps cover complete encoded objects.
`validate_batch_counts` bounds nonempty data-batch counts and aggregate headers;
record contents, exact per-record totals and format grammar require their owner.

Run the focused limit tests with `bazel test //src/codec/tests:limits_test`.
