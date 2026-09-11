# Model

`identity.h` provides distinct 16-byte object identities and a checked 32-bit
vnode index. Identity construction copies exactly 16 octets and requires at
least one nonzero octet. Default identities are nil staging values and must
not be published. Registries own uniqueness, non-reuse and context membership.

Byte views borrow from a living identity and its enclosing owner. The named
`canonical_less` comparison orders opaque bytes without implying chronology or
lineage. Hash customization includes all identity bytes and supplies no stable
serialized hash value.

Vnode construction validates a non-nil cluster, a positive power-of-two ring
size representable as `uint32_t`, and an index below that size before narrowing.
The index retains no cluster or ring ownership; callers keep that context.

`epoch.h` defines distinct segment, routing, lease, producer and manifest
epochs/generations. Published values start at one; default zero is uninitialized.
Checked successors distinguish uninitialized values from exhausted counters and
never wrap or publish authority. Callers retain the corresponding owner scope.

`batch_identity.h` provides nonzero producer-stream IDs, zero-based batch
sequences and the validated `{producer, epoch, stream, sequence}` BatchID tuple.
Its component accessors return values. Equality and hashing cover the full tuple;
`canonical_less` uses producer octet order followed by numeric components, without
implying append order across streams. Stream allocation and authoritative binding
remain owner responsibilities. The checked `producer_stream_binding` value holds
topic, range, routing epoch, segment and generation; `validate_expected` compares
all five with independently supplied context.

`batch_context.h` separates submitted metadata from assigned logical positions.
Submitted contexts retain the full BatchID, original binding, original count and
timestamp base. Checked assignment requires the expected original binding and
computes the logical span without advancing a counter. Sparse narrowing reduces
the retained count while preserving that original metadata and span; zero
retained data belongs in separate extent/retry metadata. Contexts contain no
payload or fingerprint and cannot verify survivor membership or write authority.
Their original count obeys the absolute codec ceiling; owning codecs also apply
the caller's narrower limits.

`transport_identity.h` provides separate connection-local stream, correlation
and frame-sequence values. Stream zero denotes connection control; sequence and
correlation zero remain representable. These values carry no connection state.

`position.h` separates logical record slots, retained physical record ordinals
and file bytes. Record offsets exclude `UINT64_MAX`; both endpoints of a record
span are full-domain boundaries, so `[MAX, MAX)` is a valid empty span. Checked
arithmetic takes the matching count type and rejects overflow or underflow.
Counts also express deltas in their logical or physical unit; neither is a byte
count. Reversed spans are rejected and containment excludes the upper endpoint.

File-byte spans reuse `runtime::file_position` from the narrow
`src/runtime/file_position.h` header, also available through `src/runtime/file.h`.
All position/span values require the caller's range, layout or file context;
their scalar comparisons do not establish that context.

`record_id` retains `{topic, range, logical offset}` while `physical_address`
holds `{segment, physical ordinal}`. Checked constructors reject nil identities.
Their component accessors return values, and equality/hashing include every
stored component. `canonical_less` supplies collection order;
`compare_in_range` and `compare_in_layout` instead require matching owner
contexts. Physical comparison takes the independently pinned generations;
address equality alone proves neither layout equality nor matching content.

`checked_timestamp_delta` computes signed nanoseconds between supplied wall
timestamps. `checked_timestamp_from_delta` reconstructs a timestamp through
checked duration arithmetic. Both reject unrepresentable results and perform no
clock reads. Model values have no automatic cross-shard transfer permission.

`keyspace.h` provides canonical dyadic intervals with a high-bit prefix and
depth from zero through 64. The root covers the entire hashed `uint64_t` domain;
`UINT64_MAX` is a valid hash point. The separate full boundary represents the
exclusive mathematical endpoint `2^64` and has no ordinary numeric value.
Containment includes equality; overlapping intervals exclude merely adjacent
ones. Buddy checks require distinct equal-depth intervals with the same parent,
and do not grant topology-change authority.

`ordered_keyspace_coverage` checks one interval at a time against a target.
Invalid appends permanently fail that scan; `finish()` only queries whether
coverage is complete. The synchronous `validate_keyspace_coverage` convenience
function accepts at most 64 unordered intervals, sorts fixed pointer scratch
and leaves its input unchanged. Larger callers own ordering, collection bounds
and scheduling around the incremental scan.

`segment_state.h` defines separate append, retention, remote and per-replica
enums with explicit byte codes. Zero is uninitialized and cannot be published.
Checked raw conversion rejects out-of-byte-range values before narrowing, then
rejects zero and unknown codes. `validate_segment_state` checks the required
relationships using explicit offload, expiration and aggregate-cleanup facts.
Facts distinguish unknown, incomplete and complete; a lost or removed replica
alone cannot establish aggregate cleanup. Completed deletion rejects current
offloaded/readable assertions while preserving a historical offload commitment
after remote deletion. The caller verifies evidence and matching object context;
validation does not execute transitions or grant deletion/read authority.

`topic_policy.h` provides immutable positive record-size, segment-size and
segment-lifetime settings. Defaults are 1,048,576 complete encoded record bytes,
1,000,000,000 segment bytes and one hour. The record limit includes its framing
and headers, excluding the batch envelope. `effective_record_limit` intersects
the stored topic limit with a supplied positive codec cap. A configured 512 MB
and 30 minutes uses 512,000,000 bytes and 1,800,000,000,000 nanoseconds.
Segment size is a rolling threshold; storage admission separately accounts for
complete appends, headers, padding and footers. These values perform no clock
reads, configuration updates or rolling.

Run the focused value tests with `bazel test //src/model/tests:value_tests`.
