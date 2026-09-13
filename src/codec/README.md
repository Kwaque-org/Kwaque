# Codec

`crc32c.h` provides a four-byte incremental Castagnoli checksum state. Start at
zero, or resume from a finalized CRC value; `extend` consumes an explicit byte
span and `value()` returns the finalized value. Empty spans preserve the seed.
Encode checksum fields little-endian through the integer primitives. Character
array literals require an explicit span/view so a terminator is not included
accidentally. Updates are synchronous, and native initialization can allocate or
throw; growing traversal owners supply their work bounds.

`digest.h` contains the raw 32-octet SHA-256 value and distinct semantic-batch,
checkpoint, immutable-object and extent value types. They own their bytes and
return owning snapshots. All-zero is a valid value; owners represent absence
separately. The value types carry no hashing implementation or verification state.

`sha256.h` owns the incremental native SHA context. Keep the nonmovable hasher in
its owning scope, feed bytes through `update`, then call `std::move(hasher).final()`
once. Native failures retain their exception channel. Semantic prefix arrays
include exactly one zero terminator; use their full extent. Field-owning codecs
provide the specified encoded fields. Exact-object and extent hashing consume
their named bytes without an added prefix; wrapping the digest in a distinct
value type never changes the hash input.

Integrity tests cover independent known answers, split/fragmented input and
separate-process initialization. Failed-initialization probes exit after
observing exception propagation and wrapper state; they do not test recovery
or native teardown. Warmed allocation checks remain separate from first use.

`cooperative.h` carries one immutable policy and shared residual byte/item work
allowances through sequential nested operations. Admit a conservative bound
before each synchronous leaf; oversized leaves must be split or rejected.
Empty work has a positive item cost. Poll cancellation after awaits and keep the
final poll and publication together without another suspension. Cleanup ignores
cancellation while retaining bounded work. Its inline admission path supports
teardown using the native yield awaiter without allocating another coroutine.
Use `co_await work.drain_inline(bytes, items)` for that path: it owns the required
checkpoint and charges the work before returning control. Callers do not manage
a separate boolean/yield protocol. `drain(...).get()` remains available inside a
Seastar thread.

`crc32c_cooperative.h` and `sha256_cooperative.h` consume owning buffers and split
hash updates within fragments. They use separate algorithm dependencies. The
caller admits physical input storage, coroutine frames and verified native
engine/provider/context costs against its remaining memory allowance. These
drivers check recorded buffer bounds; they do not measure native allocation
peaks. Input and native context cleanup complete before the final abort check.

`staging_cooperative.h` assembles two owning inputs in bounded splices. It charges
actual descriptor history through bounded cost queries, reserves private builder
descriptors once, and bounds overlapping share/publication storage and new tails.
Small donations follow the builder's packing rule; qualifying large donations
retain their backing. Both source objects must be distinct. Only completed output
escapes after cleanup and a final cancellation check.

`collection.h` reads and writes canonical count/key/value sequences. Keys must
be strictly ascending, and decoding rejects a duplicate or descending key before
calling its value decoder. Callbacks use the enclosing shared memory/work bounds
and private staging; their owner discards that staging on failure. Failed reads
restore the parser cursor, while memory reservations retain their own lifetimes.
Encoding accepts an lvalue range, validates order and does not sort implicitly.
Its owner admits builder capacities and reserves contiguous space for the count
prefix before the call; callbacks account subsequent field writes.

The separate `canonicalize_unordered` constructor consumes a native chunked FIFO
of fixed-size metadata entries, sorts bounded runs, and performs a complete heap
merge. It checks served run/table/heap/output capacities and aggregate residual
budgets before allocating. Comparisons, moves and destruction must have reviewed
bounded work with no hidden allocations. Reserved free input chunks and cleanup
shapes that cannot fit reject before ownership transfer. Failures drain private
owners cooperatively; the returned owner's eventual disposal belongs to its
caller.

`error.h` defines an owning 16-byte diagnostic and `codec::result<T>`. Errors
retain a generic reason, family, field and byte offset without payload text or
borrowed storage. These results are distinct from the base results used by
`limits.h`.

`integer.h` provides explicit little-/big-endian fixed-width reads and writes
for unsigned 8/16/32/64-bit and signed 32/64-bit values. Canonical unsigned varints
support 32/64-bit values, reject redundant or overflowing encodings, and probe
at most five/ten bytes before committing. This strict profile is separate from
protobuf's varint rules. Scalar reads use no shares or parser checkpoints.

`field_context` supplies the current input/output's origin and trusted diagnostic
identifiers. Offsets remain in the outer byte coordinate system. Short input is
`truncated_data` at an open boundary and `malformed_data` inside a complete parent;
both identify the first missing byte. Invalid coordinate contexts reject before
mutation. Writers append a single scalar to the owner's private builder and
preserve earlier logical bytes on failure; allocation exceptions propagate.
The owner supplies builder limits and accounts backing and operation costs.
Scalar helpers do not publish complete objects or grant additional memory.

Signed 32/64-bit varints use unsigned-bit-pattern ZigZag. Nullable lengths are
signed 32-bit varints: only -1 is null, zero is present-empty, and other negative
values are malformed. `read_nullable_length` checks the allowance and available
payload before consuming only the prefix; the payload remains for its owner.

`transaction.h` provides synchronous composition. `with_transaction` owns one
parser checkpoint and rolls it back on failure or exception. `decode_exact`
peeks an owning bounded child, supplies an absolute child origin and a complete
input boundary, requires exact consumption, then advances the parent once.
Callbacks return reviewed owning results with nonthrowing moves/destruction;
they keep parsers unmoved, balance marks, and bound their own work/allocations.
Known direct borrowed results and native futures are rejected by the interface.

Allocation-cost queries on the existing buffers/parsers inspect recorded backing
and descriptor capacity, including capacity retained after trimming. They also
reserve possible share-control promotion. Supply a stable, nonallocating,
nondecreasing charge function verified for the actual allocator profile; there
is no guessed default or allocator introspection. Additional resources hidden
behind opaque native owners remain the producer's accounting responsibility.

Call `reserve_decode_input` before structural sharing and retain that reservation
for the input's lifetime: a failed child can still promote its parent's native
ownership. `decode_budget` carries separate operation and metadata remainders.
Children receive reduced snapshots; callers account additional allocations and
retain reservations for returned owners. Cursor rollback does not refund native
ownership changes. Child admission conservatively checks the reported backing,
fragment and individual allocation bounds as well as new descriptor costs.

`staging.h` assembles an encoded prefix and an owning payload through a private
builder. It checks supplied allocator charges, retained input, tail allocation,
the preallocated descriptor array transferred at publication and synchronous copy/work bounds before
allocating. Only completed output is returned. The payload is consumed at entry
and can be lost on failure; borrowed prefix storage stays immutable through the
call. Other live usage includes prefix backing and opaque owner resources, while
the donor components and new staging are charged by the helper. New allocation
ceilings do not reject a pre-existing donor solely for having a larger allocation.

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

## Binary envelopes and evolution

`format_registry.h` contains a passive, constant family registry. It reserves
codes 1 through 10 for submitted batches, assigned batches, segment headers,
segment batch blocks, WAL prepares, durable-boundary footers, sealed extents,
sparse indexes, range manifests and read checkpoints. Registration describes
framing compatibility; the concrete body codec and its owning service supply
payload validation and decide whether that payload may be written or advertised.
An entry alone does not establish an implemented payload or an active service.

Reader support, writer output and activation have separate responsibilities:

- Each current descriptor has reader `current=1`, `oldest_readable=1`, writer
  version 1, minimum reader version 1 and required-feature support zero.
- After header integrity, readers reject `minimum_reader > writer` as malformed,
  then reject zero versions as unsupported. They require
  `minimum_reader <= current` and `writer >= oldest_readable`, a registered
  nonzero family and support for every required feature bit. Unknown nonzero
  families are unsupported; zero is malformed. There is no opaque-family
  decode-as-success fallback.
- A compatible newer writer can add optional header extensions while retaining
  the exact known body grammar. For example `(writer=2, minimum_reader=1)` may
  be readable, while `(2,2)` is unsupported by a v1 reader and `(1,2)` is
  malformed. Numeric compatibility does not permit extra or missing body fields.
- Production writers always emit `(1,1)`, a 32-byte header and zero feature bits.
  No production extension tag is assigned. Optional-read tolerance grants no
  permission to emit an unassigned tag or switch the writer's version. Synthetic
  newer-writer and extension construction is confined to tests.
- Activating a changed writer is an explicit decision by its payload/service
  owner after the required readers are available. This library provides no
  negotiation, cluster rollout or writer-activation service.

The `KQBF` prefix uses explicit little-endian fields. Header CRC32C covers all
declared header bytes, including the body CRC, with its own four-byte slot at
offset 28 replaced by zeros. Body CRC32C covers the exact encoded body, including
any counted padding owned by that body's grammar. Header extensions use ordered,
unique, nonzero u16 tags, u16 flags and u32 lengths. Flag bit 0 means mandatory;
other bits are unsupported. Unknown optional values are bounded, checksummed and
skipped. Unknown mandatory tags reject, after their full value extent is checked.

`envelope.h` exposes an unverified fixed-prefix inspection and a current-profile
prefix encoder. `envelope_integrity.h` checks only the supplied raw header CRC.
`envelope_decode.h` combines framing, integrity, compatibility and the independently
requested family before invoking a statically supplied asynchronous body decoder
on an exact complete child. One parent checkpoint covers the operation. The
body must be fully consumed; errors, exceptions and observed cancellation restore
the parent's cursor and existing marks. Temporary child and callback owners are
released before the final abort poll and commit. An eighth owned mark remains
usable, while acquiring a ninth fails without disturbing caller marks.

Before decoding, reserve the parent's backing, descriptor history and possible
share controls once through `reserve_decode_input`. Keep that reservation for
the input's lifetime, including after rejected children. Supply the residual
after verified native/frame/callback costs; each additional alias is admitted
before creation. Sequential aliases release their reservations before the next
one uses the same remaining allowance. Returned owners retain their own charges.

`envelope_encode.h` consumes an already encoded body before its first suspension,
including on later rejection. It completes body CRC before header CRC, freezes a
private header, and uses cooperative staging for the final immutable output.
Its `other_live` and `parent_remaining` convention matches the staging writer:
exclude the body/header/alias/staging charges that the writer admits itself.
All phases share work and cancellation state. Cleanup preserves the first failure;
only completed output escapes after the final abort poll. Payload grammar,
expected object/generation/position and storage-padding rules belong to body owners.

Preserving known semantics is different from preserving an immutable object's
encoded identity. A reader can skip an optional extension, but re-encoding the
known value omits those bytes and emits the current writer profile. Exact-object
digests must therefore cover the original encoded extent. Consumers that need
opaque-byte preservation must retain that extent rather than reconstruct it
from a decoded value.

A new family or extension must name its payload owner, assign an unused stable
code, specify complete field/length/integrity rules and bounds, implement its
reader and writer, and provide independent compatibility/rejection fixtures.
Incompatible body changes need an explicit decoder and minimum-reader change.
Reader availability and the owner's activation decision must precede emission.
Codes are never silently repurposed. Metadata, snapshots, remote manifests and
transport negotiation gain no speculative payloads from this registry; transport
version rules require their own owning protocol specification.

Run the focused tests with:

```bash
bazel test --config=ci-debug \
  --build_tag_filters=-fuzz,-manual --test_tag_filters=-fuzz,-manual \
  //src/codec/tests:all
```


`codec_fuzz` exercises scalar, nullable, transaction and ordered-pair codecs
against independent byte/error/cursor oracles, plus CRC backend and incremental
equivalence. Marker `0xe0` additionally exercises raw and structured envelopes;
mutations can recompute body and header CRCs to reach deeper validation.
`codec_cooperative_fuzz` composes integrity, staging, collections and shared empty
work with scripted cancellation. Selector 4 additionally exercises envelopes
with initial, queued, body-decoder and cleanup cancellation, narrowed work,
exhausted residuals and existing checkpoints. Both retain their prior dispatch
for every input and add envelope work after it. A shared test-only oracle checks
exact errors, absolute offsets, callback admission, decoded values and unchanged
suffixes independently of the production encoders and checksum engine.

Both fuzzers use the existing reactor bridge, own input before crossing it and
join all work and queued observers. Their fixed corpora are executed explicitly
by CI. `envelope_fuzz_cases_test` also exercises the oracle's independent facts
and mutation matrix as an ordinary native test. The 16-KiB input/generated-wire
cap does not qualify maximum-size operation or actual suspension.
`codec_qualification_test` supplies separate maximum-fragment, packing and merge
progress cases, plus 0/1/64-extension envelope headers, the 4,096-byte header
boundary and cancellation after body progress.

Native-only envelope qualification additionally decodes a 16-MiB body built from
64-KiB fragments under a 4,096-byte header. It observes reactor progress during
body verification, records native allocation counts after fixture setup, and
checks retained payload capacity at proven allocation bases. Its tighter native
large-span charge profile is not used to qualify the system allocator; that
profile-specific maximum-body case is skipped there. These observations do not
measure transient peak memory, compiler-frame sizes or whole-process usage.

The retained-payload qualification records usable sizes only for proven native
allocation bases, then verifies that no-copy staging preserves those same
allocations. This observation excludes descriptors, share controls, coroutine
frames, native providers and transient allocations. The cold process test writes
`integrity-cold-measurements.json` and raw sample logs under its Bazel undeclared
outputs. Each measurement uses a fresh process for one 32-, 48-, 65- or 4096-byte
CRC call, or one SHA context lifetime. Native memory observations are reactor-local
allocation counts, requested-byte totals and allocator page occupancy. System
allocator memory counters are explicitly unobserved. Neither source is an exact
whole-operation peak measurement.

`codec_bench` contains paired CRC throughput, prefix-field extension,
fragmentation, copying and cooperative-owner cases, plus validated fixed-width
and canonical-varint read/write comparisons. Both sides use matching call and
ownership boundaries; setup and validation are outside measurement windows.
Samples count whole checksums/copies/owners or individual scalar operations,
never bytes. Native allocation and reactor-task counters use the same windows.
The harness calibrates and warms its fixtures, so these are warm measurements.
The linked comparison CRC build is named `google_configured`, including its
portable ARM path; the current codec backend is named `abseil_selected`.

Run all release validation and benchmark work before switching to the fuzz
profile. Select fresh result directories for each comparison:

```bash
bazel test --config=ci-release \
  --build_tag_filters=-fuzz,-manual --test_tag_filters=-fuzz,-manual \
  //src/codec/tests:all
bazel build --config=ci-release //src/codec/tests:codec_bench

kwaque_codec_pairs=()
for size in 32 48 63 64 65 255 256 257 1007 1008 1009 4032 4080 4096 16256 16384 65536; do
  kwaque_codec_pairs+=(--pair "crc_hot_${size}.google_configured=crc_hot_${size}.abseil_selected")
done
for group in crc_fields_32 crc_fields_48 crc_frag_65536_64 crc_frag_65536_4096 crc_owner_131072_131072 crc_owner_1048576_4096; do
  kwaque_codec_pairs+=(--pair "${group}.google_configured=${group}.abseil_selected")
done
for size in 4096 65536; do
  kwaque_codec_pairs+=(--pair "crc_copy_${size}.google_copy_then_crc=crc_copy_${size}.abseil_fused_copy_crc")
done
kwaque_codec_pairs+=(
  --pair codec_fixed64_contiguous.native_validated_read=codec_fixed64_contiguous.codec_read
  --pair codec_fixed64_frag13.native_validated_read=codec_fixed64_frag13.codec_read
  --pair codec_varuint64_contiguous.canonical_baseline_read=codec_varuint64_contiguous.codec_read
  --pair codec_varuint64_frag7.canonical_baseline_read=codec_varuint64_frag7.codec_read
  --pair codec_fixed64_contiguous.native_validated_write=codec_fixed64_contiguous.codec_write
  --pair codec_varuint64_contiguous.canonical_baseline_write=codec_varuint64_contiguous.codec_write
)
python3 tools/compare_benchmarks.py \
  --binary=bazel-bin/src/codec/tests/codec_bench \
  --output-dir=.cache/codec-bench-pairs \
  "${kwaque_codec_pairs[@]}"
```

The paired driver retains binary identity and native profile, fixes CPU affinity,
and requires three rounds with at least seven samples per case. Its timing,
allocation and task gates apply only to the selected equal-work pairs. Full
backend qualification also needs native architecture coverage, cold behavior,
verified peak reservations and control progress; these results do not silently
change the production backend.

The envelope cases add 29 paired comparisons to the same `codec_bench` binary.
Prefix and TLV cases isolate their declared leaves; full decode and encode cases
include both the operation and caller-owned input/result disposal. The native
harness accumulates time and allocation/task deltas across those two measured
windows, with byte/value validation between them excluded on both sides. Setup,
allocator-base probes, CRC warm-up and fixture construction are outside timing.

The `checked_*` alternative retains the same validation, ownership, admission,
cancellation and cleanup obligations using native scalar access and the configured
Google CRC32C backend. The `codec_*` side calls the production implementation with
its selected CRC backend. Both cross matching translation-unit boundaries; the
same body grammar and primitive storage/work mechanisms are shared. These pairs
compare checked mechanisms and CRC backends, not complete brokers or a cold-start
path. The comparison backend retains its configured portable ARM behavior.

The matrix covers contiguous and fragmented prefixes, 0/1/64 TLVs, 64-byte and
32-KiB bodies, and fragmented 16-MiB bodies. The 64-TLV benchmark header is 1,056
bytes with eight-byte values; the full 4,096-byte boundary is covered by the
separate fuzz and qualification cases. Encode pairs always use the actual
extension-free writer profile. Allocation/task counts and paired timings remain
separate from peak-memory and control-progress qualification.

After the release build above, run all envelope pairs into a fresh directory:

```bash
kwaque_envelope_pairs=(
  --pair envelope_prefix_contiguous.checked_read=envelope_prefix_contiguous.codec_read
  --pair envelope_prefix_contiguous.checked_write=envelope_prefix_contiguous.codec_write
  --pair envelope_prefix_frag1.checked_read=envelope_prefix_frag1.codec_read
)
for count in 0 1 64; do
  for layout in contiguous frag7; do
    group="envelope_tlv${count}_${layout}"
    kwaque_envelope_pairs+=(--pair "${group}.checked_scan=${group}.codec_scan")
  done
done
for size in 64 32768; do
  for count in 0 1 64; do
    for layout in contiguous frag67; do
      group="envelope_body${size}_ext${count}_${layout}"
      kwaque_envelope_pairs+=(--pair "${group}.checked_decode=${group}.codec_decode")
      if [ "$count" = 0 ]; then
        kwaque_envelope_pairs+=(--pair "${group}.checked_encode=${group}.codec_encode")
      fi
    done
  done
done
for count in 0 1 64; do
  group="envelope_body16777216_ext${count}_frag32768"
  kwaque_envelope_pairs+=(--pair "${group}.checked_decode=${group}.codec_decode")
done
kwaque_envelope_pairs+=(
  --pair envelope_body16777216_ext0_frag65536.checked_encode=envelope_body16777216_ext0_frag65536.codec_encode
)
python3 tools/compare_benchmarks.py \
  --binary=bazel-bin/src/codec/tests/codec_bench \
  --output-dir=.cache/envelope-bench-pairs \
  "${kwaque_envelope_pairs[@]}"
```

Use a different output directory for each rerun. The existing driver requires
three rounds with at least seven samples, verifies the actual native production
profile and binary identity, and retains raw results and its gate decisions.

Collection construction and staging owner cases report operation costs without
an independent comparison claim. Their returned owner's validation/disposal is
outside construction timing:

```bash
mkdir -p .cache/codec-bench-owners
bazel run --config=ci-release //src/codec/tests:codec_bench -- \
  --no-perf-counters --runs=7 --duration=1 \
  --test='codec_(unordered|staging)_.*' \
  --json-output="$PWD/.cache/codec-bench-owners/owners.json"
```

Run the bounded fuzz smoke after completing release measurements:

```bash
bazel test --config=ci --config=fuzz \
  --test_output=all --test_arg=-max_total_time=2 --test_arg=-seed=1 \
  //src/codec/tests:codec_fuzz \
  //src/codec/tests:codec_cooperative_fuzz
```

Cooperative buffer assembly transfers complete published fragments, including
small fragments, without packing or splitting their backing. It reserves one
output descriptor array and one temporary slice at a time. Input backing and
its reservations remain with returned aliases until they are released;
consuming a donor does not imply destroying its backing during the operation.
Synchronous assembly copies the borrowed prefix, uses bounded existing-tail
packing, and reserves its output descriptors before mutation.
