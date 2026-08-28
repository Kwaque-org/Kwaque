# Bytes

Fragmented byte buffers and parsers for the record and network paths belong here.

A published `fragmented_buffer` is a move-only sequence of owning fragments whose
bytes are immutable. External temporary buffers are copied into frozen backing,
while shares, slices, and buffer-to-builder splices remain zero-copy between
already-published Kwaque buffers. Logical and retained backing bytes are reported
separately, so a small slice cannot hide the larger backing it keeps alive.
Explicit deep copy coalesces tiny source fragments into allocator-sized chunks;
external freeze preserves supplied fragment boundaries because those boundaries
may describe I/O layout. Contiguous conversion is always explicit and bounded.
Scatter export is independently bounded by vector slots and bytes, owns shared
claims in one descriptor allocation for async lifetime safety, and resumes
through a cursor bound to one buffer generation.
Native packet conversion uses one bounded linear copy into independent mutable
packet backing and is refused before allocation if the receiver cannot
represent the total size.

`fragmented_buffer_builder` is the only surface that mutates content. It fills
spare tail capacity before allocating, grows allocations geometrically up to a
fixed ceiling, packs small frozen fragments into that spare capacity instead of
adding links, and publishes exactly once. Logical bytes, retained backing bytes,
fragment count, and per-allocation size are independently bounded, and a
rejected append leaves exactly the content that preceded it. The packing
threshold is a fixed property of the builder rather than a setting, so
configured limits cannot contradict it.

`fragmented_buffer_parser` reads one owned buffer through a bounded cursor. Reads
are all-or-nothing, so a truncated read leaves the cursor untouched; sub-buffers
it returns are owning and outlive the parser. Fixed-width integer reads state
their byte order explicitly, accept only 8-, 16-, 32-, and 64-bit unsigned
types, and carry no record-format assumptions. Truncated input, an invalid
requested range, and malformed internal state remain distinct typed errors.
Speculative parsing uses a checkpoint stack with a fixed depth.

Buffers belong to one shard and must not be transferred across shards.
