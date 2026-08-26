# Bytes

Fragmented byte buffers and parsers for the record and network paths belong here.

A published `fragmented_buffer` is a move-only sequence of owning fragments whose
bytes are immutable. Every share or slice keeps its own claim on the backing
storage, so it stays valid after the buffer it came from is gone. Sharing and
slicing copy no payload; contiguous conversion is always explicit and bounded,
and rejected requests are counted rather than silently satisfied. Scatter
export is independently bounded by vector slots and bytes and resumes through
an explicit cursor, including from the middle of a fragment. Packet transfer
moves all fragments in one pass and is refused before ownership moves if the
receiver cannot represent the total size.

`fragmented_buffer_builder` is the only surface that mutates content. It fills
spare tail capacity before allocating, grows allocations geometrically up to a
fixed ceiling, packs small donated fragments into that spare capacity instead of
adding links, and publishes exactly once. Item, byte, fragment-count, and
per-allocation limits are all bounded, and a rejected append leaves exactly the
content that preceded it. The packing threshold is a fixed property of the
builder rather than a setting, so configured limits cannot contradict it.

`fragmented_buffer_parser` reads one owned buffer through a bounded cursor. Reads
are all-or-nothing, so a truncated read leaves the cursor untouched; sub-buffers
it returns are owning and outlive the parser. Fixed-width integer reads state
their byte order explicitly, accept only 8-, 16-, 32-, and 64-bit unsigned
types, and carry no record-format assumptions. Truncated input, an invalid
requested range, and malformed internal state remain distinct typed errors.
Speculative parsing uses a checkpoint stack with a fixed depth.

Buffers belong to one shard and must not be transferred across shards.
