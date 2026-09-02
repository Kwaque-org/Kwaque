# Observability

Metrics, tracing, diagnostics, and operational visibility belong here.

Structured events use a bounded versioned envelope with fixed event, field,
type, and public-text registries. Event fields are canonicalized by numeric key,
and unsupported event/key or text-role combinations are rejected before an
event becomes visible.

Canonical event bytes are little-endian and allocation-free to encode or
decode. Bounded event logs use a separate `KQEL` artifact, pre-reserve chunked
entry storage, and provide cooperative codec paths for large logs. Production,
test-capture, and simulation sinks share one synchronous compile-time contract;
they do not add a virtual sink hierarchy or detached work.

Callers submit validated event requests without shard or sequence fields. Each
owner-local sink stamps its own shard and monotonic sequence transactionally.
Sequences restart only with an explicitly supplied nonzero sink epoch; event-log
headers retain that epoch and the canonical configuration digest needed to
distinguish reproduction data.

Metric-owning components hold native metric groups directly in optional storage.
Startup failure and stop destroy that storage to unregister callbacks without
allocating a replacement registry.
