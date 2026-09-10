# Admin

The loopback-friendly administrative HTTP service exposes:

- `GET /v1/health/live` for process liveness.
- `GET /v1/health/ready` for completed-startup readiness.
- `GET /v1/version` for version, revision, and build-mode metadata.
- `GET /metrics` for Prometheus-formatted runtime metrics.

The metrics route reads the native default registry and currently publishes the
fixed broker, task, timer, file, network, DNS, and resource-manager families.
Shard is aggregated for runtime totals; resource-manager families retain only
the fixed workload label. No path, host, object, or other dynamic product label
is exported. Bounded-queue and simulation owners are not part of the current
broker composition.

Readiness and liveness both become false during drain, before shutdown stops
accepting administrative connections. A draining endpoint can still serve
read-only diagnostics while its owners remain alive. Supervisors must allow
graceful drain despite this liveness response. Error responses use a stable JSON envelope containing `code`,
`message`, and `correlation_id` fields. Lifecycle state, request counters, route
handlers, and metric ownership are shard-local; process-level metrics use native
metric aggregation rather than shared cross-core counters.

The listener admits at most four connections per shard before starting HTTP
processing. Each connection serves one request and closes. Request lines are
limited to 2 KiB, the complete request header to 8 KiB, and header fields,
including repeated and folded fields, to 32 physical lines. Nonzero or malformed
content lengths and transfer encodings are rejected before reading a body.
Headers must complete within five seconds; the complete exchange, including
response output, has a fifteen-second absolute lifetime. Incoming bytes do not
extend either deadline. TCP keepalive uses a 120-second idle interval, a
60-second probe interval, and three probes.

The admin owner holds a 100-share scheduling group through connection and
cross-shard scrape cleanup. Its disjoint reservation is 4 MiB per shard.
Metrics snapshots use immediate source-shard admission, bounded registry and
histogram sizes, and sequential cross-shard collection. Formatting and
aggregation use bounded working buffers; response bodies stream up to 4 MiB.
Snapshot admission charges container capacity, including per-family deque blocks,
with 1,408 KiB for metadata/function-cache state and 640 KiB for values.
Byte limits may reject a registry below its separate family/series ceilings.
Registered callbacks are trusted: each function copy must allocate at most
256 bytes, and callback temporary allocation must remain separately bounded.
Filtered families and empty series still yield so control work can progress.
Busy or oversized scrapes close without completing their body and may be
retried. Metric name, help, and aggregation query options remain available;
client-supplied label regular expressions are rejected. Scrapes support up to
1024 shards within the bounded collection workspace.
