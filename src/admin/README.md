# Admin

The loopback-friendly administrative HTTP service exposes:

- `GET /v1/health/live` for process liveness.
- `GET /v1/health/ready` for completed-startup readiness.
- `GET /v1/version` for version, revision, and build-mode metadata.
- `GET /metrics` for Prometheus-formatted runtime metrics.

The metrics route reads the native default registry and currently publishes the
fixed broker, task, timer, file, network, and DNS families. Shard is aggregated
for runtime totals; no path, host, object, or other dynamic product label is
exported. Resource-manager, bounded-queue, and simulation owners are not part of
the current broker composition.

Readiness is cleared before shutdown stops accepting administrative
connections. Error responses use a stable JSON envelope containing `code`,
`message`, and `correlation_id` fields. Lifecycle state, request counters, route
handlers, and metric ownership are shard-local; process-level metrics use native
metric aggregation rather than shared cross-core counters.
