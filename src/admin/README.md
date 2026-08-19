# Admin

The loopback-friendly administrative HTTP service exposes:

- `GET /v1/health/live` for process liveness.
- `GET /v1/health/ready` for completed-startup readiness.
- `GET /v1/version` for version, revision, and build-mode metadata.
- `GET /metrics` for Prometheus-formatted runtime metrics.

Readiness is cleared before shutdown stops accepting administrative
connections. Error responses use a stable JSON envelope containing `code`,
`message`, and `correlation_id` fields.
