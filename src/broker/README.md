# Broker

Broker assembly, process lifecycle, and top-level service wiring belong here.
Startup registers rollback ownership before invoking each service, and shutdown
is idempotent, preserves the first failure, and attempts every cleanup in reverse
dependency order. Shutdown drains and stops the administrative endpoint before
requesting abort on every shard environment, so no externally admitted work can
race owner teardown.

The package-private application state is the broker's concrete production
environment seam; the public application interface remains opaque. Its process
resource registry is configured from the smallest shard-local allocator memory
observation, retaining the resource configuration's reactor headroom before one
environment is started on each shard.
