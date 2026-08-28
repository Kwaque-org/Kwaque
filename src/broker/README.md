# Broker

Broker assembly, process lifecycle, and top-level service wiring belong here.
Startup registers rollback ownership before invoking each service, and shutdown
is idempotent, preserves the first failure, and attempts every cleanup in reverse
dependency order.
