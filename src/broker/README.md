# Broker

Broker assembly, process lifecycle, and top-level service wiring belong here.
Startup registers rollback ownership before invoking each service, and shutdown
is idempotent, preserves the first failure, and attempts every cleanup in reverse
dependency order. Shutdown publishes local health as unavailable and issues
readiness and runtime-abort notifications before awaiting either acknowledgement.
Runtime owners close new task and lease admission before cancellation callbacks
run. Admin callbacks are removed before their runtime and metric targets disappear.
Each named cleanup reports its start and finish, with informational warnings after
15 seconds and errors after 120 seconds. These timers keep waiting; they do not
kill the process. An external supervisor owns eventual forced termination.

Both readiness and liveness become false during drain. Supervisors must allow
graceful shutdown rather than interpreting drain-time liveness as a request for an
immediate restart. Health, version, and metrics remain read-only while reachable.

The PID lock remains held through service cleanup and crash bookkeeping. Production
starts increment `.kwaque-crash-loop` before services start. `crash_loop_limit`
defaults to 5: a previous count at or below the limit permits another attempt,
then increments the count. A changed loaded configuration checksum, more than one
hour since the prior attempt, or a fully started broker completing mandatory
cleanup resets tracking. `crash_loop_limit: null` disables finite rejection while
retaining restart tracking; an unlimited counter saturates instead of wrapping.
Developer mode bypasses startup limiting and qualified clean shutdown still clears
an existing tracker. Failed startup,
interrupted startup, and failed cleanup retain unclean evidence. Limiter refusal
does not create a redundant crash report.

Prepared reports under `crash_reports/` contain bounded failure classification,
build version, architecture, and stack addresses. New reports use version 2;
retention also accepts version 1 reports. Preparation retains 50 existing reports before
creating the current placeholder, and limits directory scans to 4096 entries.
Fatal writes handle short/interrupted I/O and check fsync; persistence remains best
effort and kernel I/O may block. Preinitialization crashes retain native diagnostics.
Sanitizer SIGSEGV handling stays with the sanitizer; wrapped fatal signals restore
and re-raise through their prior native handlers. Clean teardown removes an unused
placeholder; recorded or torn reports remain for diagnosis.
Cleanup retains a pending directory sync if unlink succeeded but sync failed.
Retries complete that remaining step, and concurrent cleanup calls serialize.

The package-private application state is the broker's concrete production
environment seam; the public application interface remains opaque. Its process
resource registry is configured from the smallest shard-local allocator memory
observation, retaining the resource configuration's reactor headroom before one
environment is started on each shard.
