#ifndef KWAQUE_BROKER_EXIT_CODE_H
#define KWAQUE_BROKER_EXIT_CODE_H

// Stable process exit statuses shared by the C launcher and the broker.
// Supervisors may key restart policy on them, so a value never changes
// meaning. Status 2 is the native option parser's invalid-command-line status.
#define KWAQUE_EXIT_SUCCESS 0
// An unexpected failure; retrying may succeed.
#define KWAQUE_EXIT_FAILURE 1
#define KWAQUE_EXIT_USAGE 2
// The configuration or runtime options cannot run, the LSB "program is not
// configured" status. Retrying without changing them fails the same way.
#define KWAQUE_EXIT_NOT_CONFIGURED 6
// Another process holds the data directory's PID-file lock.
#define KWAQUE_EXIT_DATA_DIRECTORY_IN_USE 10
// The crash-loop limit refused startup. Inspect the crash reports, then
// restart after a clean shutdown, a configuration change, or an hour.
#define KWAQUE_EXIT_CRASH_LOOP 11
// The CPU lacks instructions the broker was compiled for.
#define KWAQUE_EXIT_UNSUPPORTED_CPU 12

#endif
