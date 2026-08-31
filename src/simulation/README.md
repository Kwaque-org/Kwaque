# Simulation

Deterministic simulation and fault-injection components belong here.

The scheduler owns integer virtual time and a total `(deadline, priority,
event-id)` order. Synchronous pumps remain explicit; callers that share a
reactor can execute bounded ready/time batches and yield between them without
changing simulation order. Its event heap and ID index are fixed-capacity
segmented structures allocated at construction; the index uses distance-aware
Robin-Hood placement and backward-shift deletion. Accepted scheduling therefore
never discovers allocator pressure after recording trace state. Trace entries
record scheduled deadlines explicitly. Their lookup structures use chunked
storage, and canonical trace artifacts encode and decode through one linear
chunk cursor without a large contiguous allocation.

Fault schedules use explicit stable rule IDs, bounded canonical occurrence
windows, object-specific or wildcard matching, and stateless counter-based
selectors. A matched evaluation reserves its trace entry before admission and
can be committed or rolled back without allocating on the evaluation path.

The virtual filesystem state is rooted in a canonical host-independent byte
namespace. Regular files use sparse 4 KiB immutable pages with copy-on-write
volatile and durable views. File flush and directory synchronization are
independent durability boundaries; crash restoration retains only durable file
content and directory membership. Visible, durable, unsynced, and pending name
storage is bounded explicitly. Scheduled operation ownership and native file
adaptation are layered separately from this state core.

Filesystem and file-handle calls use that state through one fixed segmented
pending-slot table and the deterministic scheduler. Admission reserves count,
bytes, trace, event-ID, and open-handle capacity before committing fault or
operation coordinates. Reads and writes additionally have independent bounded
in-flight limits and deterministic operation-keyed minimum/mean latency, while
the deterministic scheduler remains the sole completion queue. Metadata and page
transitions prepare every allocation before their no-allocation commit.
Activated operations and inodes are nonmovable shard-affine objects. Native
file handles use the existing concrete runtime owner over a private Seastar file
implementation; DMA intent cancellation and caller-buffer lifetime checks
remain enforced at the selected completion.

Crash and graceful stop drain queued and parked operations in accepted
operation-ID order through pre-reserved scheduler events. Crash prepares a
bounded transition from the already-maintained durable base, schedules every
terminal, and applies it only after a `crash_applied` trace comparison. File
pages and directory names retain their
durable base plus changed-object volatile overlays, so crash discards only the
tracked deltas instead of cloning or scanning the complete filesystem. Graceful
stop uses the same typed terminal path without rolling volatile state back.
Both invalidate prior native handles, release their capacity, and leave promise
resolution under explicit scheduler pumping.

The filesystem tests keep their oracle representation independent: dense byte
vectors and directory maps generate bounded, model-legal scripts from the
storage decision stream and reconcile after every pumped result. Compound
two-file capture/replay covers delayed and torn writes, a dropped flush
completion, crashes on both sides of durability boundaries, handle invalidation,
rename synchronization, complete state comparison, and mutation of every
fault/crash/directory-sync replay boundary.
