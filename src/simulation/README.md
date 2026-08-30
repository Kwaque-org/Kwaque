# Simulation

Deterministic simulation and fault-injection components belong here.

The scheduler owns integer virtual time and a total `(deadline, priority,
event-id)` order. Synchronous pumps remain explicit; callers that share a
reactor can execute bounded ready/time batches and yield between them without
changing simulation order. Trace entries and their lookup structures use
chunked storage, and canonical trace artifacts encode and decode across chunks
without a large contiguous allocation.
