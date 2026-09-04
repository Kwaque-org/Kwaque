# Simulation

Deterministic simulation and fault-injection components belong here.

The one-shard test environment composes bounded scheduler and event artifacts,
virtual clocks and timers, deterministic random streams, typed fault rules,
fake file/network/DNS adapters, resource admission, and owner-local metrics.
Its aggregate configuration is validated before publication, and its explicit
lifecycle drains pending callbacks, component/resource leases, metrics, and
native scheduling resources in dependency order.

Environment lifecycle events use capacity reserved during construction, so
ordinary captured events cannot consume their terminal slots. Pending virtual
wall adjustments are canceled during stop rather than executing after component
shutdown. Only one simulation environment may own the shard-local virtual clock
and process resource registry at a time.
