# Changelog

All notable changes are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## [0.2.2] - 2026-10-22

### Added

- `examples/idempotent-start`, demonstrating `Engine.StartWithID`.

### Changed

- README and README-ptBR now document `StartWithID` and
  `stage.FailureCompensator`.

## [0.2.1] - 2026-10-22

### Added

- `Engine.StartWithID`, which uses a caller-supplied saga ID as idempotency key
  and returns the existing saga when it was already started;
- `stage.FailureCompensator`: stages that opt in are compensated first when
  their own `Execute` fails, covering partial effects.

## [0.2.0] - 2026-10-15

### Added

- deterministic DAG validation and stable topological execution;
- isolated parallel waves with deterministic payload merging;
- persisted retry attempts, bounded exponential backoff and jitter;
- optimistic concurrency, renewable leases and stalled-saga recovery;
- transactional outbox with at-least-once event delivery;
- resilience, race, fuzz, coverage, lint and vulnerability checks.

### Changed

- `engine.New` now validates configuration and returns `(*Engine, error)`;
- `HandleNext` now returns execution and infrastructure errors;
- `EventBus.Publish` now accepts a context and an `event.Envelope` and returns
  an error;
- repository implementations now provide leases, compare-and-swap saves and
  transactional outbox operations;
- Go 1.27.0 is now the minimum supported version.

### Removed

- implicit event subscriptions from the engine. Applications now dispatch
  durable outbox events to their own consumers.

[0.2.2]: https://github.com/thiagozs/go-saga-engine/releases/tag/v0.2.2
[0.2.1]: https://github.com/thiagozs/go-saga-engine/releases/tag/v0.2.1
[0.2.0]: https://github.com/thiagozs/go-saga-engine/releases/tag/v0.2.0

