# go-saga-engine

Read in other languages: [Português (Brasil)](README-ptBR.md)

A compact, domain-agnostic SAGA orchestration engine written in Go.

> **Development status:** blocks 1 through 4 are complete: deterministic DAG
> execution, isolated parallel waves, persisted retries, optimistic concurrency,
> renewable leases, a transactional outbox, resilience tests and hardened CI.
> The latest release is `v0.2.1`. Follow the
> [update roadmap](ROADMAP.md) and [changelog](CHANGELOG.md).

The engine coordinates distributed transactional workflows without knowing
their business rules. Typical use cases include invoices, settlements,
chargebacks and onboarding flows.

Go 1.27.0 is the minimum supported version. CI continuously verifies the latest
Go 1.27 patch release.

## What the engine provides

- validated DAG-based execution;
- stable topological ordering;
- isolated execution of parallel stages;
- deterministic payload merge;
- context cancellation and optional stage timeouts;
- reverse-order compensation, optionally including the stage that failed;
- idempotent saga start through a caller-supplied saga ID;
- persisted execution history;
- pluggable repository, retry policy and event bus contracts.

It is an orchestrator, not a BPMN engine, scheduler, cron system, message
broker or domain framework.

## Current guarantees and limitations

| Capability | Status |
| ---------- | ------ |
| DAG validation and stable topological order | Implemented |
| Isolated parallel execution and deterministic merge | Implemented |
| Reverse-order compensation after wave shutdown | Implemented |
| Deep-copy in-memory repository | Implemented |
| Durable retry limits, backoff, jitter and maximum delay | Implemented |
| Renewable lease and optimistic concurrency | Implemented |
| Transactional outbox and stalled-saga recovery | Implemented |
| Resilience suite and CI race gate | Implemented |

Event delivery is at-least-once. Consumers must deduplicate by `EventID`, and
stages remain responsible for making their external effects idempotent.

## Core concepts

### Saga state

`state.State` is the persisted source of truth:

```go
type State struct {
    SagaID          string
    Name            string
    Status          Status
    CurrentStage    string
    Version         uint64
    Payload         map[string]any

    ExecutedStages  map[string]bool
    CompletedStages []string
    Attempts        map[string]int
    History         []HistoryEntry

    Retry           *RetryInfo
    Error           *ErrorInfo
    CreatedAt       time.Time
    UpdatedAt       time.Time
}
```

- `Payload` is the business data exchanged by stages.
- `ExecutedStages` tracks stages confirmed by the engine.
- `CompletedStages` defines deterministic reverse compensation order.
- `Attempts` and `Retry` persist retry scheduling per stage.
- `Version` is used by repository compare-and-swap saves.
- `History` records execution, failure and compensation events.

### Stages

```go
type Stage interface {
    Name() string
    Execute(ctx context.Context, saga *state.State) error
    Compensate(ctx context.Context, saga *state.State) error
}

type TimedStage interface {
    Stage
    Timeout() time.Duration
}

type FailureCompensator interface {
    Stage
    CompensateOnFailure() bool
}
```

`Execute` and `Compensate` must be idempotent with respect to their external
effects. In a parallel wave, every stage receives an isolated state copy. The
engine merges only payload changes back into its state; changes to lifecycle
metadata are ignored.

Payloads should be acyclic and use maps, slices, pointers or immutable values.
Opaque values with private mutable state are copied by value and must be treated
as immutable by parallel stages.

Parallel stages may change different payload keys or write the same value to a
shared key. Different writes to the same key produce `ErrPayloadConflict` and
fail the saga rather than creating a nondeterministic result.

### DAG nodes

```go
type Node struct {
    Stage     stage.Stage
    DependsOn []string
    Parallel  bool
}
```

Construction rejects:

- an empty graph;
- nil stages or empty stage names;
- duplicate stage names;
- unknown or repeated dependencies;
- dependency cycles.

Ready nodes follow a stable topological order derived from declaration order.
A non-parallel node runs alone. Consecutive ready nodes marked `Parallel` run
as one wave.

## Creating and running an engine

The constructor validates configuration and the complete DAG:

```go
eng, err := engine.New(
    "invoice-saga",
    repo,
    bus,
    retryPolicy,
    logger,
    nodes...,
)
if err != nil {
    return err
}

saga, err := eng.Start(ctx, map[string]any{"invoice_id": "INV-2026-01"})
if err != nil {
    return err
}

if err := eng.HandleNext(ctx, saga.SagaID); err != nil {
    return err
}
```

Use `StartWithID` to make starts idempotent. The saga ID acts as the
idempotency key: starting the same ID again returns the existing saga without
creating a new one or changing it.

```go
saga, err := eng.StartWithID(ctx, "onboarding-customer-42", payload)
```

`New` now returns `(*engine.Engine, error)` and `HandleNext` returns operational
or terminal execution errors. Existing users should follow the
[`v0.2.0` migration guide](MIGRATION.md).

## Execution semantics

For every execution wave, the engine:

1. finds ready nodes in stable topological order;
2. records `EXECUTING` in that same order;
3. gives each stage an isolated state copy;
4. cancels the wave context when a stage fails;
5. waits for every started stage to return;
6. commits successful stages and merges their payload changes deterministically;
7. persists the resulting state before continuing.

A parallel stage that finishes successfully before a peer fails is confirmed
and included in compensation. This avoids leaving a successful external effect
untracked.

## Durable retry semantics

```go
type Policy interface {
    ShouldRetry(err error, saga *state.State) bool
    NextDelay(attempt int) time.Duration
}
```

On failure, the engine increments the stage attempt before consulting the
policy. The exponential policy enforces `MaxAttempts`, doubles from `BaseDelay`,
caps at `MaxDelay` and optionally applies jitter. The resulting `RetryInfo` and
retry outbox event are committed atomically before publication.

Retry events carry `Attempt` and `NotBefore`. Consumers must not invoke the
engine before `NotBefore`; an early call returns `ErrRetryNotReady` without
executing a stage.

## Concurrency and crash recovery

`HandleNext` claims a renewable lease for its `SagaID`. Another worker receives
`repository.ErrLeaseHeld` and cannot start stages for that saga. Every save uses
the state `Version` as a compare-and-swap condition, producing
`repository.ErrVersionConflict` for stale state.

`RecoverStalled(ctx, updatedBefore, limit)` creates durable `Next` events for
`RUNNING` sagas whose lease expired and whose state was not updated after the
cutoff. Stages still need idempotent external effects because a process can
crash after an external side effect and before saving its result.

Applications should call `RecoverStalled` periodically from a single scheduled
maintenance job; it is intentionally not an internal goroutine.

## Compensation semantics

On a terminal failure:

- all stages in the current wave are allowed to stop first;
- every confirmed stage is compensated in reverse `CompletedStages` order;
- a stage that implements `stage.FailureCompensator` and returns `true` from
  `CompensateOnFailure()` is compensated first when its own `Execute` fails,
  because it may have left partial effects (its `Compensate` must be idempotent
  and safe to call on a stage that never completed);
- compensation remains best-effort;
- a successful compensation records `COMPENSATED`;
- a failed compensation records `COMPENSATION_FAILED` and its error is returned;
- the saga ends as `FAILED` and emits `event.KindDeadLetter`.

## Cancellation semantics

Go cancellation is cooperative, not preemptive. Once one stage fails, every
other running stage receives `ctx.Done()`, but it may still finish. No new wave
starts after the failure.

Stages should regularly observe the context and avoid starting irreversible
work after it is canceled.

## Event bus integration

Events are persisted as `event.Envelope` values containing `EventID`, `Kind`,
`SagaID`, stage, attempt and scheduling timestamps. The engine emits:

- `event.KindNext` when a saga is created;
- `event.KindRetry` when a failure is accepted by the retry policy;
- `event.KindDeadLetter` after a terminal failure is persisted.

```go
type EventBus interface {
    Publish(ctx context.Context, envelope event.Envelope) error
}
```

State changes and new envelopes are committed atomically by the repository.
`DispatchPending` publishes the outbox using a context-aware `EventBus` and only
then marks events as published. A crash between those operations can duplicate
delivery, so consumers must deduplicate by `EventID`.

RabbitMQ, Kafka, NATS or an in-memory implementation can provide the small
`EventBus` contract.

## Examples

- [`examples/simple`](examples/simple) demonstrates the basic engine API and a
  retryable stage.
- [`examples/idempotent-start`](examples/idempotent-start) shows `StartWithID`
  preventing a duplicated request from creating a second saga.
- [`examples/order-fulfillment`](examples/order-fulfillment) models order
  validation, parallel fraud and inventory checks, payment, shipping retry,
  customer notification and reverse compensation after payment rejection.

## Verification

The current block is verified with:

```text
go test ./...
go test -race -count=20 ./...
go vet ./...
golangci-lint run ./...
govulncheck ./...
```

Tests cover invalid graphs, stable ordering, isolated payload merging,
conflicting writes, reverse compensation, repository copies, retry limits and
timing, outbox recovery, leases, version conflicts, timeout, cancellation,
persisted-wave restarts and stalled-saga scheduling. CI repeats the suite under
the race detector, and the DAG validator also has a fuzz target.

## Versioning

The project follows Semantic Versioning. Before `v1.0.0`, incompatible public
API changes increment the minor version. Starting with `v1.0.0`, incompatible
changes increment the major version; compatible features increment the minor
version and fixes increment the patch version. Releases and upgrade notes are
recorded in the [changelog](CHANGELOG.md).

## License

Distributed under the MIT License. See [LICENSE](LICENSE).

## Author

2026, Thiago Zilli Sarmento :heart:
