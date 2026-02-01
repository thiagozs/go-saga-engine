# go-saga-engine

Read in other languages: [Portugues (Brasil)](README-ptBR.md)

A robust, deterministic and testable SAGA orchestration engine written in Go.

This project provides a generic SAGA engine designed for distributed transactional workflows, with strong guarantees around:

- ordered execution (DAG)
- parallelism
- retry policies
- idempotency
- compensation
- cancellation
- observability (history / inspector)

It is domain-agnostic and can be reused for invoices, settlements, chargebacks, onboarding flows, etc.

---

## What This Engine Is (and Is Not)

### This engine IS

- A SAGA orchestrator
- A distributed transaction coordinator
- A deterministic workflow engine
- A DAG-based execution engine
- Safe for retries, restarts, and crashes
- Designed for production-grade fintech / backend systems

### This engine IS NOT

- A BPMN engine
- A job scheduler
- A cron system
- A message broker
- A domain framework

The engine does not know your business logic.  
It only guarantees execution semantics.

---

## Core Concepts

### 1. Saga

A Saga represents one distributed transaction.

Each saga has:

- a unique SagaID
- a lifecycle (PENDING → RUNNING → COMPLETED | FAILED)
- a shared mutable State
- an execution history

---

### 2. State

The state.State is the single source of truth.

```go
type State struct {
    SagaID         string
    Name           string
    Status         Status
    Payload        map[string]any

    ExecutedStages map[string]bool
    History        []HistoryEntry

    Error          *ErrorInfo
    CreatedAt      time.Time
    UpdatedAt      time.Time
}
```

- Payload is the shared contract between stages
- ExecutedStages ensures idempotency
- History enables inspection, debugging and auditing

---

### 3. Stage

A Stage is one step of the saga.

```go
type Stage interface {
    Name() string
    Execute(ctx context.Context, state *state.State) error
    Compensate(ctx context.Context, state *state.State) error
}
```

Optional timeout support:

```go
type TimedStage interface {
    Stage
    Timeout() time.Duration
}
```

Rules:

- Execute must be idempotent
- Compensate must be best-effort
- The engine never inspects the payload

---

### 4. DAG (Directed Acyclic Graph)

Execution order is defined using Nodes:

```go
type Node struct {
    Stage     stage.Stage
    DependsOn []string
    Parallel  bool
}
```

Example:

``` text
A
├── B (parallel)
└── C (parallel)
      │
      └── D
```

Dependencies are strictly enforced.

---

## Retry Semantics

Retry behavior is fully pluggable:

```go
type Policy interface {
    ShouldRetry(err error, saga *state.State) bool
    NextDelay(attempt int) time.Duration
}
```

- Only retryable errors should be retried
- Retry attempts are tracked per stage
- Retry is triggered via an event (event.Retry)

---

## Compensation Semantics

Compensation is executed when a fatal failure occurs:

- Only stages marked as ExecutedStages == true are compensated
- Compensation runs in reverse logical order
- Compensation is best-effort, not transactional

---

## Cancellation Semantics (IMPORTANT)

This engine uses context cancellation to stop execution.

### What is guaranteed

- No new stages will start after a fatal failure
- All running stages receive ctx.Done()
- Saga ends in FAILED state
- Compensation is executed
- DeadLetter event is emitted

### What is NOT guaranteed (by design)

- A parallel stage that already started may run briefly
- Cancellation is not preemptive in Go

Key rule:  
A stage may start, but it must not be marked as EXECUTED if a fatal failure occurs.

---

## Saga Inspector (History)

Every meaningful event is recorded:

``` text
EXECUTING
EXECUTED
FAILED
COMPENSATED
COMPLETED
```

Example history output:

``` text
2026-02-01T17:18:29 | identify-client        | EXECUTED
2026-02-01T17:18:30 | generate-bank-invoice | FAILED
2026-02-01T17:18:30 | identify-client        | COMPENSATED
2026-02-01T17:18:31 | SAGA                   | FAILED
```

This enables:

- debugging
- auditing
- UI inspectors
- support tooling

---

## Testing Philosophy

The engine is designed to be fully testable.

Provided tools:

- repository/memory for fast tests
- pluggable EventBus
- deterministic execution
- concurrency-safe design

Important testing rule:

Do NOT test:

``` text
"stage X should never start"
```

Instead, test:

``` text
"stage X must not be marked as EXECUTED"
```

This aligns with real-world concurrency guarantees.

---

## Event Bus Integration

The engine is event-driven.

Required events:

- event.Next
- event.Retry
- event.DeadLetter

You can plug:

- in-memory bus
- RabbitMQ
- Kafka
- NATS

The engine does not depend on a specific broker.

---

## Example Use Cases

- Invoice generation
- Monthly financial closing
- Settlement pipelines
- Onboarding workflows
- Chargeback lifecycles
- Token minting / burning flows

---

## Design Guarantees

| Feature            | Guarantee |
| ------------------ | --------- |
| Determinism        | ✅        |
| Idempotency        | ✅        |
| Retry safety       | ✅        |
| Parallel execution | ✅        |
| Cancellation       | ✅        |
| Compensation       | ✅        |
| Observability      | ✅        |
| Testability        | ✅        |

---

## Final Note

This engine is intentionally small, explicit and strict.

Complexity lives in your domain,  
not in the orchestration layer.

If you understand this README, you understand the engine.

## Licença

This project is distributed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Autor

2026, Thiago Zilli Sarmento :heart:
