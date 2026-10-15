package event

import "time"

type Kind string

const (
	KindNext       Kind = "saga.next"
	KindRetry      Kind = "saga.retry"
	KindDeadLetter Kind = "saga.dead-letter"
)

// Envelope is the durable event representation stored in the repository
// outbox. Consumers must deduplicate deliveries by EventID.
type Envelope struct {
	EventID   string
	Kind      Kind
	SagaID    string
	Stage     string
	Attempt   int
	NotBefore time.Time
	CreatedAt time.Time
}
