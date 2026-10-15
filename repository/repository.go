package repository

import (
	"context"
	"errors"
	"time"

	"github.com/thiagozs/go-saga-engine/event"
	"github.com/thiagozs/go-saga-engine/state"
)

var (
	ErrNotFound        = errors.New("saga not found")
	ErrAlreadyExists   = errors.New("saga already exists")
	ErrVersionConflict = errors.New("saga version conflict")
	ErrLeaseHeld       = errors.New("saga lease is held by another worker")
	ErrLeaseLost       = errors.New("saga lease was lost")
	ErrEventNotFound   = errors.New("outbox event not found")
)

// Repository persists saga state, leases and outbox events. Create and Save
// must commit the state and supplied events atomically.
type Repository interface {
	Create(ctx context.Context, saga *state.State, events ...event.Envelope) error
	Get(ctx context.Context, sagaID string) (*state.State, error)

	Claim(ctx context.Context, sagaID, owner string, ttl time.Duration) (*state.State, error)
	Renew(ctx context.Context, sagaID, owner string, ttl time.Duration) error
	Release(ctx context.Context, sagaID, owner string) error

	Save(
		ctx context.Context,
		saga *state.State,
		expectedVersion uint64,
		owner string,
		events ...event.Envelope,
	) error

	PendingEvents(ctx context.Context, limit int) ([]event.Envelope, error)
	MarkEventPublished(ctx context.Context, eventID string) error
	RecoverableSagaIDs(ctx context.Context, updatedBefore time.Time, limit int) ([]string, error)
}
