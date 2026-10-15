package memory

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/thiagozs/go-saga-engine/event"
	"github.com/thiagozs/go-saga-engine/repository"
	"github.com/thiagozs/go-saga-engine/state"
)

type lease struct {
	owner string
	until time.Time
}

type Repository struct {
	mu sync.RWMutex

	store       map[string]*state.State
	leases      map[string]lease
	outbox      map[string]event.Envelope
	outboxOrder []string
	now         func() time.Time
}

func New() *Repository {
	return NewWithClock(time.Now)
}

func NewWithClock(now func() time.Time) *Repository {
	if now == nil {
		now = time.Now
	}
	return &Repository{
		store:  make(map[string]*state.State),
		leases: make(map[string]lease),
		outbox: make(map[string]event.Envelope),
		now:    now,
	}
}

func (r *Repository) Create(ctx context.Context, saga *state.State, events ...event.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.store[saga.SagaID]; exists {
		return repository.ErrAlreadyExists
	}
	if err := r.validateEvents(events); err != nil {
		return err
	}

	created := state.Clone(saga)
	created.Version = 1
	r.store[saga.SagaID] = created
	saga.Version = created.Version
	r.appendEvents(events)
	return nil
}

func (r *Repository) Get(ctx context.Context, sagaID string) (*state.State, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	saga, exists := r.store[sagaID]
	if !exists {
		return nil, repository.ErrNotFound
	}
	return state.Clone(saga), nil
}

func (r *Repository) Claim(
	ctx context.Context,
	sagaID string,
	owner string,
	ttl time.Duration,
) (*state.State, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if owner == "" || ttl <= 0 {
		return nil, repository.ErrLeaseLost
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	saga, exists := r.store[sagaID]
	if !exists {
		return nil, repository.ErrNotFound
	}
	now := r.now()
	current := r.leases[sagaID]
	if current.owner != "" && current.owner != owner && current.until.After(now) {
		return nil, repository.ErrLeaseHeld
	}
	r.leases[sagaID] = lease{owner: owner, until: now.Add(ttl)}
	return state.Clone(saga), nil
}

func (r *Repository) Renew(
	ctx context.Context,
	sagaID string,
	owner string,
	ttl time.Duration,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.leases[sagaID]
	if !exists || current.owner != owner || !current.until.After(r.now()) {
		return repository.ErrLeaseLost
	}
	r.leases[sagaID] = lease{owner: owner, until: r.now().Add(ttl)}
	return nil
}

func (r *Repository) Release(ctx context.Context, sagaID, owner string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.leases[sagaID]
	if !exists {
		return nil
	}
	if current.owner != owner {
		return repository.ErrLeaseLost
	}
	delete(r.leases, sagaID)
	return nil
}

func (r *Repository) Save(
	ctx context.Context,
	saga *state.State,
	expectedVersion uint64,
	owner string,
	events ...event.Envelope,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	stored, exists := r.store[saga.SagaID]
	if !exists {
		return repository.ErrNotFound
	}
	currentLease, exists := r.leases[saga.SagaID]
	if !exists || currentLease.owner != owner || !currentLease.until.After(r.now()) {
		return repository.ErrLeaseLost
	}
	if stored.Version != expectedVersion {
		return repository.ErrVersionConflict
	}
	if err := r.validateEvents(events); err != nil {
		return err
	}

	saved := state.Clone(saga)
	saved.Version = expectedVersion + 1
	r.store[saga.SagaID] = saved
	saga.Version = saved.Version
	r.appendEvents(events)
	return nil
}

func (r *Repository) PendingEvents(ctx context.Context, limit int) ([]event.Envelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if limit <= 0 || limit > len(r.outboxOrder) {
		limit = len(r.outboxOrder)
	}
	events := make([]event.Envelope, 0, limit)
	for _, eventID := range r.outboxOrder {
		if envelope, exists := r.outbox[eventID]; exists {
			events = append(events, envelope)
			if len(events) == limit {
				break
			}
		}
	}
	return events, nil
}

func (r *Repository) MarkEventPublished(ctx context.Context, eventID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.outbox[eventID]; !exists {
		return repository.ErrEventNotFound
	}
	delete(r.outbox, eventID)
	for i, pendingID := range r.outboxOrder {
		if pendingID == eventID {
			r.outboxOrder = append(r.outboxOrder[:i], r.outboxOrder[i+1:]...)
			break
		}
	}
	return nil
}

func (r *Repository) RecoverableSagaIDs(
	ctx context.Context,
	updatedBefore time.Time,
	limit int,
) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0)
	for sagaID, saga := range r.store {
		if saga.Status == state.StatusRunning && !saga.UpdatedAt.After(updatedBefore) {
			ids = append(ids, sagaID)
		}
	}
	slices.Sort(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

func (r *Repository) validateEvents(events []event.Envelope) error {
	seen := make(map[string]struct{}, len(events))
	for _, envelope := range events {
		if envelope.EventID == "" || envelope.SagaID == "" || envelope.Kind == "" {
			return errors.New("invalid outbox event")
		}
		if _, duplicate := seen[envelope.EventID]; duplicate {
			return errors.New("duplicate outbox event")
		}
		if _, duplicate := r.outbox[envelope.EventID]; duplicate {
			return errors.New("outbox event already exists")
		}
		seen[envelope.EventID] = struct{}{}
	}
	return nil
}

func (r *Repository) appendEvents(events []event.Envelope) {
	for _, envelope := range events {
		r.outbox[envelope.EventID] = envelope
		r.outboxOrder = append(r.outboxOrder, envelope.EventID)
	}
}
