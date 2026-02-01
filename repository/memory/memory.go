package memory

import (
	"context"
	"sync"

	"github.com/thiagozs/go-saga-engine/state"
)

type Repository struct {
	mu    sync.RWMutex
	store map[string]*state.State
}

func New() *Repository {
	return &Repository{
		store: make(map[string]*state.State),
	}
}

func (r *Repository) Create(ctx context.Context, s *state.State) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[s.SagaID] = clone(s)
	return nil
}

func (r *Repository) Update(ctx context.Context, s *state.State) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[s.SagaID] = clone(s)
	return nil
}

func (r *Repository) Get(ctx context.Context, sagaID string) (*state.State, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	s, ok := r.store[sagaID]
	if !ok {
		return nil, nil
	}
	return clone(s), nil
}

func clone(s *state.State) *state.State {
	c := *s
	return &c
}
