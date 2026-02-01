package repository

import (
	"context"

	"github.com/thiagozs/go-saga-engine/state"
)

type Repository interface {
	Create(ctx context.Context, state *state.State) error
	Update(ctx context.Context, state *state.State) error
	Get(ctx context.Context, sagaID string) (*state.State, error)
}
