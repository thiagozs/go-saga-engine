package stage

import (
	"context"
	"time"

	"github.com/thiagozs/go-saga-engine/state"
)

type Stage interface {
	Name() string
	Execute(ctx context.Context, state *state.State) error
	Compensate(ctx context.Context, state *state.State) error
}

type TimedStage interface {
	Stage
	Timeout() time.Duration
}
