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

// FailureCompensator is implemented by stages whose Compensate is idempotent
// and safe to call after their own Execute failed, because Execute may have
// left partial effects. The engine then compensates the failed stage first.
type FailureCompensator interface {
	Stage
	CompensateOnFailure() bool
}
