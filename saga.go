package saga

import (
	"context"
	"log/slog"
	"time"

	"github.com/thiagozs/go-saga-engine/state"
)

type Engine interface {
	Start(ctx context.Context, payload map[string]any) (*state.State, error)
	HandleNext(ctx context.Context, sagaID string) error
	DispatchPending(ctx context.Context, limit int) error
	RecoverStalled(ctx context.Context, updatedBefore time.Time, limit int) (int, error)
}

type Config struct {
	Name string
	Log  *slog.Logger
}
