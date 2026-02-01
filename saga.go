package saga

import (
	"context"
	"log/slog"

	"github.com/thiagozs/go-saga-engine/state"
)

type Engine interface {
	Start(ctx context.Context, payload map[string]any) (*state.State, error)
	HandleNext(ctx context.Context, sagaID string)
}

type Config struct {
	Name string
	Log  *slog.Logger
}
