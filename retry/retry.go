package retry

import (
	"time"

	"github.com/thiagozs/go-saga-engine/state"
)

type Policy interface {
	ShouldRetry(err error, saga *state.State) bool
	NextDelay(attempt int) time.Duration
}
