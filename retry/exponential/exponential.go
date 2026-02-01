package exponential

import (
	"time"

	"github.com/thiagozs/go-saga-engine/errors"
	"github.com/thiagozs/go-saga-engine/state"
)

type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
}

func New(max int, base time.Duration) *Policy {
	return &Policy{
		MaxAttempts: max,
		BaseDelay:   base,
	}
}

func (p *Policy) ShouldRetry(err error, saga *state.State) bool {
	e, ok := err.(*errors.StageError)
	if !ok {
		return false
	}
	return e.Retryable
}

func (p *Policy) NextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	return time.Duration(1<<attempt) * p.BaseDelay
}
