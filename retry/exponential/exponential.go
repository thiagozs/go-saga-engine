package exponential

import (
	stderrors "errors"
	"math/rand/v2"
	"time"

	"github.com/thiagozs/go-saga-engine/errors"
	"github.com/thiagozs/go-saga-engine/state"
)

type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      float64
}

func New(max int, base time.Duration) *Policy {
	return &Policy{
		MaxAttempts: max,
		BaseDelay:   base,
		MaxDelay:    30 * time.Second,
		Jitter:      0.2,
	}
}

func (p *Policy) ShouldRetry(err error, saga *state.State) bool {
	var stageErr *errors.StageError
	if !stderrors.As(err, &stageErr) || !stageErr.Retryable || p.MaxAttempts <= 1 {
		return false
	}
	return saga.Attempts[saga.CurrentStage] < p.MaxAttempts
}

func (p *Policy) NextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	delay := p.BaseDelay
	for i := 1; i < attempt && delay < p.MaxDelay; i++ {
		if delay > p.MaxDelay/2 {
			delay = p.MaxDelay
			break
		}
		delay *= 2
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	if p.Jitter <= 0 || delay <= 0 {
		return delay
	}
	spread := float64(delay) * min(p.Jitter, 1)
	factor := (rand.Float64()*2 - 1) * spread
	return max(0, delay+time.Duration(factor))
}
