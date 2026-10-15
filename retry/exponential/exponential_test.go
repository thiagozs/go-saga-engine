package exponential

import (
	"fmt"
	"testing"
	"time"

	sagaerrors "github.com/thiagozs/go-saga-engine/errors"
	"github.com/thiagozs/go-saga-engine/state"
)

func TestPolicyLimitsAttempts(t *testing.T) {
	t.Parallel()

	policy := New(3, time.Second)
	policy.Jitter = 0
	saga := &state.State{CurrentStage: "A", Attempts: map[string]int{"A": 1}}
	if !policy.ShouldRetry(sagaerrors.Retryable("temporary"), saga) {
		t.Fatal("first failed attempt should be retried")
	}
	saga.Attempts["A"] = 3
	if policy.ShouldRetry(sagaerrors.Retryable("temporary"), saga) {
		t.Fatal("maximum attempt must be terminal")
	}
	if policy.ShouldRetry(sagaerrors.Fatal("fatal"), saga) {
		t.Fatal("fatal error must not be retried")
	}
	saga.Attempts["A"] = 1
	if !policy.ShouldRetry(fmt.Errorf("wrapped: %w", sagaerrors.Retryable("temporary")), saga) {
		t.Fatal("wrapped retryable error should be retried")
	}
}

func TestPolicyAppliesExponentialDelayAndCap(t *testing.T) {
	t.Parallel()

	policy := New(5, time.Second)
	policy.Jitter = 0
	policy.MaxDelay = 3 * time.Second

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 3 * time.Second},
		{attempt: 20, want: 3 * time.Second},
	}
	for _, test := range tests {
		if got := policy.NextDelay(test.attempt); got != test.want {
			t.Fatalf("attempt %d: got %s, want %s", test.attempt, got, test.want)
		}
	}
}
