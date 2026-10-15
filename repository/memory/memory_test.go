package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thiagozs/go-saga-engine/event"
	"github.com/thiagozs/go-saga-engine/repository"
	"github.com/thiagozs/go-saga-engine/state"
)

func TestRepositoryReturnsDeepCopies(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := New()
	original := &state.State{
		SagaID:          "saga-1",
		Payload:         map[string]any{"nested": map[string]any{"value": "original"}},
		ExecutedStages:  map[string]bool{"A": true},
		CompletedStages: []string{"A"},
		Attempts:        map[string]int{"A": 1},
		History:         []state.HistoryEntry{{Stage: "A"}},
		Retry:           &state.RetryInfo{Stage: "A", Attempt: 1},
	}
	if err := repo.Create(ctx, original); err != nil {
		t.Fatal(err)
	}

	original.Payload["nested"].(map[string]any)["value"] = "changed outside"
	original.ExecutedStages["B"] = true
	original.CompletedStages[0] = "changed"
	original.Attempts["A"] = 9
	original.History[0].Stage = "changed"
	original.Retry.Stage = "changed"

	stored, err := repo.Get(ctx, original.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.Payload["nested"].(map[string]any)["value"]; got != "original" {
		t.Fatalf("payload was aliased: %v", got)
	}
	if stored.ExecutedStages["B"] {
		t.Fatal("executed stages map was aliased")
	}
	if stored.CompletedStages[0] != "A" || stored.History[0].Stage != "A" {
		t.Fatal("state slices were aliased")
	}
	if stored.Attempts["A"] != 1 || stored.Retry.Stage != "A" {
		t.Fatal("retry state was aliased")
	}
}

func TestRepositoryEnforcesLeaseVersionAndAtomicOutbox(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := New()
	saga := &state.State{SagaID: "saga-1", ExecutedStages: make(map[string]bool)}
	started := event.Envelope{
		EventID: "event-1", Kind: event.KindNext, SagaID: saga.SagaID, CreatedAt: time.Now(),
	}
	if err := repo.Create(ctx, saga, started); err != nil {
		t.Fatal(err)
	}
	if saga.Version != 1 {
		t.Fatalf("expected initial version 1, got %d", saga.Version)
	}

	claimed, err := repo.Claim(ctx, saga.SagaID, "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Claim(ctx, saga.SagaID, "worker-2", time.Minute); !errors.Is(err, repository.ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld, got %v", err)
	}

	completed := event.Envelope{
		EventID: "event-2", Kind: event.KindDeadLetter, SagaID: saga.SagaID, CreatedAt: time.Now(),
	}
	if err := repo.Save(ctx, claimed, 99, "worker-1", completed); !errors.Is(err, repository.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict, got %v", err)
	}
	pending, err := repo.PendingEvents(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("failed save must not append outbox events: %+v", pending)
	}

	claimed.Status = state.StatusRunning
	if err := repo.Save(ctx, claimed, 1, "worker-1", completed); err != nil {
		t.Fatal(err)
	}
	if claimed.Version != 2 {
		t.Fatalf("expected version 2, got %d", claimed.Version)
	}
	pending, err = repo.PendingEvents(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected atomic outbox append, got %+v", pending)
	}

	if err := repo.Release(ctx, saga.SagaID, "worker-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Claim(ctx, saga.SagaID, "worker-2", time.Minute); err != nil {
		t.Fatalf("lease was not released: %v", err)
	}
	if err := repo.Save(ctx, claimed, 2, "worker-1"); !errors.Is(err, repository.ErrLeaseLost) {
		t.Fatalf("old worker should not save after losing lease: %v", err)
	}
}
