package engine

import (
	"context"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/thiagozs/go-saga-engine/errors"
	"github.com/thiagozs/go-saga-engine/event"
	"github.com/thiagozs/go-saga-engine/repository/memory"
	"github.com/thiagozs/go-saga-engine/state"
)

type MemoryBus struct {
	mu       sync.Mutex
	handlers map[string][]func(context.Context, any)
}

func NewMemoryBus() *MemoryBus {
	return &MemoryBus{
		handlers: make(map[string][]func(context.Context, any)),
	}
}

func (b *MemoryBus) Publish(evt any) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := reflect.TypeOf(evt).String()
	for _, h := range b.handlers[key] {
		go h(context.Background(), evt)
	}
}

func (b *MemoryBus) Subscribe(evt any, h func(context.Context, any)) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := reflect.TypeOf(evt).String()
	b.handlers[key] = append(b.handlers[key], h)
}

type AlwaysRetryOnce struct {
	attempts map[string]int
}

func NewAlwaysRetryOnce() *AlwaysRetryOnce {
	return &AlwaysRetryOnce{
		attempts: make(map[string]int),
	}
}

func (r *AlwaysRetryOnce) ShouldRetry(err error, s *state.State) bool {
	e, ok := err.(*errors.StageError)
	if !ok || !e.Retryable {
		return false
	}

	r.attempts[s.CurrentStage]++
	return r.attempts[s.CurrentStage] == 1
}

func (r *AlwaysRetryOnce) NextDelay(attempt int) time.Duration {
	return 10 * time.Millisecond
}

type TestStage struct {
	name        string
	failOnce    bool
	executions  int
	compensated bool
	mu          sync.Mutex
}

func NewTestStage(name string, failOnce bool) *TestStage {
	return &TestStage{name: name, failOnce: failOnce}
}

func (s *TestStage) Name() string { return s.name }

func (s *TestStage) Execute(ctx context.Context, st *state.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.executions++

	if s.failOnce && s.executions == 1 {
		return errors.Retryable("temporary error")
	}
	return nil
}

func (s *TestStage) Compensate(ctx context.Context, st *state.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.compensated = true
	return nil
}

func TestSagaEngine_ParallelRetryCompensation(t *testing.T) {
	ctx := context.Background()

	repo := memory.New()
	bus := NewMemoryBus()
	retry := NewAlwaysRetryOnce()

	stageA := NewTestStage("A", false)
	stageB := NewTestStage("B", true) // falha 1x → retry
	stageC := NewTestStage("C", false)

	engine := New(
		"test-saga",
		repo,
		bus,
		retry,
		slog.Default(),
		Node{
			Stage:    stageA,
			Parallel: false,
		},
		Node{
			Stage:     stageB,
			DependsOn: []string{"A"},
			Parallel:  true,
		},
		Node{
			Stage:     stageC,
			DependsOn: []string{"A"},
			Parallel:  true,
		},
	)

	// wiring do bus
	bus.Subscribe(event.Next{}, func(ctx context.Context, evt any) {
		engine.HandleNext(ctx, evt.(event.Next).SagaID)
	})
	bus.Subscribe(event.Retry{}, func(ctx context.Context, evt any) {
		time.Sleep(20 * time.Millisecond)
		engine.HandleNext(ctx, evt.(event.Retry).SagaID)
	})

	saga, err := engine.Start(ctx, map[string]any{
		"foo": "bar",
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	final, _ := repo.Get(ctx, saga.SagaID)

	// ✅ assertions
	if final.Status != state.StatusCompleted {
		t.Fatalf("expected COMPLETED, got %s", final.Status)
	}

	if stageB.executions != 2 {
		t.Fatalf("expected stage B retry once, got %d", stageB.executions)
	}

	if len(final.History) == 0 {
		t.Fatal("expected saga history")
	}

	if !final.ExecutedStages["A"] ||
		!final.ExecutedStages["B"] ||
		!final.ExecutedStages["C"] {
		t.Fatal("not all stages executed")
	}
}

type NeverRetry struct{}

func (n *NeverRetry) ShouldRetry(err error, s *state.State) bool {
	return false
}

func (n *NeverRetry) NextDelay(attempt int) time.Duration {
	return 0
}

func TestSagaEngine_FatalFailureCancelsParallel(t *testing.T) {
	ctx := context.Background()

	repo := memory.New()
	bus := NewMemoryBus()

	// nunca faz retry
	retry := &NeverRetry{}

	stageA := NewTestStage("A", false)
	stageB := NewTestStage("B", true)  // falha SEMPRE
	stageC := NewTestStage("C", false) // paralelo

	engine := New(
		"test-saga",
		repo,
		bus,
		retry,
		slog.Default(),
		Node{
			Stage: stageA,
		},
		Node{
			Stage:     stageB,
			DependsOn: []string{"A"},
			Parallel:  true,
		},
		Node{
			Stage:     stageC,
			DependsOn: []string{"A"},
			Parallel:  true,
		},
	)

	// wiring do bus
	bus.Subscribe(event.Next{}, func(ctx context.Context, evt any) {
		engine.HandleNext(ctx, evt.(event.Next).SagaID)
	})

	saga, err := engine.Start(ctx, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	final, _ := repo.Get(ctx, saga.SagaID)

	// Saga deve falhar
	if final.Status != state.StatusFailed {
		t.Fatalf("expected FAILED, got %s", final.Status)
	}

	// Stage B tentou executar (e falhou)
	if stageB.executions == 0 {
		t.Fatal("expected stage B to be executed at least once")
	}

	// Stage C pode até ter iniciado,
	// mas NÃO pode ser considerado EXECUTADO com sucesso
	if final.ExecutedStages["C"] {
		t.Fatal("stage C must not be marked as executed after fatal failure")
	}

	// compensação deve ter ocorrido para stages concluídos
	if !stageA.compensated {
		t.Fatal("expected compensation for stage A")
	}

	// sanity check: histórico tem falha registrada
	foundFailure := false
	for _, h := range final.History {
		if h.Stage == "B" && h.Status == "FAILED" {
			foundFailure = true
			break
		}
	}
	if !foundFailure {
		t.Fatal("expected failure entry for stage B in history")
	}
}
