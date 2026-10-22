package engine

import (
	"context"
	stderrors "errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/thiagozs/go-saga-engine/errors"
	"github.com/thiagozs/go-saga-engine/event"
	"github.com/thiagozs/go-saga-engine/repository"
	"github.com/thiagozs/go-saga-engine/repository/memory"
	"github.com/thiagozs/go-saga-engine/retry/exponential"
	"github.com/thiagozs/go-saga-engine/state"
)

type FuncStage struct {
	name       string
	execute    func(context.Context, *state.State) error
	compensate func(context.Context, *state.State) error
}

type TimedFuncStage struct {
	*FuncStage
	timeout time.Duration
}

func (s *TimedFuncStage) Timeout() time.Duration { return s.timeout }

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

func (s *FuncStage) Name() string { return s.name }

func (s *FuncStage) Execute(ctx context.Context, saga *state.State) error {
	if s.execute == nil {
		return nil
	}
	return s.execute(ctx, saga)
}

func (s *FuncStage) Compensate(ctx context.Context, saga *state.State) error {
	if s.compensate == nil {
		return nil
	}
	return s.compensate(ctx, saga)
}

type MemoryBus struct {
	mu         sync.Mutex
	events     []event.Envelope
	publishErr error
}

type faultRepository struct {
	repository.Repository

	mu        sync.Mutex
	saveCalls int
	saveErrAt map[int]error
	afterSave func(int)
}

func (r *faultRepository) Save(
	ctx context.Context,
	saga *state.State,
	expectedVersion uint64,
	owner string,
	events ...event.Envelope,
) error {
	r.mu.Lock()
	r.saveCalls++
	call := r.saveCalls
	failure := r.saveErrAt[call]
	afterSave := r.afterSave
	r.mu.Unlock()
	if failure != nil {
		return failure
	}
	if err := r.Repository.Save(ctx, saga, expectedVersion, owner, events...); err != nil {
		return err
	}
	if afterSave != nil {
		afterSave(call)
	}
	return nil
}

func NewMemoryBus() *MemoryBus {
	return &MemoryBus{}
}

func (b *MemoryBus) Publish(_ context.Context, envelope event.Envelope) error {
	b.mu.Lock()
	if b.publishErr != nil {
		err := b.publishErr
		b.mu.Unlock()
		return err
	}
	b.events = append(b.events, envelope)
	b.mu.Unlock()
	return nil
}

func (b *MemoryBus) setPublishError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.publishErr = err
}

func (b *MemoryBus) publishedEvents() []event.Envelope {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.events)
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

	clock := newFakeClock()
	repo := memory.NewWithClock(clock.Now)
	bus := NewMemoryBus()
	retry := NewAlwaysRetryOnce()

	stageA := NewTestStage("A", false)
	stageB := NewTestStage("B", true) // falha 1x → retry
	stageC := NewTestStage("C", false)

	engine, err := NewWithConfig(
		Config{
			Name: "test-saga", Repository: repo, EventBus: bus,
			RetryPolicy: retry, Logger: slog.Default(), Clock: clock,
		},
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
	if err != nil {
		t.Fatal(err)
	}

	saga, err := engine.Start(ctx, map[string]any{
		"foo": "bar",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := engine.HandleNext(ctx, saga.SagaID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Millisecond)
	if err := engine.HandleNext(ctx, saga.SagaID); err != nil {
		t.Fatal(err)
	}

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

	engine, err := New(
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
	if err != nil {
		t.Fatal(err)
	}

	saga, err := engine.Start(ctx, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	if err := engine.HandleNext(ctx, saga.SagaID); err == nil {
		t.Fatal("expected fatal stage error")
	}

	final, _ := repo.Get(ctx, saga.SagaID)

	// Saga deve falhar
	if final.Status != state.StatusFailed {
		t.Fatalf("expected FAILED, got %s", final.Status)
	}

	// Stage B tentou executar (e falhou)
	if stageB.executions == 0 {
		t.Fatal("expected stage B to be executed at least once")
	}

	// Um stage paralelo que concluiu antes do cancelamento é confirmado e
	// imediatamente compensado junto com os stages anteriores.
	if !final.ExecutedStages["C"] || !stageC.compensated {
		t.Fatal("expected successful parallel stage C to be committed and compensated")
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

func TestNewRejectsInvalidDAG(t *testing.T) {
	t.Parallel()

	repo := memory.New()
	bus := NewMemoryBus()
	stageA := &FuncStage{name: "A"}
	stageB := &FuncStage{name: "B"}
	var nilStage *FuncStage

	tests := map[string][]Node{
		"empty":              nil,
		"nil stage":          {{Stage: nilStage}},
		"empty stage name":   {{Stage: &FuncStage{}}},
		"duplicate stage":    {{Stage: stageA}, {Stage: stageA}},
		"unknown dependency": {{Stage: stageA, DependsOn: []string{"missing"}}},
		"duplicate dependency": {
			{Stage: stageA},
			{Stage: stageB, DependsOn: []string{"A", "A"}},
		},
		"cycle": {
			{Stage: stageA, DependsOn: []string{"B"}},
			{Stage: stageB, DependsOn: []string{"A"}},
		},
	}

	for name, nodes := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := New("test", repo, bus, nil, nil, nodes...)
			if !stderrors.Is(err, ErrInvalidDAG) {
				t.Fatalf("expected ErrInvalidDAG, got %v", err)
			}
		})
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	repo := memory.New()
	bus := NewMemoryBus()
	node := Node{Stage: &FuncStage{name: "A"}}

	tests := []struct {
		name       string
		engineName string
		repo       repository.Repository
		bus        EventBus
	}{
		{name: "empty name", repo: repo, bus: bus},
		{name: "nil repository", engineName: "test", bus: bus},
		{name: "nil event bus", engineName: "test", repo: repo},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(test.engineName, test.repo, test.bus, nil, nil, node)
			if !stderrors.Is(err, ErrInvalidConfig) {
				t.Fatalf("expected ErrInvalidConfig, got %v", err)
			}
		})
	}
}

func TestCompensationWaitsForFailedWaveToStop(t *testing.T) {
	t.Parallel()

	parallelStarted := make(chan struct{})
	parallelStopped := make(chan struct{})
	stageA := &FuncStage{
		name: "A",
		compensate: func(_ context.Context, _ *state.State) error {
			select {
			case <-parallelStopped:
				return nil
			default:
				return stderrors.New("compensation started while wave was running")
			}
		},
	}
	stageB := &FuncStage{name: "B", execute: func(_ context.Context, _ *state.State) error {
		<-parallelStarted
		return errors.Fatal("fatal")
	}}
	stageC := &FuncStage{name: "C", execute: func(ctx context.Context, _ *state.State) error {
		close(parallelStarted)
		<-ctx.Done()
		close(parallelStopped)
		return ctx.Err()
	}}

	repo := memory.New()
	engine, err := New(
		"test", repo, NewMemoryBus(), nil, nil,
		Node{Stage: stageA},
		Node{Stage: stageB, DependsOn: []string{"A"}, Parallel: true},
		Node{Stage: stageC, DependsOn: []string{"A"}, Parallel: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleNext(context.Background(), saga.SagaID); err == nil {
		t.Fatal("expected terminal stage error")
	}

	final, err := repo.Get(context.Background(), saga.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range final.History {
		if entry.Status == "COMPENSATION_FAILED" {
			t.Fatalf("compensation overlapped wave: %s", entry.Message)
		}
	}
}

func TestNewBuildsStableTopologicalOrder(t *testing.T) {
	t.Parallel()

	stageA := &FuncStage{name: "A"}
	stageB := &FuncStage{name: "B"}
	stageC := &FuncStage{name: "C"}
	stageD := &FuncStage{name: "D"}
	engine, err := New(
		"test",
		memory.New(),
		NewMemoryBus(),
		nil,
		nil,
		Node{Stage: stageD, DependsOn: []string{"B", "C"}},
		Node{Stage: stageB, DependsOn: []string{"A"}},
		Node{Stage: stageA},
		Node{Stage: stageC, DependsOn: []string{"A"}},
	)
	if err != nil {
		t.Fatal(err)
	}

	got := make([]string, 0, len(engine.nodes))
	for _, node := range engine.nodes {
		got = append(got, node.Stage.Name())
	}
	want := []string{"A", "B", "C", "D"}
	if !slices.Equal(got, want) {
		t.Fatalf("unexpected topological order: got %v, want %v", got, want)
	}
}

func TestParallelStagesMergePayloadDeterministically(t *testing.T) {
	t.Parallel()

	stageA := &FuncStage{name: "A", execute: func(_ context.Context, saga *state.State) error {
		saga.Payload["a"] = map[string]any{"value": 1}
		return nil
	}}
	stageB := &FuncStage{name: "B", execute: func(_ context.Context, saga *state.State) error {
		saga.Payload["b"] = []any{"ok"}
		return nil
	}}
	repo := memory.New()
	engine, err := New(
		"test", repo, NewMemoryBus(), nil, nil,
		Node{Stage: stageA, Parallel: true},
		Node{Stage: stageB, Parallel: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(context.Background(), map[string]any{"input": "value"})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleNext(context.Background(), saga.SagaID); err != nil {
		t.Fatal(err)
	}

	final, err := repo.Get(context.Background(), saga.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != state.StatusCompleted {
		t.Fatalf("expected completed saga, got %s", final.Status)
	}
	if !slices.Equal(final.CompletedStages, []string{"A", "B"}) {
		t.Fatalf("unexpected completion order: %v", final.CompletedStages)
	}
	if _, ok := final.Payload["a"]; !ok {
		t.Fatal("stage A payload was not merged")
	}
	if _, ok := final.Payload["b"]; !ok {
		t.Fatal("stage B payload was not merged")
	}
}

func TestParallelPayloadConflictFailsAndCompensatesInReverseOrder(t *testing.T) {
	t.Parallel()

	var (
		mu                sync.Mutex
		compensationOrder []string
	)
	newStage := func(name, value string) *FuncStage {
		return &FuncStage{
			name: name,
			execute: func(_ context.Context, saga *state.State) error {
				saga.Payload["shared"] = value
				return nil
			},
			compensate: func(_ context.Context, _ *state.State) error {
				mu.Lock()
				defer mu.Unlock()
				compensationOrder = append(compensationOrder, name)
				return nil
			},
		}
	}

	repo := memory.New()
	engine, err := New(
		"test", repo, NewMemoryBus(), nil, nil,
		Node{Stage: newStage("A", "a"), Parallel: true},
		Node{Stage: newStage("B", "b"), Parallel: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(context.Background(), map[string]any{"shared": "initial"})
	if err != nil {
		t.Fatal(err)
	}
	err = engine.HandleNext(context.Background(), saga.SagaID)
	if !stderrors.Is(err, ErrPayloadConflict) {
		t.Fatalf("expected payload conflict, got %v", err)
	}

	final, err := repo.Get(context.Background(), saga.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != state.StatusFailed {
		t.Fatalf("expected failed saga, got %s", final.Status)
	}
	if !slices.Equal(compensationOrder, []string{"B", "A"}) {
		t.Fatalf("unexpected compensation order: %v", compensationOrder)
	}
	if final.Payload["shared"] != "initial" {
		t.Fatalf("conflicting payload must not be committed: %v", final.Payload)
	}
}

func TestRetryIsPersistedLimitedAndNotImmediatelyExecutable(t *testing.T) {
	t.Parallel()

	executions := 0
	stageA := &FuncStage{name: "A", execute: func(_ context.Context, _ *state.State) error {
		executions++
		if executions == 1 {
			return errors.Retryable("temporary")
		}
		return nil
	}}
	policy := exponential.New(2, time.Hour)
	policy.Jitter = 0
	clock := newFakeClock()
	repo := memory.NewWithClock(clock.Now)
	bus := NewMemoryBus()
	engine, err := NewWithConfig(
		Config{Name: "test", Repository: repo, EventBus: bus, RetryPolicy: policy, Clock: clock},
		Node{Stage: stageA},
	)
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleNext(context.Background(), saga.SagaID); err != nil {
		t.Fatal(err)
	}

	waiting, err := repo.Get(context.Background(), saga.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	if waiting.Status != state.StatusWaitingRetry || waiting.Attempts["A"] != 1 {
		t.Fatalf("retry state was not persisted: %+v", waiting)
	}
	if waiting.Retry == nil || waiting.Retry.Stage != "A" || waiting.Retry.Attempt != 1 {
		t.Fatalf("retry metadata was not persisted: %+v", waiting.Retry)
	}
	if err := engine.HandleNext(context.Background(), saga.SagaID); !stderrors.Is(err, ErrRetryNotReady) {
		t.Fatalf("expected ErrRetryNotReady, got %v", err)
	}
	events := bus.publishedEvents()
	if len(events) != 2 || events[1].Kind != event.KindRetry || events[1].Attempt != 1 {
		t.Fatalf("unexpected published events: %+v", events)
	}
	clock.Advance(time.Hour)
	restarted, err := NewWithConfig(
		Config{Name: "test", Repository: repo, EventBus: bus, RetryPolicy: policy, Clock: clock},
		Node{Stage: stageA},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.HandleNext(context.Background(), saga.SagaID); err != nil {
		t.Fatal(err)
	}
	completed, err := repo.Get(context.Background(), saga.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != state.StatusCompleted || executions != 2 {
		t.Fatalf("retry did not run at NotBefore: status=%s executions=%d", completed.Status, executions)
	}
}

func TestRetryStopsAtMaximumAttempts(t *testing.T) {
	t.Parallel()

	stageA := &FuncStage{name: "A", execute: func(_ context.Context, _ *state.State) error {
		return errors.Retryable("temporary")
	}}
	policy := exponential.New(2, 0)
	policy.Jitter = 0
	repo := memory.New()
	engine, err := New("test", repo, NewMemoryBus(), policy, nil, Node{Stage: stageA})
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleNext(context.Background(), saga.SagaID); err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleNext(context.Background(), saga.SagaID); err == nil {
		t.Fatal("expected the second failed attempt to become terminal")
	}

	final, err := repo.Get(context.Background(), saga.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != state.StatusFailed || final.Attempts["A"] != 2 {
		t.Fatalf("unexpected terminal retry state: %+v", final)
	}
}

func TestConcurrentHandleNextIsRejectedByLease(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	stageA := &FuncStage{name: "A", execute: func(_ context.Context, _ *state.State) error {
		close(started)
		<-release
		return nil
	}}
	repo := memory.New()
	engine, err := New("test", repo, NewMemoryBus(), nil, nil, Node{Stage: stageA})
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- engine.HandleNext(context.Background(), saga.SagaID) }()
	<-started
	if err := engine.HandleNext(context.Background(), saga.SagaID); !stderrors.Is(err, repository.ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld, got %v", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestOutboxSurvivesEventBusFailure(t *testing.T) {
	t.Parallel()

	repo := memory.New()
	bus := NewMemoryBus()
	bus.setPublishError(stderrors.New("broker unavailable"))
	engine, err := New(
		"test", repo, bus, nil, nil,
		Node{Stage: &FuncStage{name: "A"}},
	)
	if err != nil {
		t.Fatal(err)
	}

	saga, err := engine.Start(context.Background(), nil)
	if saga == nil || err == nil {
		t.Fatalf("expected persisted saga and publication error, got saga=%v err=%v", saga, err)
	}
	pending, err := repo.PendingEvents(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Kind != event.KindNext {
		t.Fatalf("start event was not retained in outbox: %+v", pending)
	}

	bus.setPublishError(nil)
	if err := engine.DispatchPending(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	pending, err = repo.PendingEvents(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("published outbox was not cleared: %+v", pending)
	}
}

func TestRecoverStalledSchedulesDurableNextEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newFakeClock()
	repo := memory.NewWithClock(clock.Now)
	bus := NewMemoryBus()
	engine, err := NewWithConfig(
		Config{Name: "test", Repository: repo, EventBus: bus, Clock: clock},
		Node{Stage: &FuncStage{name: "A"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	stalled, err := repo.Claim(ctx, saga.SagaID, "setup", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	stalled.Status = state.StatusRunning
	stalled.UpdatedAt = clock.Now().Add(-time.Hour)
	if err := repo.Save(ctx, stalled, stalled.Version, "setup"); err != nil {
		t.Fatal(err)
	}
	if err := repo.Release(ctx, saga.SagaID, "setup"); err != nil {
		t.Fatal(err)
	}

	recovered, err := engine.RecoverStalled(ctx, clock.Now().Add(-30*time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("expected one recovered saga, got %d", recovered)
	}
	events := bus.publishedEvents()
	if len(events) != 2 || events[1].Kind != event.KindNext || events[1].SagaID != saga.SagaID {
		t.Fatalf("unexpected recovery events: %+v", events)
	}

	stored, err := repo.Get(ctx, saga.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range stored.History {
		if entry.Status == "RECOVERY_SCHEDULED" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("recovery was not recorded in history")
	}
}

func TestTimedStageFailureIsPersisted(t *testing.T) {
	t.Parallel()

	timed := &TimedFuncStage{
		FuncStage: &FuncStage{name: "timed", execute: func(ctx context.Context, _ *state.State) error {
			<-ctx.Done()
			return ctx.Err()
		}},
		timeout: time.Millisecond,
	}
	repo := memory.New()
	engine, err := New("test", repo, NewMemoryBus(), nil, nil, Node{Stage: timed})
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleNext(context.Background(), saga.SagaID); !stderrors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	final, err := repo.Get(context.Background(), saga.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != state.StatusFailed || final.Error == nil || final.Error.Stage != "timed" {
		t.Fatalf("timeout was not persisted: %+v", final)
	}
}

func TestCanceledExecutionCanBeRecoveredWithoutRepeatingCommittedStage(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	repo := memory.NewWithClock(clock.Now)
	stageAExecutions := 0
	stageBExecutions := 0
	stageBStarted := make(chan struct{})
	stageA := &FuncStage{name: "A", execute: func(_ context.Context, _ *state.State) error {
		stageAExecutions++
		return nil
	}}
	stageB := &FuncStage{name: "B", execute: func(ctx context.Context, _ *state.State) error {
		stageBExecutions++
		if stageBExecutions == 1 {
			close(stageBStarted)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}}
	bus := NewMemoryBus()
	engine, err := NewWithConfig(
		Config{Name: "test", Repository: repo, EventBus: bus, Clock: clock},
		Node{Stage: stageA},
		Node{Stage: stageB, DependsOn: []string{"A"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- engine.HandleNext(ctx, saga.SagaID) }()
	<-stageBStarted
	cancel()
	if err := <-done; !stderrors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}

	interrupted, err := repo.Get(context.Background(), saga.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	if !interrupted.ExecutedStages["A"] || interrupted.ExecutedStages["B"] {
		t.Fatalf("unexpected interrupted state: %+v", interrupted.ExecutedStages)
	}
	clock.Advance(time.Hour)
	if _, err := engine.RecoverStalled(context.Background(), clock.Now(), 10); err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleNext(context.Background(), saga.SagaID); err != nil {
		t.Fatal(err)
	}
	if stageAExecutions != 1 || stageBExecutions != 2 {
		t.Fatalf("unexpected restart executions: A=%d B=%d", stageAExecutions, stageBExecutions)
	}
}

func TestSaveFailureBeforeExecutionDoesNotStartStage(t *testing.T) {
	t.Parallel()

	injected := stderrors.New("save unavailable")
	repo := &faultRepository{
		Repository: memory.New(),
		saveErrAt:  map[int]error{1: injected},
	}
	executions := 0
	stageA := &FuncStage{name: "A", execute: func(_ context.Context, _ *state.State) error {
		executions++
		return nil
	}}
	engine, err := New("test", repo, NewMemoryBus(), nil, nil, Node{Stage: stageA})
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleNext(context.Background(), saga.SagaID); !stderrors.Is(err, injected) {
		t.Fatalf("expected injected save error, got %v", err)
	}
	if executions != 0 {
		t.Fatalf("stage started before RUNNING state was persisted: %d", executions)
	}
}

func TestRestartReexecutesStageWhoseResultWasNotPersisted(t *testing.T) {
	t.Parallel()

	injected := stderrors.New("crash before wave commit")
	repo := &faultRepository{
		Repository: memory.New(),
		saveErrAt:  map[int]error{2: injected},
	}
	executions := 0
	stageA := &FuncStage{name: "A", execute: func(_ context.Context, _ *state.State) error {
		executions++
		return nil
	}}
	engine, err := New("test", repo, NewMemoryBus(), nil, nil, Node{Stage: stageA})
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleNext(context.Background(), saga.SagaID); !stderrors.Is(err, injected) {
		t.Fatalf("expected injected save error, got %v", err)
	}
	if err := engine.HandleNext(context.Background(), saga.SagaID); err != nil {
		t.Fatal(err)
	}
	if executions != 2 {
		t.Fatalf("unpersisted stage must be reexecuted, got %d executions", executions)
	}
}

func TestRestartDoesNotRepeatPersistedWave(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	repo := &faultRepository{Repository: memory.New(), saveErrAt: make(map[int]error)}
	repo.afterSave = func(call int) {
		if call == 2 {
			cancel()
		}
	}
	stageAExecutions := 0
	stageBExecutions := 0
	stageA := &FuncStage{name: "A", execute: func(_ context.Context, _ *state.State) error {
		stageAExecutions++
		return nil
	}}
	stageB := &FuncStage{name: "B", execute: func(_ context.Context, _ *state.State) error {
		stageBExecutions++
		return nil
	}}
	bus := NewMemoryBus()
	engine, err := New(
		"test", repo, bus, nil, nil,
		Node{Stage: stageA}, Node{Stage: stageB, DependsOn: []string{"A"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	saga, err := engine.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleNext(ctx, saga.SagaID); !stderrors.Is(err, context.Canceled) {
		t.Fatalf("expected simulated crash cancellation, got %v", err)
	}

	restarted, err := New(
		"test", repo, bus, nil, nil,
		Node{Stage: stageA}, Node{Stage: stageB, DependsOn: []string{"A"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.HandleNext(context.Background(), saga.SagaID); err != nil {
		t.Fatal(err)
	}
	if stageAExecutions != 1 || stageBExecutions != 1 {
		t.Fatalf("persisted wave was repeated: A=%d B=%d", stageAExecutions, stageBExecutions)
	}
}

type failureCompensatingStage struct{ *FuncStage }

func (failureCompensatingStage) CompensateOnFailure() bool { return true }

func TestCompensatesFailedStageWhenOptedIn(t *testing.T) {
	ctx := context.Background()
	var order []string
	mk := func(name string, fail bool) *FuncStage {
		return &FuncStage{
			name: name,
			execute: func(context.Context, *state.State) error {
				if fail {
					return stderrors.New("boom")
				}
				return nil
			},
			compensate: func(context.Context, *state.State) error {
				order = append(order, name)
				return nil
			},
		}
	}
	eng, err := New("s", memory.New(), NewMemoryBus(), &NeverRetry{}, slog.Default(),
		Node{Stage: mk("A", false)},
		Node{Stage: failureCompensatingStage{mk("B", true)}, DependsOn: []string{"A"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	saga, err := eng.Start(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.HandleNext(ctx, saga.SagaID); err == nil {
		t.Fatal("expected failure")
	}
	if !slices.Equal(order, []string{"B", "A"}) {
		t.Fatalf("compensation order = %v, want [B A]", order)
	}
}

func TestStartWithIDIsIdempotent(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	eng, err := New("s", repo, NewMemoryBus(), &NeverRetry{}, slog.Default(),
		Node{Stage: &FuncStage{name: "A"}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := eng.StartWithID(ctx, "order-42", nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := eng.StartWithID(ctx, "order-42", nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.SagaID != "order-42" || second.SagaID != "order-42" {
		t.Fatalf("unexpected ids %q %q", first.SagaID, second.SagaID)
	}
	if _, err := eng.StartWithID(ctx, "", nil); !stderrors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}
