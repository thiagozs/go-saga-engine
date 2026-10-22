package engine

import (
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/thiagozs/go-saga-engine/event"
	"github.com/thiagozs/go-saga-engine/repository"
	"github.com/thiagozs/go-saga-engine/retry"
	"github.com/thiagozs/go-saga-engine/stage"
	"github.com/thiagozs/go-saga-engine/state"
)

var (
	ErrInvalidConfig   = stderrors.New("invalid engine configuration")
	ErrInvalidDAG      = stderrors.New("invalid saga DAG")
	ErrNoProgress      = stderrors.New("saga DAG cannot make progress")
	ErrPayloadConflict = stderrors.New("parallel stages changed the same payload key")
	ErrRetryNotReady   = stderrors.New("saga retry is not ready")
)

// EventBus transports engine lifecycle events. Durable implementations should
// provide their own delivery guarantees and deduplication.
type EventBus interface {
	Publish(ctx context.Context, envelope event.Envelope) error
}

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type Config struct {
	Name        string
	Repository  repository.Repository
	EventBus    EventBus
	RetryPolicy retry.Policy
	Logger      *slog.Logger
	LeaseTTL    time.Duration
	Clock       Clock
}

// Node associates a stage with its dependencies. Parallel nodes that become
// ready next to each other in topological order execute in the same wave.
type Node struct {
	Stage     stage.Stage
	DependsOn []string
	Parallel  bool
}

type Engine struct {
	name  string
	nodes []Node
	index map[string]Node

	repo        repository.Repository
	retryPolicy retry.Policy
	bus         EventBus
	log         *slog.Logger
	leaseTTL    time.Duration
	clock       Clock
}

// New validates the complete graph and returns an engine with a stable
// topological execution order.
func New(
	name string,
	repo repository.Repository,
	bus EventBus,
	retryPolicy retry.Policy,
	log *slog.Logger,
	nodes ...Node,
) (*Engine, error) {
	return NewWithConfig(Config{
		Name: name, Repository: repo, EventBus: bus, RetryPolicy: retryPolicy, Logger: log,
	}, nodes...)
}

func NewWithConfig(config Config, nodes ...Node) (*Engine, error) {
	if config.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidConfig)
	}
	if isNil(config.Repository) {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidConfig)
	}
	if isNil(config.EventBus) {
		return nil, fmt.Errorf("%w: event bus is required", ErrInvalidConfig)
	}
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if config.LeaseTTL <= 0 {
		config.LeaseTTL = 30 * time.Second
	}
	if config.Clock == nil {
		config.Clock = realClock{}
	}

	ordered, index, err := validateAndSort(nodes)
	if err != nil {
		return nil, err
	}

	return &Engine{
		name:        config.Name,
		nodes:       ordered,
		index:       index,
		repo:        config.Repository,
		retryPolicy: config.RetryPolicy,
		bus:         config.EventBus,
		log:         config.Logger,
		leaseTTL:    config.LeaseTTL,
		clock:       config.Clock,
	}, nil
}

func (e *Engine) Start(ctx context.Context, payload map[string]any) (*state.State, error) {
	return e.StartWithID(ctx, uuid.NewString(), payload)
}

// StartWithID is Start with a caller-supplied saga ID acting as idempotency
// key. If a saga with that ID already exists it is returned unchanged.
func (e *Engine) StartWithID(ctx context.Context, sagaID string, payload map[string]any) (*state.State, error) {
	if sagaID == "" {
		return nil, fmt.Errorf("%w: saga id is required", ErrInvalidConfig)
	}
	now := e.clock.Now()
	s := &state.State{
		SagaID:         sagaID,
		Name:           e.name,
		Status:         state.StatusPending,
		Payload:        state.ClonePayload(payload),
		ExecutedStages: make(map[string]bool),
		Attempts:       make(map[string]int),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	e.record(s, "SAGA", "STARTED", "")

	nextEvent := e.newEvent(event.KindNext, s.SagaID, "", 0, time.Time{})
	if err := e.repo.Create(ctx, s, nextEvent); err != nil {
		if stderrors.Is(err, repository.ErrAlreadyExists) {
			existing, getErr := e.repo.Get(ctx, sagaID)
			if getErr != nil {
				return nil, fmt.Errorf("load existing saga %q: %w", sagaID, getErr)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("create saga: %w", err)
	}
	if err := e.DispatchPending(ctx, 100); err != nil {
		return state.Clone(s), fmt.Errorf("dispatch saga start event: %w", err)
	}
	return state.Clone(s), nil
}

// HandleNext executes ready stages until the saga completes, needs a retry, or
// fails. Calls for the same SagaID are serialized through a renewable lease and
// every state transition uses optimistic concurrency.
func (e *Engine) HandleNext(ctx context.Context, sagaID string) (returnErr error) {
	owner := uuid.NewString()
	s, err := e.repo.Claim(ctx, sagaID, owner, e.leaseTTL)
	if err != nil {
		return fmt.Errorf("claim saga %q: %w", sagaID, err)
	}
	execCtx, cancel := context.WithCancel(ctx)
	stopRenewal := e.keepLease(execCtx, cancel, sagaID, owner)
	defer func() {
		cancel()
		returnErr = stderrors.Join(returnErr, stopRenewal())
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer releaseCancel()
		if releaseErr := e.repo.Release(releaseCtx, sagaID, owner); releaseErr != nil &&
			!stderrors.Is(releaseErr, repository.ErrLeaseLost) {
			returnErr = stderrors.Join(returnErr, fmt.Errorf("release saga lease: %w", releaseErr))
		}
		if dispatchErr := e.DispatchPending(releaseCtx, 100); dispatchErr != nil {
			returnErr = stderrors.Join(returnErr, dispatchErr)
		}
	}()

	if s.Status == state.StatusCompleted || s.Status == state.StatusFailed {
		return nil
	}
	if s.ExecutedStages == nil {
		s.ExecutedStages = make(map[string]bool)
	}
	if s.Attempts == nil {
		s.Attempts = make(map[string]int)
	}
	if s.Retry != nil && e.clock.Now().Before(s.Retry.NotBefore) {
		return fmt.Errorf("%w: retry at %s", ErrRetryNotReady, s.Retry.NotBefore.Format(time.RFC3339Nano))
	}
	if s.Retry != nil {
		e.record(s, s.Retry.Stage, "RETRYING", "")
		s.Retry = nil
	}

	s.Status = state.StatusRunning
	s.UpdatedAt = e.clock.Now()
	if err := e.save(execCtx, s, owner); err != nil {
		return fmt.Errorf("mark saga running: %w", err)
	}

	for !e.allStagesExecuted(s) {
		if err := execCtx.Err(); err != nil {
			return err
		}

		batch := e.nextBatch(s)
		if len(batch) == 0 {
			return e.fail(execCtx, s, owner, "SAGA", ErrNoProgress)
		}

		results := e.executeBatch(execCtx, s, batch)
		mergeErr := e.commitSuccessful(s, results)
		failedName, stageErr := firstFailure(results)
		if mergeErr != nil {
			failedName, stageErr = "SAGA", mergeErr
		}

		if stageErr != nil {
			e.recordFailures(s, results, failedName)
			if mergeErr != nil {
				e.record(s, "SAGA", "FAILED", mergeErr.Error())
			}
			s.UpdatedAt = e.clock.Now()
			if err := e.save(execCtx, s, owner); err != nil {
				return stderrors.Join(stageErr, fmt.Errorf("persist failed wave: %w", err))
			}
			return e.fail(execCtx, s, owner, failedName, stageErr)
		}

		s.UpdatedAt = e.clock.Now()
		if err := e.save(execCtx, s, owner); err != nil {
			return fmt.Errorf("persist completed wave: %w", err)
		}
	}

	s.Status = state.StatusCompleted
	s.UpdatedAt = e.clock.Now()
	e.record(s, "SAGA", "COMPLETED", "")
	if err := e.save(execCtx, s, owner); err != nil {
		return fmt.Errorf("complete saga: %w", err)
	}
	return nil
}

type stageResult struct {
	node  Node
	state *state.State
	err   error
}

func (e *Engine) executeBatch(ctx context.Context, s *state.State, batch []Node) []stageResult {
	waveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([]stageResult, len(batch))
	var (
		wg       sync.WaitGroup
		failOnce sync.Once
	)

	for i, node := range batch {
		e.record(s, node.Stage.Name(), "EXECUTING", "")
		results[i] = stageResult{node: node, state: state.Clone(s)}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			stageCtx := waveCtx
			if timed, ok := results[i].node.Stage.(stage.TimedStage); ok {
				var cancelTimeout context.CancelFunc
				stageCtx, cancelTimeout = context.WithTimeout(waveCtx, timed.Timeout())
				defer cancelTimeout()
			}

			results[i].err = results[i].node.Stage.Execute(stageCtx, results[i].state)
			if results[i].err != nil {
				failOnce.Do(cancel)
			}
		}(i)
	}

	wg.Wait()
	return results
}

func (e *Engine) commitSuccessful(s *state.State, results []stageResult) error {
	base := state.ClonePayload(s.Payload)
	merged := state.ClonePayload(base)
	changedBy := make(map[string]string)
	var conflict error

	for _, result := range results {
		if result.err != nil {
			continue
		}
		changes := payloadChanges(base, result.state.Payload)
		for _, change := range changes {
			if previous, exists := changedBy[change.key]; exists &&
				!samePayloadChange(merged, change) {
				conflict = fmt.Errorf(
					"%w: stages %q and %q changed key %q",
					ErrPayloadConflict, previous, result.node.Stage.Name(), change.key,
				)
				continue
			}
			changedBy[change.key] = result.node.Stage.Name()
			if change.deleted {
				delete(merged, change.key)
			} else {
				merged[change.key] = state.CloneValue(change.value)
			}
		}
	}

	if conflict == nil {
		s.Payload = merged
	}
	for _, result := range results {
		if result.err == nil {
			name := result.node.Stage.Name()
			s.ExecutedStages[name] = true
			s.CompletedStages = append(s.CompletedStages, name)
			e.record(s, name, "EXECUTED", "")
		}
	}
	return conflict
}

func (e *Engine) fail(
	ctx context.Context,
	s *state.State,
	owner string,
	failedStage string,
	cause error,
) error {
	s.CurrentStage = failedStage
	s.Attempts[failedStage]++
	attempt := s.Attempts[failedStage]
	e.log.Error(
		"saga stage failed",
		"saga_id", s.SagaID,
		"stage", failedStage,
		"error", cause,
	)

	if e.retryPolicy != nil && e.retryPolicy.ShouldRetry(cause, s) {
		delay := max(time.Duration(0), e.retryPolicy.NextDelay(attempt))
		notBefore := e.clock.Now().Add(delay)
		s.Status = state.StatusWaitingRetry
		s.Retry = &state.RetryInfo{
			Stage: failedStage, Attempt: attempt, NotBefore: notBefore, LastError: cause.Error(),
		}
		s.UpdatedAt = e.clock.Now()
		e.record(s, failedStage, "RETRY_SCHEDULED", notBefore.Format(time.RFC3339Nano))
		retryEvent := e.newEvent(event.KindRetry, s.SagaID, failedStage, attempt, notBefore)
		if err := e.save(ctx, s, owner, retryEvent); err != nil {
			return stderrors.Join(cause, fmt.Errorf("persist retry: %w", err))
		}
		return nil
	}

	compensationErr := e.compensate(ctx, s, failedStage)
	s.Status = state.StatusFailed
	s.Error = &state.ErrorInfo{Stage: failedStage, Message: cause.Error()}
	s.UpdatedAt = e.clock.Now()
	e.record(s, "SAGA", "FAILED", cause.Error())

	deadLetter := e.newEvent(event.KindDeadLetter, s.SagaID, failedStage, attempt, time.Time{})
	if err := e.save(ctx, s, owner, deadLetter); err != nil {
		return stderrors.Join(cause, compensationErr, fmt.Errorf("persist failed saga: %w", err))
	}
	return stderrors.Join(cause, compensationErr)
}

func (e *Engine) save(
	ctx context.Context,
	s *state.State,
	owner string,
	events ...event.Envelope,
) error {
	expectedVersion := s.Version
	if err := e.repo.Save(ctx, s, expectedVersion, owner, events...); err != nil {
		return err
	}
	s.Version = expectedVersion + 1
	return nil
}

func (e *Engine) newEvent(
	kind event.Kind,
	sagaID string,
	stageName string,
	attempt int,
	notBefore time.Time,
) event.Envelope {
	return event.Envelope{
		EventID: uuid.NewString(), Kind: kind, SagaID: sagaID, Stage: stageName,
		Attempt: attempt, NotBefore: notBefore, CreatedAt: e.clock.Now(),
	}
}

// DispatchPending publishes durable outbox events. Publication is at-least-once:
// consumers must deduplicate by EventID.
func (e *Engine) DispatchPending(ctx context.Context, limit int) error {
	events, err := e.repo.PendingEvents(ctx, limit)
	if err != nil {
		return fmt.Errorf("load pending events: %w", err)
	}
	for _, envelope := range events {
		if err := e.bus.Publish(ctx, envelope); err != nil {
			return fmt.Errorf("publish event %q: %w", envelope.EventID, err)
		}
		if err := e.repo.MarkEventPublished(ctx, envelope.EventID); err != nil &&
			!stderrors.Is(err, repository.ErrEventNotFound) {
			return fmt.Errorf("mark event %q published: %w", envelope.EventID, err)
		}
	}
	return nil
}

// RecoverStalled schedules durable Next events for RUNNING sagas that have not
// been updated since updatedBefore and whose previous lease is no longer held.
func (e *Engine) RecoverStalled(ctx context.Context, updatedBefore time.Time, limit int) (int, error) {
	ids, err := e.repo.RecoverableSagaIDs(ctx, updatedBefore, limit)
	if err != nil {
		return 0, fmt.Errorf("list recoverable sagas: %w", err)
	}
	recovered := 0
	for _, sagaID := range ids {
		owner := uuid.NewString()
		saga, claimErr := e.repo.Claim(ctx, sagaID, owner, e.leaseTTL)
		if stderrors.Is(claimErr, repository.ErrLeaseHeld) {
			continue
		}
		if claimErr != nil {
			return recovered, fmt.Errorf("claim stalled saga %q: %w", sagaID, claimErr)
		}

		if saga.Status == state.StatusRunning && !saga.UpdatedAt.After(updatedBefore) {
			e.record(saga, "SAGA", "RECOVERY_SCHEDULED", "")
			saga.UpdatedAt = e.clock.Now()
			nextEvent := e.newEvent(event.KindNext, sagaID, "", 0, time.Time{})
			if saveErr := e.save(ctx, saga, owner, nextEvent); saveErr != nil {
				releaseErr := e.repo.Release(context.WithoutCancel(ctx), sagaID, owner)
				return recovered, stderrors.Join(
					fmt.Errorf("schedule recovery for saga %q: %w", sagaID, saveErr),
					releaseErr,
				)
			}
			recovered++
		}
		if releaseErr := e.repo.Release(context.WithoutCancel(ctx), sagaID, owner); releaseErr != nil {
			return recovered, fmt.Errorf("release recovered saga %q: %w", sagaID, releaseErr)
		}
	}
	if err := e.DispatchPending(ctx, 100); err != nil {
		return recovered, err
	}
	return recovered, nil
}

func (e *Engine) keepLease(
	ctx context.Context,
	cancel context.CancelFunc,
	sagaID string,
	owner string,
) func() error {
	done := make(chan struct{})
	stopped := make(chan struct{})
	errCh := make(chan error, 1)
	interval := e.leaseTTL / 3
	if interval <= 0 {
		interval = time.Second
	}
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := e.repo.Renew(ctx, sagaID, owner, e.leaseTTL); err != nil {
					errCh <- fmt.Errorf("renew saga lease: %w", err)
					cancel()
					return
				}
			}
		}
	}()

	var once sync.Once
	return func() error {
		once.Do(func() { close(done) })
		<-stopped
		select {
		case err := <-errCh:
			return err
		default:
			return nil
		}
	}
}

func (e *Engine) compensate(ctx context.Context, s *state.State, failedStage string) error {
	completed := slices.Clone(s.CompletedStages)
	if len(completed) == 0 {
		for _, node := range e.nodes {
			if s.ExecutedStages[node.Stage.Name()] {
				completed = append(completed, node.Stage.Name())
			}
		}
	}

	// A failed stage may have produced partial effects; stages that opt in via
	// stage.FailureCompensator are compensated first (best effort, idempotent).
	if node, ok := e.index[failedStage]; ok && !s.ExecutedStages[failedStage] {
		if fc, ok := node.Stage.(stage.FailureCompensator); ok && fc.CompensateOnFailure() {
			completed = append(completed, failedStage)
		}
	}

	var compensationErr error
	for i := len(completed) - 1; i >= 0; i-- {
		name := completed[i]
		node, ok := e.index[name]
		if !ok {
			continue
		}
		if err := node.Stage.Compensate(ctx, s); err != nil {
			e.record(s, name, "COMPENSATION_FAILED", err.Error())
			compensationErr = stderrors.Join(compensationErr, fmt.Errorf("compensate %q: %w", name, err))
			continue
		}
		e.record(s, name, "COMPENSATED", "")
	}
	return compensationErr
}

func (e *Engine) nextBatch(s *state.State) []Node {
	ready := make([]Node, 0)
	for _, node := range e.nodes {
		name := node.Stage.Name()
		if s.ExecutedStages[name] {
			continue
		}
		allDependenciesDone := true
		for _, dependency := range node.DependsOn {
			if !s.ExecutedStages[dependency] {
				allDependenciesDone = false
				break
			}
		}
		if allDependenciesDone {
			ready = append(ready, node)
		}
	}
	if len(ready) == 0 || !ready[0].Parallel {
		return ready[:min(1, len(ready))]
	}

	end := 0
	for end < len(ready) && ready[end].Parallel {
		end++
	}
	return ready[:end]
}

func (e *Engine) allStagesExecuted(s *state.State) bool {
	for _, node := range e.nodes {
		if !s.ExecutedStages[node.Stage.Name()] {
			return false
		}
	}
	return true
}

func validateAndSort(nodes []Node) ([]Node, map[string]Node, error) {
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("%w: at least one node is required", ErrInvalidDAG)
	}

	index := make(map[string]Node, len(nodes))
	position := make(map[string]int, len(nodes))
	for i, node := range nodes {
		if isNil(node.Stage) {
			return nil, nil, fmt.Errorf("%w: node %d has a nil stage", ErrInvalidDAG, i)
		}
		name := node.Stage.Name()
		if name == "" {
			return nil, nil, fmt.Errorf("%w: node %d has an empty stage name", ErrInvalidDAG, i)
		}
		if _, exists := index[name]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate stage %q", ErrInvalidDAG, name)
		}
		index[name] = node
		position[name] = i
	}

	indegree := make(map[string]int, len(nodes))
	dependents := make(map[string][]string, len(nodes))
	for _, node := range nodes {
		name := node.Stage.Name()
		seen := make(map[string]struct{}, len(node.DependsOn))
		for _, dependency := range node.DependsOn {
			if _, exists := index[dependency]; !exists {
				return nil, nil, fmt.Errorf("%w: stage %q depends on unknown stage %q", ErrInvalidDAG, name, dependency)
			}
			if _, duplicate := seen[dependency]; duplicate {
				return nil, nil, fmt.Errorf("%w: stage %q repeats dependency %q", ErrInvalidDAG, name, dependency)
			}
			seen[dependency] = struct{}{}
			indegree[name]++
			dependents[dependency] = append(dependents[dependency], name)
		}
	}

	ready := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if indegree[node.Stage.Name()] == 0 {
			ready = append(ready, node.Stage.Name())
		}
	}
	ordered := make([]Node, 0, len(nodes))
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		ordered = append(ordered, index[name])
		for _, dependent := range dependents[name] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
		slices.SortStableFunc(ready, func(a, b string) int {
			return position[a] - position[b]
		})
	}
	if len(ordered) != len(nodes) {
		return nil, nil, fmt.Errorf("%w: graph contains a cycle", ErrInvalidDAG)
	}
	return ordered, index, nil
}

type payloadChange struct {
	key     string
	value   any
	deleted bool
}

func payloadChanges(base, changed map[string]any) []payloadChange {
	keys := make([]string, 0, len(base)+len(changed))
	seen := make(map[string]struct{}, len(base)+len(changed))
	for key := range base {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range changed {
		if _, exists := seen[key]; !exists {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)

	changes := make([]payloadChange, 0)
	for _, key := range keys {
		baseValue, inBase := base[key]
		changedValue, inChanged := changed[key]
		if inBase == inChanged && reflect.DeepEqual(baseValue, changedValue) {
			continue
		}
		changes = append(changes, payloadChange{key: key, value: changedValue, deleted: !inChanged})
	}
	return changes
}

func samePayloadChange(payload map[string]any, change payloadChange) bool {
	value, exists := payload[change.key]
	if change.deleted {
		return !exists
	}
	return exists && reflect.DeepEqual(value, change.value)
}

func firstFailure(results []stageResult) (string, error) {
	var canceled *stageResult
	for _, result := range results {
		if result.err == nil {
			continue
		}
		if !stderrors.Is(result.err, context.Canceled) {
			return result.node.Stage.Name(), result.err
		}
		if canceled == nil {
			resultCopy := result
			canceled = &resultCopy
		}
	}
	if canceled != nil {
		return canceled.node.Stage.Name(), canceled.err
	}
	return "", nil
}

func (e *Engine) recordFailures(s *state.State, results []stageResult, primary string) {
	for _, result := range results {
		if result.err == nil {
			continue
		}
		status := "FAILED"
		if stderrors.Is(result.err, context.Canceled) && result.node.Stage.Name() != primary {
			status = "CANCELLED"
		}
		e.record(s, result.node.Stage.Name(), status, result.err.Error())
	}
}

func (e *Engine) record(s *state.State, stageName, status, message string) {
	s.History = append(s.History, state.HistoryEntry{
		Stage: stageName, Status: status, Message: message, Timestamp: e.clock.Now(),
	})
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
