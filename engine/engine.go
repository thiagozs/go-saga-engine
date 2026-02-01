package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/google/uuid"

	"github.com/thiagozs/go-saga-engine/event"
	"github.com/thiagozs/go-saga-engine/repository"
	"github.com/thiagozs/go-saga-engine/retry"
	"github.com/thiagozs/go-saga-engine/stage"
	"github.com/thiagozs/go-saga-engine/state"
)

/*
========================
 EventBus contract
========================
*/

type EventBus interface {
	Publish(evt any)
	Subscribe(evt any, handler func(context.Context, any))
}

/*
========================
 DAG Node
========================
*/

type Node struct {
	Stage     stage.Stage
	DependsOn []string
	Parallel  bool
}

/*
========================
 Engine
========================
*/

type Engine struct {
	name  string
	nodes map[string]Node

	repo  repository.Repository
	retry retry.Policy
	bus   EventBus
	log   *slog.Logger
}

/*
========================
 Constructor
========================
*/

func New(
	name string,
	repo repository.Repository,
	bus EventBus,
	retry retry.Policy,
	log *slog.Logger,
	nodes ...Node,
) *Engine {
	m := make(map[string]Node)
	for _, n := range nodes {
		m[n.Stage.Name()] = n
	}

	return &Engine{
		name:  name,
		nodes: m,
		repo:  repo,
		retry: retry,
		bus:   bus,
		log:   log,
	}
}

/*
========================
 Start saga
========================
*/

func (e *Engine) Start(
	ctx context.Context,
	payload map[string]any,
) (*state.State, error) {

	s := &state.State{
		SagaID:         uuid.NewString(),
		Name:           e.name,
		Status:         state.StatusPending,
		Payload:        payload,
		ExecutedStages: make(map[string]bool),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	if err := e.repo.Create(ctx, s); err != nil {
		return nil, err
	}

	e.record(s, "SAGA", "STARTED", "")
	e.bus.Publish(event.Next{SagaID: s.SagaID})

	return s, nil
}

/*
========================
 Handle execution
========================
*/

func (e *Engine) HandleNext(ctx context.Context, sagaID string) {
	// contexto global cancelável
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	s, err := e.repo.Get(execCtx, sagaID)
	if err != nil || s == nil {
		return
	}

	s.Status = state.StatusRunning
	_ = e.repo.Update(execCtx, s)

	for {
		select {
		case <-execCtx.Done():
			return
		default:
		}

		ready := e.readyStages(s)
		if len(ready) == 0 {
			break
		}

		var (
			wg         sync.WaitGroup
			waveFailed atomic.Bool
		)

		for _, n := range ready {
			if s.ExecutedStages[n.Stage.Name()] {
				continue
			}

			node := n // capture

			wg.Add(1)
			run := func() {
				defer wg.Done()

				// não inicia se já falhou
				if waveFailed.Load() {
					return
				}

				stageCtx := execCtx
				if ts, ok := node.Stage.(stage.TimedStage); ok {
					var cancelTimeout context.CancelFunc
					stageCtx, cancelTimeout = context.WithTimeout(execCtx, ts.Timeout())
					defer cancelTimeout()
				}

				e.record(s, node.Stage.Name(), "EXECUTING", "")

				if err := node.Stage.Execute(stageCtx, s); err != nil {
					if waveFailed.CompareAndSwap(false, true) {
						e.fail(cancel, ctx, s, node.Stage, err)
					}
					return
				}
			}

			if node.Parallel {
				go run()
			} else {
				run()
			}
		}

		wg.Wait()

		// se a wave falhou, não confirma nada
		if waveFailed.Load() {
			return
		}

		// commit da wave
		for _, n := range ready {
			if s.ExecutedStages[n.Stage.Name()] {
				continue
			}
			s.ExecutedStages[n.Stage.Name()] = true
			e.record(s, n.Stage.Name(), "EXECUTED", "")
		}

		s.UpdatedAt = time.Now()
		_ = e.repo.Update(execCtx, s)
	}

	s.Status = state.StatusCompleted
	s.UpdatedAt = time.Now()
	e.record(s, "SAGA", "COMPLETED", "")
	_ = e.repo.Update(execCtx, s)
}

/*
========================
 Ready stages (DAG)
========================
*/

func (e *Engine) readyStages(s *state.State) []Node {
	var out []Node

	for name, node := range e.nodes {
		if s.ExecutedStages[name] {
			continue
		}

		ok := true
		for _, dep := range node.DependsOn {
			if !s.ExecutedStages[dep] {
				ok = false
				break
			}
		}

		if ok {
			out = append(out, node)
		}
	}

	return out
}

/*
========================
 Failure handling
========================
*/

func (e *Engine) fail(
	cancel context.CancelFunc,
	ctx context.Context,
	s *state.State,
	st stage.Stage,
	err error,
) {
	// cancela TODA execução paralela
	cancel()

	e.log.Error(
		"saga stage failed",
		"saga_id", s.SagaID,
		"stage", st.Name(),
		"error", err,
	)

	e.record(s, st.Name(), "FAILED", err.Error())

	// retry
	if e.retry != nil && e.retry.ShouldRetry(err, s) {
		e.bus.Publish(event.Retry{
			SagaID: s.SagaID,
			Stage:  st.Name(),
		})
		return
	}

	// compensação apenas do que já foi executado
	for name, node := range e.nodes {
		if s.ExecutedStages[name] {
			_ = node.Stage.Compensate(ctx, s)
			e.record(s, name, "COMPENSATED", "")
		}
	}

	s.Status = state.StatusFailed
	s.Error = &state.ErrorInfo{
		Stage:   st.Name(),
		Message: err.Error(),
	}
	s.UpdatedAt = time.Now()

	_ = e.repo.Update(ctx, s)
	e.bus.Publish(event.DeadLetter{SagaID: s.SagaID})
}

/*
========================
 History recorder
========================
*/

func (e *Engine) record(
	s *state.State,
	stage string,
	status string,
	msg string,
) {
	s.History = append(s.History, state.HistoryEntry{
		Stage:     stage,
		Status:    status,
		Message:   msg,
		Timestamp: time.Now(),
	})
}
