package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/thiagozs/go-saga-engine/engine"
	"github.com/thiagozs/go-saga-engine/errors"
	"github.com/thiagozs/go-saga-engine/event"
	"github.com/thiagozs/go-saga-engine/repository/memory"
	"github.com/thiagozs/go-saga-engine/state"
)

/*
   ========================
   EventBus em memória
   ========================
*/

type MemoryBus struct {
	mu       sync.Mutex
	handlers map[event.Kind][]func(context.Context, event.Envelope)
}

func NewMemoryBus() *MemoryBus {
	return &MemoryBus{
		handlers: make(map[event.Kind][]func(context.Context, event.Envelope)),
	}
}

func (b *MemoryBus) Publish(_ context.Context, envelope event.Envelope) error {
	b.mu.Lock()
	handlers := append([]func(context.Context, event.Envelope){}, b.handlers[envelope.Kind]...)
	b.mu.Unlock()
	for _, handler := range handlers {
		go handler(context.Background(), envelope)
	}
	return nil
}

func (b *MemoryBus) Subscribe(kind event.Kind, handler func(context.Context, event.Envelope)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[kind] = append(b.handlers[kind], handler)
}

/*
   ========================
   Retry simples (1 retry)
   ========================
*/

type RetryOnce struct {
	mu       sync.Mutex
	attempts map[string]int
}

func NewRetryOnce() *RetryOnce {
	return &RetryOnce{attempts: make(map[string]int)}
}

func (r *RetryOnce) ShouldRetry(err error, s *state.State) bool {
	e, ok := err.(*errors.StageError)
	if !ok || !e.Retryable {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.attempts[s.CurrentStage]++
	return r.attempts[s.CurrentStage] == 1
}

func (r *RetryOnce) NextDelay(attempt int) time.Duration {
	return 200 * time.Millisecond
}

/*
   ========================
   Stages fake (realistas)
   ========================
*/

type SimpleStage struct {
	name     string
	failOnce bool
	exec     int
}

func NewStage(name string, failOnce bool) *SimpleStage {
	return &SimpleStage{name: name, failOnce: failOnce}
}

func (s *SimpleStage) Name() string { return s.name }

func (s *SimpleStage) Execute(ctx context.Context, st *state.State) error {
	s.exec++
	slog.Info("executing stage", "stage", s.name, "attempt", s.exec)

	timer := time.NewTimer(300 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	if s.failOnce && s.exec == 1 {
		return errors.Retryable("temporary failure")
	}

	// simula escrita no payload
	st.Payload[s.name] = "ok"
	return nil
}

func (s *SimpleStage) Compensate(ctx context.Context, st *state.State) error {
	slog.Warn("compensating stage", "stage", s.name)
	return nil
}

/*
   ========================
   MAIN
   ========================
*/

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(log)

	if err := run(context.Background(), log); err != nil {
		log.Error("saga example failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	repo := memory.New()
	bus := NewMemoryBus()
	retry := NewRetryOnce()

	// stages
	identify := NewStage("identify-client", false)
	accounting := NewStage("close-accounting", false)
	bankInvoice := NewStage("generate-bank-invoice", true) // falha 1x
	pdf := NewStage("generate-pdf", false)
	email := NewStage("send-email", false)

	// nodes do saga
	nodes := []engine.Node{}

	nodes = append(nodes, engine.Node{
		Stage: identify,
	})
	nodes = append(nodes, engine.Node{
		Stage:     accounting,
		DependsOn: []string{"identify-client"},
	})
	nodes = append(nodes, engine.Node{
		Stage:     bankInvoice,
		DependsOn: []string{"close-accounting"},
		Parallel:  true,
	})
	nodes = append(nodes, engine.Node{
		Stage:     pdf,
		DependsOn: []string{"close-accounting", "generate-bank-invoice"},
		Parallel:  true,
	})
	nodes = append(nodes, engine.Node{
		Stage:     email,
		DependsOn: []string{"generate-bank-invoice", "generate-pdf"},
	})

	// engine saga
	eng, err := engine.New("invoice-saga",
		repo, bus, retry,
		log, nodes...,
	)
	if err != nil {
		return fmt.Errorf("create saga engine: %w", err)
	}

	completed := make(chan struct{})
	var completeOnce sync.Once
	markComplete := func() {
		completeOnce.Do(func() { close(completed) })
	}
	handle := func(ctx context.Context, envelope event.Envelope) {
		if err := eng.HandleNext(ctx, envelope.SagaID); err != nil {
			slog.Error("handle saga event", "kind", envelope.Kind, "error", err)
			markComplete()
			return
		}
		current, err := repo.Get(ctx, envelope.SagaID)
		if err != nil {
			slog.Error("read saga after event", "kind", envelope.Kind, "error", err)
			markComplete()
			return
		}
		if current.Status == state.StatusCompleted || current.Status == state.StatusFailed {
			markComplete()
		}
	}

	// wiring do bus
	bus.Subscribe(event.KindNext, handle)

	bus.Subscribe(event.KindRetry, func(ctx context.Context, envelope event.Envelope) {
		timer := time.NewTimer(max(time.Duration(0), time.Until(envelope.NotBefore)))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			slog.Error("wait for saga retry", "error", ctx.Err())
			markComplete()
			return
		case <-timer.C:
		}
		handle(ctx, envelope)
	})

	// start saga
	saga, err := eng.Start(ctx, map[string]any{
		"invoice_id": "INV-2026-01",
	})
	if err != nil {
		return fmt.Errorf("start saga: %w", err)
	}

	// aguarda o fluxo assíncrono concluir, com um limite explícito
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("wait for saga %q: timeout", saga.SagaID)
	}

	final, err := repo.Get(ctx, saga.SagaID)
	if err != nil {
		return fmt.Errorf("load final saga state: %w", err)
	}

	fmt.Println("\n======================")
	fmt.Println(" SAGA FINAL STATE")
	fmt.Println("======================")
	fmt.Println("ID:", final.SagaID)
	fmt.Println("STATUS:", final.Status)
	fmt.Println("PAYLOAD:", final.Payload)

	fmt.Println("\n--- HISTORY ---")
	for _, h := range final.History {
		fmt.Printf(
			"%s | %-20s | %-10s | %s\n",
			h.Timestamp.Format(time.RFC3339),
			h.Stage,
			h.Status,
			h.Message,
		)
	}

	return nil
}
