package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"reflect"
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

	time.Sleep(300 * time.Millisecond)

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

	ctx := context.Background()

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
	eng := engine.New("invoice-saga",
		repo, bus, retry,
		log, nodes...,
	)

	// wiring do bus
	bus.Subscribe(event.Next{}, func(ctx context.Context, evt any) {
		eng.HandleNext(ctx, evt.(event.Next).SagaID)
	})

	bus.Subscribe(event.Retry{}, func(ctx context.Context, evt any) {
		time.Sleep(300 * time.Millisecond)
		eng.HandleNext(ctx, evt.(event.Retry).SagaID)
	})

	// start saga
	saga, err := eng.Start(ctx, map[string]any{
		"invoice_id": "INV-2025-01",
	})
	if err != nil {
		panic(err)
	}

	// aguarda execução
	time.Sleep(3 * time.Second)

	final, _ := repo.Get(ctx, saga.SagaID)

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
}
