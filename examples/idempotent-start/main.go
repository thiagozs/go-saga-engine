// Command idempotent-start shows how Engine.StartWithID makes saga creation
// idempotent: the same business key never creates a second saga.
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/thiagozs/go-saga-engine/engine"
	"github.com/thiagozs/go-saga-engine/event"
	"github.com/thiagozs/go-saga-engine/repository/memory"
	"github.com/thiagozs/go-saga-engine/state"
)

// discardBus drops events; this example drives HandleNext directly.
type discardBus struct{}

func (discardBus) Publish(context.Context, event.Envelope) error { return nil }

// createAccount counts how many times its external effect really runs.
type createAccount struct{ executions atomic.Int32 }

func (*createAccount) Name() string { return "create-account" }

func (s *createAccount) Execute(context.Context, *state.State) error {
	s.executions.Add(1)
	return nil
}

func (*createAccount) Compensate(context.Context, *state.State) error { return nil }

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	stage := &createAccount{}

	eng, err := engine.New(
		"onboarding", memory.New(), discardBus{}, nil, nil,
		engine.Node{Stage: stage},
	)
	if err != nil {
		return fmt.Errorf("create engine: %w", err)
	}

	// The business key (here, the request ID) is the saga ID.
	const requestID = "onboarding-customer-42"
	payload := map[string]any{"customer_id": "42"}

	first, err := eng.StartWithID(ctx, requestID, payload)
	if err != nil {
		return fmt.Errorf("first start: %w", err)
	}
	// A duplicated request (client retry, redelivered message) is harmless.
	second, err := eng.StartWithID(ctx, requestID, payload)
	if err != nil {
		return fmt.Errorf("second start: %w", err)
	}
	fmt.Printf("first:  %s\nsecond: %s\n", first.SagaID, second.SagaID)

	if err := eng.HandleNext(ctx, first.SagaID); err != nil {
		return fmt.Errorf("handle saga: %w", err)
	}
	fmt.Println("stage executions:", stage.executions.Load())
	return nil
}
